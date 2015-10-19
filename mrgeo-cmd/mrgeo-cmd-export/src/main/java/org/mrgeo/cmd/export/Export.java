/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.cmd.export;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScaleManager;
import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.mrgeo.colorscale.applier.JpegColorScaleApplier;
import org.mrgeo.colorscale.applier.PngColorScaleApplier;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.tile.TileNotFoundException;
import org.mrgeo.image.*;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Export extends Command
{
private static Logger log = LoggerFactory.getLogger(Export.class);

private final static String X = "$X";
private final static String Y = "$Y";
private final static String ZOOM = "$ZOOM";
private final static String ID = "$ID";
private final static String LAT = "$LAT";
private final static String LON = "$LON";

int maxTiles = -1;
boolean useRand = false;
boolean singleImage = false;
int mosaicTileCount = -1;
boolean mosaicTiles = false;
Bounds bounds = null;
boolean useBounds = false;
Set<Long> tileset = null;
boolean useTileSet = false;
boolean all = false;
ColorScale colorscale = null;

boolean useTMS = false;

public Options createOptions()
{
  Options result = MrGeo.createOptions();

  final Option output = new Option("o", "output", true, "Output directory");
  output.setRequired(true);
  result.addOption(output);

  final Option zoom = new Option("z", "zoom", true, "Zoom level");
  zoom.setRequired(false);
  result.addOption(zoom);

  final Option count = new Option("c", "count", true, "Number of tiles to export");
  count.setRequired(false);
  result.addOption(count);

  final Option mosaic = new Option("m", "mosaic", true, "Number of adjacent tiles to mosaic");
  mosaic.setRequired(false);
  result.addOption(mosaic);

  final Option fmt = new Option("f", "format", true, "Output format (tif, jpg, png)");
  fmt.setRequired(false);
  result.addOption(fmt);

  final Option random = new Option("r", "random", false, "Randomize export");
  random.setRequired(false);
  result.addOption(random);

  final Option single = new Option("s", "single", false, "Export as a single image");
  single.setRequired(false);
  result.addOption(single);

  final Option tms = new Option("tms", "tms", false, "Export in TMS tile naming scheme");
  tms.setRequired(false);
  result.addOption(tms);

  final Option color = new Option("cs", "colorscale", true, "Color scale to apply");
  color.setRequired(false);
  result.addOption(color);

  final Option tileIds = new Option("t", "tileids", true,
      "A comma separated list of tile ID's to export");
  tileIds.setRequired(false);
  result.addOption(tileIds);

  final Option bounds = new Option("b", "bounds", true,
      "Returns the tiles intersecting and interior to"
          + "the bounds specified.  Lat/Lon Bounds in w, s, e, n format "
          + "(e.g., \"-180\",\"-90\",180,90). Does not honor \"-r\"");
  bounds.setRequired(false);
  result.addOption(bounds);

  final Option all = new Option("a", "all-levels", false, "Output all levels");
  all.setRequired(false);
  result.addOption(all);

  return result;
}

private Bounds parseBounds(String boundsOption)
{
  // since passed the arg parser, now can remove the " " around negative numbers
  boundsOption = boundsOption.replace("\"", "");
  final String[] bounds = boundsOption.split(",");
  assert (bounds.length == 4);
  final double w = Double.valueOf(bounds[0]);
  final double s = Double.valueOf(bounds[1]);
  final double e = Double.valueOf(bounds[2]);
  final double n = Double.valueOf(bounds[3]);

  assert (w >= -180 && w <= 180);
  assert (s >= -90 && s <= 90);
  assert (e >= -180 && e <= 180);
  assert (n >= -90 && n <= 90);
  assert (s <= n);
  assert (w <= e);

  return new Bounds(w, s, e, n);
}

private boolean saveSingleTile(final String output, final MrsImage image, String format,
    final long tileid, final int zoom, int tilesize)
{
  try
  {
    final MrsImagePyramidMetadata metadata = image.getMetadata();

    final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoom);

    Raster raster = image.getTile(t.tx, t.ty);

    log.info("Saving tile tx: " + t.tx + " ty: " + t.ty);

    if (raster != null)
    {
      String out = makeOutputName(output, format, tileid, zoom, tilesize, true);

      if (colorscale != null || !format.equals("tif"))
      {
        ColorScaleApplier applier = null;
        switch (format)
        {
        case "tif":
        case "png":
          applier = new PngColorScaleApplier();
          break;
        case "jpg":
        case "jpeg":
          applier = new JpegColorScaleApplier();
          break;
        }

        if (applier != null)
        {
          raster = applier.applyColorScale(raster, colorscale, image.getExtrema(), image.getMetadata().getDefaultValues());
        }
      }

      GDALJavaUtils.saveRasterTile(raster, out, t.tx, t.ty, image.getZoomlevel(), metadata.getDefaultValue(0), format);

      System.out.println("Wrote output to " + out);
      return true;
    }

    log.info("no raster!");

  }
  catch (final Exception e)
  {
    e.printStackTrace();
  }
  return false;
}




private boolean saveMultipleTiles(String output, String format, final MrsImage image,
    final long[] tiles)
{
  try
  {
    final MrsImagePyramidMetadata metadata = image.getMetadata();

    Raster raster = RasterTileMerger.mergeTiles(image, tiles);
    Raster sampleRaster = null;

    TMSUtils.Bounds imageBounds = null;

    long minId = tiles[0];
    final int tilesize = image.getTilesize();
    final int zoomlevel = image.getZoomlevel();
    for (final long lid : tiles)
    {
      if (minId > lid)
      {
        minId = lid;
      }
      final TMSUtils.Tile tile = TMSUtils.tileid(lid, zoomlevel);
      final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);

      // expand the image bounds by the tile
      if (imageBounds == null)
      {
        imageBounds = bounds;
      }
      else
      {
        imageBounds.expand(bounds);
      }

      if (sampleRaster == null)
      {
        try
        {
          sampleRaster = image.getTile(tile.tx, tile.ty);
        }
        catch (final TileNotFoundException e)
        {
          // we can eat the TileNotFoundException...
        }
      }
    }

    if (imageBounds == null)
    {
      throw new MrsImageException("Error, could not calculate the bounds of the tiles");
    }
    if (sampleRaster == null)
    {
      throw new MrsImageException("Error, could not load any tiles");
    }
    String out = makeOutputName(output, format, minId, zoomlevel, tilesize, false);

    if (colorscale != null || !format.equals("tif"))
    {
      ColorScaleApplier applier = null;
      switch (format)
      {
      case "tif":
      case "png":
        applier = new PngColorScaleApplier();
        break;
      case "jpg":
      case "jpeg":
        applier = new JpegColorScaleApplier();
        break;
      }

      if (applier != null)
      {
        raster = applier.applyColorScale(raster, colorscale, image.getExtrema(), image.getMetadata().getDefaultValues());
      }
    }

    GDALJavaUtils.saveRaster(raster, out, imageBounds, metadata.getDefaultValue(0), format);

    System.out.println("Wrote output to " + out);
    return true;
  }
  catch (final Exception e)
  {
    e.printStackTrace();
  }
  return false;
}

@Override
public int run(final String[] args, Configuration conf, ProviderProperties providerProperties)
{
  log.info("Export");

  try
  {
    final Options options = createOptions();
    CommandLine line;
    try
    {
      final CommandLineParser parser = new PosixParser();
      line = parser.parse(options, args);

      if (line == null || line.hasOption("h")) {
        new HelpFormatter().printHelp("Export", options);
        return 1;
      }

      if (line.hasOption("b") &&
          (line.hasOption("t") || line.hasOption("c") || line.hasOption("r")))
      {
        log.debug("Option -b is currently incompatible with -t, -c, and -r");
        throw new ParseException("Incorrect combination of arguments");
      }

      if (line.hasOption("s") && line.hasOption("m"))
      {
        log.debug("Cannot use both -s and -m");
        throw new ParseException("Incorrect combination of arguments");
      }
      if (line.hasOption("v"))
      {
        LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
      }
      if (line.hasOption("d"))
      {
        LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
      }

      if (line.hasOption("l"))
      {
        System.out.println("Using local runner");
        HadoopUtils.setupLocalRunner(conf);
      }

      String outputbase = line.getOptionValue("o");

      if (line.hasOption("c"))
      {
        maxTiles = Integer.valueOf(line.getOptionValue("c"));
      }

      useRand = line.hasOption("r");
      all = line.hasOption("a");

      singleImage = line.hasOption("s");
      mosaicTiles = line.hasOption("m");
      if (mosaicTiles)
      {
        mosaicTileCount = Integer.valueOf(line.getOptionValue("m"));
      }

      useBounds = line.hasOption("b");
      if (useBounds)
      {
        final String boundsOption = line.getOptionValue("b");
        bounds = parseBounds(boundsOption);
      }

      if (line.hasOption("cs"))
      {
        colorscale = ColorScaleManager.fromName(line.getOptionValue("cs"));
      }

      useTileSet = line.hasOption("t");
      if (useTileSet)
      {
        final String tileIdOption = line.getOptionValue("t");
        final String[] tileIds = tileIdOption.split(",");
        for (final String tileId : tileIds)
        {
          tileset.add(Long.valueOf(tileId));
        }
      }

      int zoomlevel = -1;
      if (line.hasOption("z"))
      {
        zoomlevel = Integer.valueOf(line.getOptionValue("z"));
      }

      String format = "tif";
      if (line.hasOption("f"))
      {
        format = line.getOptionValue("f");
      }

      useTMS = line.hasOption("tms");

//      if (!singleImage)
//      {
//        FileUtils.createDir(new File(outputbase));
//      }

      for (final String arg : line.getArgs())
      {
        // The input can be either an image or a vector.
        MrsImagePyramid imagePyramid;
        MrsPyramid pyramid = null;
        try
        {
          imagePyramid = MrsImagePyramid.open(arg, providerProperties);
          pyramid = imagePyramid;
        }
        catch(IOException e)
        {
          imagePyramid = null;
        }

        if (imagePyramid == null)
        {
          throw new IOException("Specified input must be either an image or a vector");
        }
        if (zoomlevel <= 0)
        {
          zoomlevel = pyramid.getMaximumLevel();
        }

        int end = zoomlevel;
        if (all)
        {
          end = 1;
        }

        while (zoomlevel >= end)
        {
          //final String output = outputbase + (all ? "_" + Integer.toString(zoomlevel) : "");

          MrsImage image = null;
          if (imagePyramid != null)
          {
            image = imagePyramid.getImage(zoomlevel);
          }
          try
          {
            final Set<Long> tiles = calculateTiles(pyramid, zoomlevel);

            int tilesize = imagePyramid.getTileSize();

            if (singleImage)
            {
              if (imagePyramid != null)
              {
                saveMultipleTiles(outputbase, format,
                    image, ArrayUtils.toPrimitive(tiles.toArray(new Long[tiles.size()])));
              }
            }
            else if (mosaicTiles && mosaicTileCount > 0)
            {

              if (!outputbase.contains(X) || !outputbase.contains(Y) ||
                  !outputbase.contains(LAT) || !outputbase.contains(LON))
              {
                outputbase = outputbase + "/$Y-$X";
              }
              for (final Long tileid : tiles)
              {
                final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoomlevel);
                final Set<Long> tilesToMosaic = new HashSet<>();
                final LongRectangle tileBounds = pyramid.getTileBounds(zoomlevel);
                for (long ty1 = t.ty; ((ty1 < (t.ty + mosaicTileCount)) && (ty1 <= tileBounds
                    .getMaxY())); ty1++)
                {
                  for (long tx1 = t.tx; ((tx1 < (t.tx + mosaicTileCount)) && (tx1 <= tileBounds
                      .getMaxX())); tx1++)
                  {
                    tilesToMosaic.add(TMSUtils.tileid(tx1, ty1, zoomlevel));
                  }
                }
//                final String mosaicOutput = output + "/" + t.ty + "-" + t.tx + "-" +
//                    TMSUtils.tileid(t.tx, t.ty, zoomlevel);
                if (imagePyramid != null)
                {
                  saveMultipleTiles(outputbase, format,
                      image, ArrayUtils.toPrimitive(tilesToMosaic.toArray(new Long[tilesToMosaic.size()])));
                }
              }
            }
            else
            {
              for (final Long tileid : tiles)
              {
                if (imagePyramid != null)
                {
                  saveSingleTile(outputbase, image, format, tileid, zoomlevel, tilesize);
                }
              }
            }
          }
          finally
          {
            if (image != null)
            {
              image.close();
            }
          }

          zoomlevel--;
        }
      }
    }
    catch (final ParseException e)
    {
      new HelpFormatter().printHelp("Export", options);
      return 1;
    }

    return 0;
  }
  catch (Exception e)
  {
    e.printStackTrace();
  }

  return -1;
}

private String makeTMSOutputName(String base, String format, long tileid, int zoom) throws IOException
{
  final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoom);

  String output = String.format("%s/%d/%d/%d", base, zoom, t.tx, t.ty);

  switch (format)
  {
  case "tif":
    output += ".tif";
    break;
  case "png":
    output += ".png";
    break;
  case "jpg":
  case "jpeg":
    output += ".jpg";
    break;
  }

  File f = new File(output);
  FileUtils.createDir(f.getParentFile());

  return output;
}

private String makeOutputName(String template, String format, long tileid, int zoom, int tilesize, boolean reformat) throws IOException
{
  if (useTMS)
  {
    return makeTMSOutputName(template, format, tileid, zoom);
  }

  final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoom);
  TMSUtils.Bounds bounds = TMSUtils.tileBounds(t.tx, t.ty, zoom, tilesize);

  String output;

  if (template.contains(X) || template.contains(Y) || template.contains(ZOOM) || template.contains(ID) ||
      template.contains(LAT) || template.contains(LON))
  {
    output = template.replace(X, String.format("%03d", t.tx));
    output = output.replace(Y, String.format("%03d", t.ty));
    output = output.replace(ZOOM, String.format("%d", zoom));
    output = output.replace(ID, String.format("%d", tileid));
    if (output.contains(LAT))
    {
      double lat = bounds.s;

      String dir = "N";
      if (lat < 0)
      {
        dir = "S";
      }
      output = output.replace(LAT, String.format("%s%3d", dir, Math.abs((int) lat)));
    }

    if (output.contains(LON))
    {
      double lon = bounds.w;

      String dir = "E";
      if (lon < 0)
      {
        dir = "W";
      }
      output = output.replace(LON, String.format("%s%3d", dir, Math.abs((int) lon)));
    }
  }
  else
  {
    output = template;
    if (reformat)
    {
      if (template.endsWith("/"))
      {
        output += "tile-";
      }
      output += String.format("%d", tileid) + "-" + String.format("%03d", t.ty) + "-" + String.format("%03d", t.tx);
    }
  }

  if (format.equals("tif") && (!output.endsWith(".tif") || !output.endsWith(".tiff")))
  {
    output += ".tif";
  }
  else if (format.equals("png") && !output.endsWith(".png"))
  {
    output += ".png";
  }
  else if (format.equals("jpg") && (!output.endsWith(".jpg") || !output.endsWith(".jpeg")))
  {
    output += ".jpg";
  }

  File f = new File(output);
  FileUtils.createDir(f.getParentFile());

  return output;
}

private Set<Long> calculateTiles(final MrsPyramid pyramid, int zoomlevel)
{
  final Set<Long> tiles = new HashSet<>();

  if (tileset != null)
  {
    return tileset;
  }

  LongRectangle tileBounds = pyramid.getTileBounds(zoomlevel);
  if (mosaicTiles && mosaicTileCount > 0)
  {
    for (long ty = tileBounds.getMinY(); ty <= tileBounds.getMaxY(); ty += mosaicTileCount)
    {
      for (long tx = tileBounds.getMinX(); tx <= tileBounds.getMaxX(); tx += mosaicTileCount)
      {
        tiles.add(TMSUtils.tileid(tx, ty, zoomlevel));
      }
    }

    return tiles;
  }

  if (useBounds)
  {
    return RasterTileMerger.getTileIdsFromBounds(bounds, zoomlevel, pyramid.getTileSize());
  }

  if (useRand && maxTiles > 0)
  {
    for (int i = 0; i < maxTiles; i++)
    {
      final long tx = (long) (tileBounds.getMinX() +
          (Math.random() * (tileBounds.getMaxX() - tileBounds.getMinX())));
      final long ty = (long) (tileBounds.getMinY() +
          (Math.random() * (tileBounds.getMaxY() - tileBounds.getMinY())));

      final long id = TMSUtils.tileid(tx, ty, zoomlevel);
      if (!tiles.contains(id))
      {
        tiles.add(id);
      }
    }

    return tiles;
  }

  for (long ty = tileBounds.getMinY(); ty <= tileBounds.getMaxY(); ty++)
  {
    for (long tx = tileBounds.getMinX(); tx <= tileBounds.getMaxX(); tx++)
    {
      tiles.add(TMSUtils.tileid(tx, ty, zoomlevel));
    }
  }

  return tiles;
}
}
