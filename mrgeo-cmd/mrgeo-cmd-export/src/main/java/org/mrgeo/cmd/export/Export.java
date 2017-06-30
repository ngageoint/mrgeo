/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.cmd.export;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScaleManager;
import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.mrgeo.colorscale.applier.JpegColorScaleApplier;
import org.mrgeo.colorscale.applier.PngColorScaleApplier;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.tile.TileNotFoundException;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.utils.*;
import org.mrgeo.utils.logging.LoggingUtils;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Export extends Command
{
private final static String X = "$X";
private final static String Y = "$Y";
private final static String ZOOM = "$ZOOM";
private final static String ID = "$ID";
private final static String LAT = "$LAT";
private final static String LON = "$LON";
private static Logger log = LoggerFactory.getLogger(Export.class);
private int maxTiles = -1;
private boolean useRand = false;
private int mosaicTileCount = -1;
private boolean mosaicTiles = false;
private Bounds bounds = null;
private boolean useBounds = false;
private Set<Long> tileset = null;
private ColorScale colorscale = null;
private boolean useTMS;
private int maxSizeInKb = -1;

@Override
@SuppressWarnings("squid:S1166") // Exception caught and handled
public int run(CommandLine line, Configuration conf,
               ProviderProperties providerProperties) throws ParseException
{
  log.info("Export");

  try
  {
    if (line.hasOption("b") &&
        (line.hasOption("t") || line.hasOption("c") || line.hasOption("r") || line.hasOption("p")))
    {
      throw new ParseException("Option -b is currently incompatible with -t, -c, -p, and -r");
    }

    if (line.hasOption("s") && line.hasOption("m"))
    {
      throw new ParseException("Cannot use both -s and -m");
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
      maxTiles = Integer.parseInt(line.getOptionValue("c"));
    }

    useRand = line.hasOption("r");
    boolean all = line.hasOption("a");

    boolean singleImage = line.hasOption("s");
    mosaicTiles = line.hasOption("m");
    if (mosaicTiles)
    {
      mosaicTileCount = Integer.parseInt(line.getOptionValue("m"));
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

    if (line.hasOption("ms"))
    {
      maxSizeInKb = SparkUtils.humantokb(line.getOptionValue("ms"));
    }

    boolean useTileSet = line.hasOption("t");
    if (useTileSet)
    {
      tileset = new HashSet<Long>();
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
      zoomlevel = Integer.parseInt(line.getOptionValue("z"));
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

    if (maxSizeInKb > 0 && !singleImage) {
      throw new ParseException("maxsize can only be specified along with single");
    }
    for (final String arg : line.getArgs())
    {
      MrsPyramid imagePyramid;
      MrsPyramid pyramid = null;
      String pyramidName = "";
      try
      {
        MrsImageDataProvider dp =
            DataProviderFactory.getMrsImageDataProvider(arg, DataProviderFactory.AccessMode.READ, providerProperties);
        imagePyramid = MrsPyramid.open(dp);
        pyramidName = dp.getSimpleResourceName();
        pyramid = imagePyramid;
      }
      catch (IOException e)
      {
        imagePyramid = null;
      }

      if (imagePyramid == null)
      {
        throw new ParseException("Specified input must be an image");
      }

      MrsPyramidMetadata meta = pyramid.getMetadata();
      ColorScaleApplier applier = null;
      if (colorscale != null || !"tif".equalsIgnoreCase(format)) {
        switch (format) {
          case "jpg":
          case "jpeg":
            applier = new JpegColorScaleApplier();
            break;
          case "tif":
          case "png":
          default:
            applier = new PngColorScaleApplier();
            break;
        }
      }

      if (maxSizeInKb > 0) {
        // Compute the zoom level required to keep the output image smaller
        // than the specified max size.
        MrGeoRaster rasterForAnyTile = pyramid.getHighestResImage().getAnyTile();
        int bytesPerPixelPerBand = (applier != null) ? applier.getBytesPerPixelPerBand() : rasterForAnyTile.bytesPerPixel();
        int bands = (applier != null) ? applier.getBands(pyramid.getMetadata().getBands()) : 1;
        Bounds b = (useBounds) ? bounds : pyramid.getBounds();
        int maxZoom = pyramid.getMaximumLevel();
        zoomlevel = RasterUtils.getMaxPixelsForSize(maxSizeInKb, b,
                bytesPerPixelPerBand, bands, meta.getTilesize()).getZoom();
        zoomlevel = Math.min(zoomlevel, maxZoom);
        System.out.println("Exporting image at zoom level " + zoomlevel);
      }
      else if (zoomlevel <= 0)
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

        // If tile id's were not specified with -t, but -p is specified, then we
        // need to re-compute the tiles for the specified points for each zoom level.
        if (!useTileSet && line.hasOption("p"))
        {
          tileset = new HashSet<Long>();
          final String pointsOption = line.getOptionValue("p");
          final String[] points = pointsOption.split(",");
          if (points.length % 2 != 0)
          {
            throw new IOException("The -p option requires lon/lat pairs");
          }
          for (int i = 0; i < points.length; i += 2)
          {
            double lon = Double.valueOf(points[i].trim());
            double lat = Double.valueOf(points[i + 1].trim());
            Tile pointTile = TMSUtils.latLonToTile(lat, lon, zoomlevel, pyramid.getTileSize());
            tileset.add(Long.valueOf(TMSUtils.tileid(pointTile.getTx(), pointTile.getTy(), zoomlevel)));
          }
        }

        MrsImage image = imagePyramid.getImage(zoomlevel);
        try
        {
          final Set<Long> tiles = calculateTiles(pyramid, zoomlevel);

          int tilesize = imagePyramid.getTileSize();

          if (singleImage)
          {
            String ob = outputbase;
            if (all)
            {
              ob += "-" + zoomlevel;
            }
            saveMultipleTiles(ob, pyramidName, format, image, applier,
                    ArrayUtils.toPrimitive(tiles.toArray(new Long[tiles.size()])),
                    providerProperties);
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
              final Tile t = TMSUtils.tileid(tileid, zoomlevel);
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
              saveMultipleTiles(outputbase, pyramidName, format, image, applier,
                      ArrayUtils.toPrimitive(tilesToMosaic.toArray(new Long[tilesToMosaic.size()])),
                      providerProperties);
            }
          }
          else
          {
            for (final Long tileid : tiles)
            {
              saveSingleTile(outputbase, pyramidName, image, applier, format,
                      tileid, zoomlevel, tilesize, providerProperties);
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

    return 0;
  }
  catch (ColorScale.ColorScaleException | IOException e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}

@Override
public String getUsage() { return "export <options> <input>"; }

@Override
public void addOptions(Options options)
{
  final Option maxImageSize = new Option("ms", "maxsize", true, "Maximum size of output image (in kb)");
  maxImageSize.setRequired(false);
  options.addOption(maxImageSize);

  final Option output = new Option("o", "output", true, "Output directory");
  output.setRequired(true);
  options.addOption(output);

  final Option zoom = new Option("z", "zoom", true, "Zoom level");
  zoom.setRequired(false);
  options.addOption(zoom);

  final Option count = new Option("c", "count", true, "Number of tiles to export");
  count.setRequired(false);
  options.addOption(count);

  final Option mosaic = new Option("m", "mosaic", true, "Number of adjacent tiles to mosaic");
  mosaic.setRequired(false);
  options.addOption(mosaic);

  final Option fmt = new Option("f", "format", true, "Output format (tif, jpg, png)");
  fmt.setRequired(false);
  options.addOption(fmt);

  final Option random = new Option("r", "random", false, "Randomize export");
  random.setRequired(false);
  options.addOption(random);

  final Option single = new Option("s", "single", false, "Export as a single image");
  single.setRequired(false);
  options.addOption(single);

  final Option tms = new Option("tms", "tms", false, "Export in TMS tile naming scheme");
  tms.setRequired(false);
  options.addOption(tms);

  final Option color = new Option("cs", "colorscale", true, "Color scale to apply");
  color.setRequired(false);
  options.addOption(color);

  final Option tileIds = new Option("t", "tileids", true,
      "A comma separated list of tile ID's to export");
  tileIds.setRequired(false);
  options.addOption(tileIds);

  final Option points = new Option("p", "points", true,
      "A comma separated list of lon, lat, lon, lat, ... for which to export tiles");
  points.setRequired(false);
  options.addOption(points);

  final Option bounds = new Option("b", "bounds", true,
      "Returns the tiles intersecting and interior to"
          + "the bounds specified.  Lat/Lon Bounds in w, s, e, n format "
          + "(e.g., \"-180\",\"-90\",180,90). Does not honor \"-r\"");
  bounds.setRequired(false);
  options.addOption(bounds);

  final Option all = new Option("a", "all-levels", false, "Output all levels");
  all.setRequired(false);
  options.addOption(all);
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

private MrGeoRaster colorRaster(MrGeoRaster raster,
                                MrsPyramidMetadata metadata,
                                ColorScaleApplier applier,
                                final int zoom,
                                final String pyramidName,
                                final String format,
                                final ProviderProperties providerProperties) throws IOException
{
  if (colorscale != null || !"tif".equals(format))
  {
    if (colorscale == null) {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(pyramidName,
              DataProviderFactory.AccessMode.READ, providerProperties);
      MrsPyramidMetadata meta = dp.getMetadataReader().read();

      String csname = meta.getTag(MrGeoConstants.MRGEO_DEFAULT_COLORSCALE);
      if (csname != null)
      {
        try
        {
          colorscale = ColorScaleManager.fromName(csname);
        }
        catch (ColorScale.ColorScaleException ignored)
        {
        }
        if (colorscale == null)
        {
          throw new IOException("Can not load default style: "  + csname);
        }
      }
      else
      {
        colorscale = ColorScale.createDefaultGrayScale();
      }

    }
    try
    {
      raster = applier.applyColorScale(raster,
              colorscale,
              metadata.getExtrema(zoom),
              metadata.getDefaultValues(),
              metadata.getQuantiles());
    }
    catch (Exception e)
    {
      log.error("Exception thrown", e);
    }
  }
  return raster;
}

private boolean saveSingleTile(final String output, final String pyramidName, final MrsImage image,
    ColorScaleApplier applier, String format, final long tileid,
    final int zoom, int tilesize, ProviderProperties providerProperties)
{
  try
  {
    final MrsPyramidMetadata metadata = image.getMetadata();

    final Tile t = TMSUtils.tileid(tileid, zoom);

    MrGeoRaster raster = image.getTile(t.tx, t.ty);

    log.info("Saving tile tx: " + t.tx + " ty: " + t.ty);

    if (raster != null)
    {
      String out = makeOutputName(output, pyramidName, format, tileid, zoom, tilesize, true);

      raster = colorRaster(raster, metadata, applier, zoom, pyramidName, format, providerProperties);
      Bounds bnds = TMSUtils.tileBounds(t.tx, t.ty, image.getZoomlevel(), metadata.getTilesize());
      GDALJavaUtils.saveRasterTile(raster.toDataset(bnds, metadata.getDefaultValues()),
          out, t.tx, t.ty, image.getZoomlevel(), metadata.getDefaultValue(0), format);

      System.out.println("Wrote output to " + out);
      return true;
    }

    log.info("no raster!");

  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
  }
  catch(TileNotFoundException e) {
    // skip missing tile.
  }
  return false;
}

private boolean saveMultipleTiles(String output, String pyramidName,
                                  String format, final MrsImage image,
                                  ColorScaleApplier applier,
                                  final long[] tiles, ProviderProperties providerProperties)
{
  try
  {
    final int tilesize = image.getTilesize();
    final int zoomlevel = image.getZoomlevel();

    final MrsPyramidMetadata metadata = image.getMetadata();

    MrGeoRaster raster;
    if (useBounds)
    {
      TileBounds tb = TMSUtils.boundsToTile(bounds, zoomlevel, tilesize);
      raster = image.getRaster(tb);
    }
    else
    {
      raster = image.getRaster(tiles);
    }

    Bounds imageBounds = null;

    long minId = tiles[0];
    for (final long lid : tiles)
    {
      if (minId > lid)
      {
        minId = lid;
      }
      final Tile tile = TMSUtils.tileid(lid, zoomlevel);
      final Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);

      // expand the image bounds by the tile
      if (imageBounds == null)
      {
        imageBounds = bounds;
      }
      else
      {
        imageBounds = imageBounds.expand(bounds);
      }

    }

    if (imageBounds == null)
    {
      throw new MrsImageException("Error, could not load any tiles");
    }
    String out = makeOutputName(output, pyramidName, format, minId, zoomlevel, tilesize, false);
    raster = colorRaster(raster, metadata, applier, zoomlevel, pyramidName, format, providerProperties);
    GDALJavaUtils
        .saveRaster(raster.toDataset(imageBounds, metadata.getDefaultValues()), out, null, metadata.getDefaultValue(0),
            format);

    System.out.println("Wrote output to " + out);
    return true;
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
  }
  return false;
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() constructing a filename and checking for existence")
private String makeTMSOutputName(String base, String format, long tileid, int zoom) throws IOException
{
  final Tile t = TMSUtils.tileid(tileid, zoom);

  String output = String.format("%s/%d/%d/%d", base, zoom, t.tx, t.ty);

  switch (format)
  {
  case "png":
    output += ".png";
    break;
  case "jpg":
  case "jpeg":
    output += ".jpg";
    break;
  case "tif":
  default:
    output += ".tif";
    break;
  }

  File f = new File(output);
  FileUtils.createDir(f.getParentFile());

  return output;
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() constructing a filename and checking for existence")
private String makeOutputName(String template, String imageName, String format,
    long tileid, int zoom, int tilesize, boolean reformat) throws IOException
{
  if (useTMS)
  {
    return makeTMSOutputName(template, format, tileid, zoom);
  }

  final Tile t = TMSUtils.tileid(tileid, zoom);
  Bounds bounds = TMSUtils.tileBounds(t.tx, t.ty, zoom, tilesize);

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
      File f = new File(template);
      if (template.endsWith(File.separator) || (f.exists() && f.isDirectory()))
      {
        if (!template.endsWith(File.separator))
        {
          output += File.separator;
        }
        output += "tile-";
      }
      output += String.format("%d", tileid) + "-" + String.format("%03d", t.ty) + "-" + String.format("%03d", t.tx);
    }
    else
    {
      // If output is a dir, then write imageName.tif to that dir
      File f = new File(output).getCanonicalFile();
      if (output.endsWith(File.separator) || (f.exists() && f.isDirectory()))
      {
        // If the output is an existing directory, then use the image name (not the full path)
        // as the name of the output in that directory.
        File imageFile = new File(imageName);
        f = new File(f, imageFile.getName());
        output = f.getPath();
      }
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

@SuppressFBWarnings(value = "PREDICTABLE_RANDOM", justification = "Random() used to get random tileids")
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
    TileBounds tb = TMSUtils.boundsToTile(bounds, zoomlevel, pyramid.getTileSize());
    for (long x = tb.w; x <= tb.e; x++)
    {
      for (long y = tb.s; y <= tb.n; y++)
      {
        tiles.add(TMSUtils.tileid(x, y, zoomlevel));
      }
    }
    return tiles;
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
