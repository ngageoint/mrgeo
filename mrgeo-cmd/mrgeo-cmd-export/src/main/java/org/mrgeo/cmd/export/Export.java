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
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.*;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.tile.TileNotFoundException;
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

  public static Options createOptions()
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

    final Option random = new Option("r", "random", false, "Randomize export");
    random.setRequired(false);
    result.addOption(random);

    final Option single = new Option("s", "single", false, "Export as a single image");
    single.setRequired(false);
    result.addOption(single);

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

  private static Bounds parseBounds(String boundsOption)
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

  private static boolean saveSingleTile(final String output, final MrsImage image, final long ty,
    final long tx)
  {
    try
    {
      final MrsImagePyramidMetadata metadata = image.getMetadata();

      final Raster raster = image.getTile(tx, ty);

      log.info("tx: " + tx + " ty: " + ty);

      if (raster != null)
      {
        GDALUtils.saveRaster(raster, output, tx, ty, image.getZoomlevel(),
            image.getTilesize(), metadata.getDefaultValue(0));
        System.out.println("Wrote output to " + output);
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




  private static boolean saveMultipleTiles(String output, final MrsImage image, final long[] tiles)
  {
    try
    {
      final MrsImagePyramidMetadata metadata = image.getMetadata();

      final Raster raster = RasterTileMerger.mergeTiles(image, tiles);
      Raster sampleRaster = null;

      TMSUtils.Bounds imageBounds = null;

      final int tilesize = image.getTilesize();
      final int zoomlevel = image.getZoomlevel();
      for (final long lid : tiles)
      {
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

      if (!output.endsWith(".tif") || !output.endsWith(".tiff"))
      {
        output += ".tif";
      }
      GDALUtils.saveRaster(raster, output, imageBounds,
          zoomlevel, tilesize, metadata.getDefaultValue(0));

      System.out.println("Wrote output to " + output);
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

    OpImageRegistrar.registerMrGeoOps();
    try
    {
      final Options options = Export.createOptions();
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

        final String outputbase = line.getOptionValue("o");

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

        if (!singleImage)
        {
          org.apache.commons.io.FileUtils.forceMkdir(new File(outputbase));
        }

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
            final String output = outputbase + (all ? "_" + Integer.toString(zoomlevel) : "");

            MrsImage image = null;
            if (imagePyramid != null)
            {
              image = imagePyramid.getImage(zoomlevel);
            }
            try
            {
              final Set<Long> tiles = calculateTiles(pyramid, zoomlevel);

              if (singleImage)
              {
                if (imagePyramid != null)
                {
                  saveMultipleTiles(output, image, ArrayUtils.toPrimitive(tiles.toArray(new Long[tiles.size()])));
                }
              }
              else if (mosaicTiles && mosaicTileCount > 0)
              {
                for (final Long tileid : tiles)
                {
                  final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoomlevel);
                  final Set<Long> tilesToMosaic = new HashSet<Long>();
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
                  final String mosaicOutput = output + "/" + t.ty + "-" + t.tx + "-" +
                      TMSUtils.tileid(t.tx, t.ty, zoomlevel);
                  if (imagePyramid != null)
                  {
                    saveMultipleTiles(mosaicOutput, image,
                      ArrayUtils.toPrimitive(tilesToMosaic.toArray(new Long[tilesToMosaic.size()])));
                  }
                }
              }
              else
              {
                for (final Long tileid : tiles)
                {
                  final TMSUtils.Tile tile = TMSUtils.tileid(tileid, zoomlevel);
                  if (imagePyramid != null)
                  {
                    String imageOutput = output + "/" + tile.ty + "-" + tile.tx + "-" +
                        TMSUtils.tileid(tile.tx, tile.ty, zoomlevel);
                    saveSingleTile(imageOutput, image, tile.ty, tile.tx);
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

  private Set<Long> calculateTiles(final MrsPyramid pyramid, int zoomlevel)
  {
    final Set<Long> tiles = new HashSet<Long>();
    //final int zoomlevel = pyramid.getMaximumLevel();

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
