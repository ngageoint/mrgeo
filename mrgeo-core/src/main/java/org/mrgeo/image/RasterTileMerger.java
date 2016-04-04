/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.image;

import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TileNotFoundException;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a utility class to merge tiles together from an image. It contains convenience methods to
 * merge tiles using their tile-ids, bounds, etc.
 */
public class RasterTileMerger
{
private static final Logger log = LoggerFactory.getLogger(RasterTileMerger.class);

public static Set<Long> getTileIdsFromBounds(final MrsImage image, final Bounds bounds)
{
  return getTileIdsFromBounds(bounds, image.getZoomlevel(), image.getTilesize());
}

public static Set<Long> getTileIdsFromBounds( final Bounds bounds, final int zoomlevel, final int tilesize)
{
  final TileBounds tb = TMSUtils.boundsToTile(bounds, zoomlevel, tilesize);

  // we used to check if the tx/ty was within the image, but that was removed because when we
  // send in a bounds, we really need an image that matches those bounds (in tile space),
  // with nodata in the missing tile areas.

  // create a list of all tileIds for the given bounding box.
  final Set<Long> tileIds = new HashSet<>();

  for (long tx = tb.w; tx <= tb.e; tx++)
  {
    for (long ty = tb.s; ty <= tb.n; ty++)
    {
      final long tileid = TMSUtils.tileid(tx, ty, zoomlevel);

      log.debug("mergeTiles adding tile {}, {} ({})", tx, ty, tileid);

      tileIds.add(tileid);
    }
  }
  log.debug("mergeTiles added {} tiles", tileIds.size());
  return tileIds;
}

public static WritableRaster mergeTiles(final MrsImage image)
{
  return mergeTiles(image, image.getTileBounds());
}

public static WritableRaster mergeTiles(final MrsImage image, final Bounds bounds)
{
  final TileBounds tb = TMSUtils.boundsToTile(bounds, image
      .getZoomlevel(), image.getTilesize());

  return RasterTileMerger.mergeTiles(image, tb);
}

public static WritableRaster mergeTiles(final MrsImage image, final long[] tiles)
{
  final Tile[] tileids = new Tile[tiles.length];
  for (int i = 0; i < tiles.length; i++)
  {
    tileids[i] = TMSUtils.tileid(tiles[i], image.getZoomlevel());
  }

  return RasterTileMerger.mergeTiles(image, tileids);
}

public static WritableRaster mergeTiles(final MrsImage image, final LongRectangle tileBounds)
{
  final TileBounds tb = new TileBounds(tileBounds.getMinX(), tileBounds
      .getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY());
  return RasterTileMerger.mergeTiles(image, tb);
}

public static WritableRaster mergeTiles(final MrsImage image, final TileIdWritable[] tiles)
{
  final Tile[] tileids = new Tile[tiles.length];
  for (int i = 0; i < tiles.length; i++)
  {
    tileids[i] = TMSUtils.tileid(tiles[i].get(), image.getZoomlevel());
  }

  return RasterTileMerger.mergeTiles(image, tileids);
}

public static WritableRaster mergeTiles(final MrsImage image, final Tile[] tiles)
{
  final int zoom = image.getZoomlevel();
  final int tilesize = image.getTilesize();

  // 1st calculate the pixel size of the merged image.
  Bounds imageBounds = null;
  WritableRaster merged = null;

  for (final Tile tile : tiles)
  {
    log.debug("tx: {} ty: {}", tile.tx, tile.ty);
    final Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, tilesize);
    try
    {
      // expand the image bounds by the tile
      if (imageBounds == null)
      {
        imageBounds = tb;
      }
      else
      {
        imageBounds = imageBounds.expand(tb);
      }

    }
    catch (final TileNotFoundException e)
    {
      // bad tile - tile could be out of bounds - ignore it
    }
  }

  if (imageBounds == null)
  {
    throw new MrsImageException("Error, could not calculate the bounds of the tiles");
  }

  final Pixel ul = TMSUtils.latLonToPixelsUL(imageBounds.n, imageBounds.w, image
      .getZoomlevel(), image.getTilesize());
  final Pixel lr = TMSUtils.latLonToPixelsUL(imageBounds.s, imageBounds.e, image
      .getZoomlevel(), image.getTilesize());

  for (final Tile tile : tiles)
  {
    final Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, image.getZoomlevel(),
        image.getTilesize());

    // calculate the starting pixel for the source
    // make sure we use the upper-left lat/lon
    final Pixel start = TMSUtils.latLonToPixelsUL(bounds.n, bounds.w, image
        .getZoomlevel(), image.getTilesize());

    Raster source;
    try
    {
      source = image.getTile((int) tile.tx, (int) tile.ty);

      if (source != null)
      {
        log.debug("Tile {}, {} with bounds {}, {}, {}, {} pasted onto px {} py {}", tile.tx,
            tile.ty, bounds.w, bounds.s, bounds.e, bounds.n, start.px - ul.px, start.py - ul.py);

        if (merged == null)
        {
          final int width = (int) (lr.px - ul.px);
          final int height = (int) (lr.py - ul.py);

          log.debug("w: {} h: {}", width, height);

          final SampleModel model = source.getSampleModel().createCompatibleSampleModel(width,
              height);

          merged = Raster.createWritableRaster(model, null);

          // Initialize the full raster to the default value for the image
          final double[] defaultValue = image.getMetadata().getDefaultValues();
          if (defaultValue != null && defaultValue.length > 0)
          {
            final double[] defaultRow = new double[merged.getWidth()];
            Arrays.fill(defaultRow, defaultValue[0]);
            for (int y = merged.getMinY(); y < merged.getMinY() + merged.getHeight(); y++)
            {
              merged.setSamples(merged.getMinX(), y, merged.getWidth(), 1, 0, defaultRow);
            }
          }
        }

        merged.setDataElements((int) (start.px - ul.px), (int) (start.py - ul.py), source);
      }
    }
    catch (final TileNotFoundException e)
    {
      // bad tile - tile could be out of bounds - ignore it
    }

  }

  return merged;
}

public static WritableRaster
mergeTiles(final MrsImage image, final TileBounds tileBounds)
{
  final int zoom = image.getZoomlevel();
  final int tilesize = image.getTilesize();

  // 1st calculate the pixel size of the merged image.
  final Bounds imageBounds = TMSUtils.tileToBounds(tileBounds, zoom, tilesize);
  WritableRaster merged;

  final Pixel ul = TMSUtils.latLonToPixelsUL(imageBounds.n, imageBounds.w, image
      .getZoomlevel(), image.getTilesize());

  final Pixel lr = TMSUtils.latLonToPixelsUL(imageBounds.s, imageBounds.e, image
      .getZoomlevel(), image.getTilesize());

  final int width = (int) (lr.px - ul.px);
  final int height = (int) (lr.py - ul.py);

  log.debug("w: {} h: {}", width, height);

  try
  {
    final Raster sample = image.getAnyTile();
    final SampleModel model = sample.getSampleModel().createCompatibleSampleModel(width,
        height);

    merged = Raster.createWritableRaster(model, null);

  }
  catch (IOException e)
  {
    throw new MrsImageException(
        "Catastrophic error merging tiles!  Can't create empty merged image", e);
  }
  // Initialize the full raster to the default value for the image
  final double[] defaultValue = image.getMetadata().getDefaultValues();
  if (defaultValue != null && defaultValue.length > 0)
  {
    final double[] defaultRow = new double[merged.getWidth()];
    Arrays.fill(defaultRow, defaultValue[0]);
    for (int y = merged.getMinY(); y < merged.getMinY() + merged.getHeight(); y++)
    {
      merged.setSamples(merged.getMinX(), y, merged.getWidth(), 1, 0, defaultRow);
    }
  }

  log.debug("Merging tiles: zoom: {}  {}, {} ({}) to {}, {} ({})", zoom, tileBounds.w, tileBounds.s,
      TMSUtils.tileid(tileBounds.w, tileBounds.s, zoom),
      tileBounds.e, tileBounds.n, TMSUtils.tileid(tileBounds.e, tileBounds.n, zoom));

  // the iterator is _much_ faster than requesting individual tiles...
  // final KVIterator<TileIdWritable, Raster> iter = image.getTiles(TileBounds
  // .convertToLongRectangle(tileBounds));
  for (long row = tileBounds.s; row <= tileBounds.n; row++)
  {
    final TileIdWritable rowStart = new TileIdWritable(TMSUtils.tileid(tileBounds.w, row, zoom));
    final TileIdWritable rowEnd = new TileIdWritable(TMSUtils.tileid(tileBounds.e, row, zoom));

    final KVIterator<TileIdWritable, Raster> iter = image.getTiles(rowStart, rowEnd);
    while (iter.hasNext())
    {
      final Raster source = iter.currentValue();
      if (source != null)
      {
        final Tile tile = TMSUtils.tileid(iter.currentKey().get(), zoom);

        final Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, tilesize);

        // calculate the starting pixel for the source
        // make sure we use the upper-left lat/lon
        final Pixel start = TMSUtils
            .latLonToPixelsUL(bounds.n, bounds.w, zoom, tilesize);

        log.debug("Tile {}, {} with bounds {}, {}, {}, {} pasted onto px {} py {}", tile.tx,
            tile.ty, bounds.w, bounds.s, bounds.e, bounds.n, start.px - ul.px, start.py - ul.py);


        // stamp in the source tile.
        merged.setDataElements((int) (start.px - ul.px), (int) (start.py - ul.py), source);
      }
    }
    if (iter instanceof CloseableKVIterator)
    {
      try
      {
        ((CloseableKVIterator)iter).close();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
    }
  }
  return merged;
}
}
