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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsPyramidReaderContext;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.image.MrsImageReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TileNotFoundException;
import org.mrgeo.utils.*;
import org.mrgeo.utils.tms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author tim.tisler
 *
 */
public class MrsImage implements AutoCloseable
{

private static final Logger log = LoggerFactory.getLogger(MrsImage.class);

// create a simple color model
//protected ColorModel colorModel = null;
//protected SampleModel sampleModel = null;
//protected String measurement = "Ratio"; // not sure what this is for...
private MrsImageReader reader = null; // The MrsImageReader for fetching the tiles
private MrsPyramidMetadata metadata = null; // image metadata

private int zoomlevel = -1; // current zoom level of this image
private int tilesize = -1; // size of a tile (here for convenience, it is in the metadata)
private MrsImageDataProvider provider;
private MrsPyramidReaderContext context;

private MrsImage(MrsImageDataProvider provider, final int zoomlevel)
{
  context = new MrsPyramidReaderContext();
  context.setZoomlevel(zoomlevel);
  this.provider = provider;

  openReader();

  if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
  {
    LeakChecker.instance().add(
        this,
        ExceptionUtils.getStackTrace(new Throwable(
            "MrsImage creation stack(ignore the Throwable...)")));
  }
}

public static MrsImage open(MrsImageDataProvider provider, final int zoomlevel) throws IOException
{
  try
  {
    MrsPyramidMetadata meta = provider.getMetadataReader(null).read();
    // Check to see if there is an image at this zoom level before opening
    final String name = meta.getName(zoomlevel);
    if (name != null)
    {
      return new MrsImage(provider, zoomlevel);
    }
  }
  // TODO this seems weird to catch and eat these here...
  catch (final MrsImageException | NullPointerException e)
  {
    // e.printStackTrace();
  }

  return null;
}


public static MrsImage open(final String name, final int zoomlevel,
    final ProviderProperties providerProperties) throws IOException
{
  if (name == null)
  {
    throw new IOException("Unable to open image. Resource name is empty.");
  }
  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(name,
      AccessMode.READ, providerProperties);
  return open(dp, zoomlevel);
}

/**
 * Releases any resources held by this image, in particular its reader. Be sure to call this after
 * done using an MrsImage, and if necessary, in a finally block of a try/catch/finally
 */
public void close()
{
  if (reader != null)
  {
    LeakChecker.instance().remove(this);

    reader.close();
    reader = null;
  }
}

public LatLng convertToLatLng(final double px, final double py)
{
  return new LatLng(convertToWorldY(py), convertToWorldX(px));
}

public LatLng convertToPixelCenterLatLng(final double px, final double py)
{
  return new LatLng(convertToWorldY(py + 0.5), convertToWorldX(px + 0.5));
}

public final double convertToWorldX(final double px)
{
  final double resolution = TMSUtils.resolution(getZoomlevel(), getTilesize());
  final Bounds bounds = getMetadata().getBounds();

  return bounds.w + (px * resolution);
}

public final double convertToWorldY(final double py)
{
  final double resolution = TMSUtils.resolution(getZoomlevel(), getTilesize());
  final Bounds bounds = getMetadata().getBounds();

  return bounds.s + ((getPixelMaxY() - py) * resolution);
}

public MrGeoRaster getAnyTile() throws IOException
{
  if (reader == null)
  {
    openReader();
  }
  final Iterator<MrGeoRaster> it = reader.get();
  try
  {
    return it.next();
  }
  finally
  {
    if (!reader.canBeCached() && it instanceof Closeable)
    {
      ((Closeable) it).close();
      reader.close();
      reader = null;
    }
  }
}

public Bounds getBounds()
{
  return getMetadata().getBounds();
}

public Bounds getBounds(final int tx, final int ty)
{
  final double bounds[] = TMSUtils.tileSWNEBoundsArray(tx, ty, getZoomlevel(), getTilesize());

  return new Bounds(bounds[0], bounds[1], bounds[2], bounds[3]);
}


/**
 * Retrieves the overall minimum and maximum raster values in the source data
 *
 * @return array with the overall minimum value in first position and overall maximum value in the
 *         second
 */
public double[] getExtrema()
{
  final double[] extrema = new double[3];
  final ImageStats stats = getMetadata().getImageStats(getZoomlevel(), 0);
  if (stats != null)
  {
    extrema[1] = stats.max;
    extrema[0] = Math.max(0.0, stats.min);
  }
  else
  {
    extrema[1] = 1.0;
    extrema[0] = 0.0;
  }
  return extrema;
}

public long getHeight()
{
  return getMetadata().getPixelBounds(getZoomlevel()).getHeight();
}


public long getMaxTileX()
{
  return getMetadata().getTileBounds(getZoomlevel()).getMaxX();
}

public long getMaxTileY()
{
  return getMetadata().getTileBounds(getZoomlevel()).getMaxY();
}

public int getMaxZoomlevel()
{
  return getMetadata().getMaxZoomLevel();
}

public MrsPyramidMetadata getMetadata()
{
  if (metadata == null)
  {
    try
    {
      metadata = provider.getMetadataReader().read();
    }
    catch (IOException e)
    {
      log.error("Unable to read metadata for " + provider.getResourceName(), e);
    }
  }
  return metadata;
}

public long getMinTileX()
{
  return getMetadata().getTileBounds(getZoomlevel()).getMinX();
}

public long getMinTileY()
{
  return getMetadata().getTileBounds(getZoomlevel()).getMinY();
}

public String getName()
{
  return metadata.getName(getZoomlevel());
}

public long getNumXTiles()
{
  return getMetadata().getTileBounds(getZoomlevel()).getWidth();
}

public long getNumYTiles()
{
  return getMetadata().getTileBounds(getZoomlevel()).getHeight();
}

public long getPixelMaxX()
{
  return getMetadata().getPixelBounds(getZoomlevel()).getMaxX();
}

public long getPixelMaxY()
{
  return getMetadata().getPixelBounds(getZoomlevel()).getMaxY();
}

public long getPixelMinX()
{
  return getMetadata().getPixelBounds(getZoomlevel()).getMinX();
}

public long getPixelMinY()
{
  return getMetadata().getPixelBounds(getZoomlevel()).getMinY();
}

public LongRectangle getPixelRect()
{
  return new LongRectangle(getPixelMinX(), getPixelMinY(), getWidth(), getHeight());
}


public MrGeoRaster getTile(final long tx, final long ty) throws TileNotFoundException
{
  if (tx < getMinTileX() || tx > getMaxTileX() || ty < getMinTileY() || ty > getMaxTileY())
  {
    final String msg = String.format(
        "Tile x/y out of range. (%d, %d) range: (%d, %d) to (%d, %d) (inclusive)", tx, ty,
        getMinTileX(), getMinTileY(), getMaxTileX(), getMaxTileY());
    throw new TileNotFoundException(msg);
  }
  if (reader == null)
  {
    openReader();
  }

  return reader.get(new TileIdWritable(TMSUtils.tileid(tx, ty, getZoomlevel())));
}

public LongRectangle getTileBounds()
{
  return getMetadata().getTileBounds(getZoomlevel());
}

public int getTileHeight()
{
  return getTilesize();
}

public KVIterator<TileIdWritable, MrGeoRaster> getTiles()
{
  if (reader == null)
  {
    openReader();
  }
  return reader.get();
}

public KVIterator<TileIdWritable, MrGeoRaster> getTiles(final LongRectangle tileBounds)
{
  if (reader == null)
  {
    openReader();
  }
  return reader.get(tileBounds);
}

public KVIterator<TileIdWritable, MrGeoRaster> getTiles(final TileIdWritable start,
    final TileIdWritable end)
{
  if (reader == null)
  {
    openReader();
  }
  return reader.get(start, end);
}

public int getTilesize()
{
  if (tilesize < 0)
  {
    tilesize = getMetadata().getTilesize();
  }
  return tilesize;
}

public int getTileType()
{
  return getMetadata().getTileType();
}

public int getTileWidth()
{
  return getTilesize();
}

public long getWidth()
{
  return getMetadata().getPixelBounds(getZoomlevel()).getWidth();
}

public int getZoomlevel()
{
  if (zoomlevel < 0)
  {
    if (reader == null)
    {
      openReader();
    }
    zoomlevel = reader.getZoomlevel();
  }
  return zoomlevel;
}

public boolean isTileEmpty(final long tx, final long ty)
{
  if (reader == null)
  {
    openReader();
  }
  return !reader.exists(new TileIdWritable(TMSUtils.tileid(tx, ty, getZoomlevel())));
}


@Override
public String toString()
{
  return getClass().getSimpleName() + ": " + getMetadata().getPyramid() + ":" + getZoomlevel();
}

// log an error if finalize() is called and close() was not called
@Override
protected void finalize() throws Throwable
{
  try
  {
    if (reader != null)
    {
      log.info("MrsImage.finalize(): looks like close() was not called. This is a potential " +
          "memory leak. Please call close(). The MrsImage points to \"" + metadata.getPyramid() +
          "\" with zoom level " + getZoomlevel());
    }
  }
  finally
  {
    super.finalize();
  }
}

private void openReader()
{
  try
  {
    if (reader != null )
    {
      reader.close();
    }

    reader = provider.getMrsTileReader(context);
    if (reader == null)
    {
      throw new MrsImageException("Error Reading Image");
    }
  }
  catch (IOException e)
  {
    throw new MrsImageException(e);
  }
}

public Set<Long> getTileIdsFromBounds(final Bounds bounds)
{
  return getTileIdsFromBounds(bounds, getZoomlevel(), getTilesize());
}

public static Set<Long> getTileIdsFromBounds(final Bounds bounds, final int zoomlevel, final int tilesize)
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

      log.debug("getRaster adding tile {}, {} ({})", tx, ty, tileid);

      tileIds.add(tileid);
    }
  }
  log.debug("getRaster added {} tiles", tileIds.size());
  return tileIds;
}

public MrGeoRaster getRaster() throws MrGeoRaster.MrGeoRasterException
{
  return getRaster(getTileBounds());
}

public MrGeoRaster getRaster(final Bounds bounds) throws MrGeoRaster.MrGeoRasterException
{
  final TileBounds tb = TMSUtils.boundsToTile(bounds, getZoomlevel(), getTilesize());

  return getRaster(tb);
}

public MrGeoRaster getRaster(final long[] tiles) throws MrGeoRaster.MrGeoRasterException
{
  final Tile[] tileids = new Tile[tiles.length];
  for (int i = 0; i < tiles.length; i++)
  {
    tileids[i] = TMSUtils.tileid(tiles[i], getZoomlevel());
  }

  return getRaster(tileids);
}

public MrGeoRaster getRaster(final LongRectangle tileBounds) throws MrGeoRaster.MrGeoRasterException
{
  final TileBounds tb = new TileBounds(tileBounds.getMinX(), tileBounds
      .getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY());
  return getRaster(tb);
}

public MrGeoRaster getRaster(final TileIdWritable[] tiles) throws MrGeoRaster.MrGeoRasterException
{
  final Tile[] tileids = new Tile[tiles.length];
  for (int i = 0; i < tiles.length; i++)
  {
    tileids[i] = TMSUtils.tileid(tiles[i].get(), getZoomlevel());
  }

  return getRaster(tileids);
}

public MrGeoRaster getRaster(final Tile[] tiles) throws MrGeoRaster.MrGeoRasterException
{
  getMetadata(); // make sure metadata is loaded

  final int tilesize = metadata.getTilesize();

  // 1st calculate the pixel size of the merged image.
  Bounds imageBounds = null;

  int zoomlevel = getZoomlevel();
  for (final Tile tile : tiles)
  {
    log.debug("tx: {} ty: {}", tile.tx, tile.ty);
    final Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);
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

  final Pixel ul = TMSUtils.latLonToPixelsUL(imageBounds.n, imageBounds.w, zoomlevel, tilesize);
  final Pixel lr = TMSUtils.latLonToPixelsUL(imageBounds.s, imageBounds.e, zoomlevel, tilesize);

  MrGeoRaster merged = MrGeoRaster.createEmptyRaster((int) (lr.px - ul.px), (int) (lr.py - ul.py),
      metadata.getBands(), metadata.getTileType());
  merged.fill(metadata.getDefaultValuesDouble());

  for (final Tile tile : tiles)
  {
    final Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);

    // calculate the starting pixel for the source
    // make sure we use the upper-left lat/lon
    final Pixel start = TMSUtils.latLonToPixelsUL(bounds.n, bounds.w, zoomlevel, tilesize);

    try
    {
      MrGeoRaster source = getTile((int) tile.tx, (int) tile.ty);

      if (source != null)
      {
        log.debug("Tile {}, {} with bounds {}, {}, {}, {} pasted onto px {} py {}", tile.tx,
            tile.ty, bounds.w, bounds.s, bounds.e, bounds.n, start.px - ul.px, start.py - ul.py);

        merged.copyFrom(0, 0, source.width(), source.height(),
                        source, (int) (start.px - ul.px), (int) (start.py - ul.py));
      }
    }
    catch (final TileNotFoundException e)
    {
      // bad tile - tile could be out of bounds - ignore it
    }

  }

  return merged;
}

public MrGeoRaster getRaster(final TileBounds tileBounds) throws MrGeoRaster.MrGeoRasterException
{
  getMetadata(); // make sure metadata is loaded

  final int tilesize = metadata.getTilesize();

  int zoomlevel = getZoomlevel();
  // 1st calculate the pixel size of the merged image.
  final Bounds imageBounds = TMSUtils.tileToBounds(tileBounds, zoomlevel, tilesize);

  final Pixel ul = TMSUtils.latLonToPixelsUL(imageBounds.n, imageBounds.w, zoomlevel, tilesize);
  final Pixel lr = TMSUtils.latLonToPixelsUL(imageBounds.s, imageBounds.e, zoomlevel, tilesize);


  MrGeoRaster merged = MrGeoRaster.createEmptyRaster((int) (lr.px - ul.px), (int) (lr.py - ul.py),
      metadata.getBands(), metadata.getTileType());
  merged.fill(metadata.getDefaultValuesDouble());


  log.debug("Merging tiles: zoom: {}  {}, {} ({}) to {}, {} ({})", zoomlevel, tileBounds.w, tileBounds.s,
      TMSUtils.tileid(tileBounds.w, tileBounds.s, zoomlevel),
      tileBounds.e, tileBounds.n, TMSUtils.tileid(tileBounds.e, tileBounds.n, zoomlevel));

  // the iterator is _much_ faster than requesting individual tiles...
  // final KVIterator<TileIdWritable, Raster> iter = image.getTiles(TileBounds
  // .convertToLongRectangle(tileBounds));
  for (long row = tileBounds.s; row <= tileBounds.n; row++)
  {
    final TileIdWritable rowStart = new TileIdWritable(TMSUtils.tileid(tileBounds.w, row, zoomlevel));
    final TileIdWritable rowEnd = new TileIdWritable(TMSUtils.tileid(tileBounds.e, row, zoomlevel));

    final KVIterator<TileIdWritable, MrGeoRaster> iter = getTiles(rowStart, rowEnd);
    while (iter.hasNext())
    {
      final MrGeoRaster source = iter.currentValue();
      if (source != null)
      {
        final Tile tile = TMSUtils.tileid(iter.currentKey().get(), zoomlevel);

        final Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);

        // calculate the starting pixel for the source
        // make sure we use the upper-left lat/lon
        final Pixel start = TMSUtils
            .latLonToPixelsUL(bounds.n, bounds.w, zoomlevel, tilesize);

        log.debug("Tile {}, {} with bounds {}, {}, {}, {} pasted onto px {} py {}", tile.tx,
            tile.ty, bounds.w, bounds.s, bounds.e, bounds.n, start.px - ul.px, start.py - ul.py);

        merged.copyFrom(0, 0, source.width(), source.height(),
                        source, (int) (start.px - ul.px), (int) (start.py - ul.py));
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
