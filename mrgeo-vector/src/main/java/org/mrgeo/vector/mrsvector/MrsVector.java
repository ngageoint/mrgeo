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

package org.mrgeo.vector.mrsvector;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mrgeo.tile.TileNotFoundException;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.tile.MrsTileException;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LeakChecker;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class MrsVector
{
  private static final Logger log = LoggerFactory.getLogger(MrsVector.class);

  // this will create a max tile of 256M pixels, or 1GB. Simply a sanity check.
  protected static final int MAX_TILE_SIZE = 16384;

  public static final double UNSPECIFIED_DEFAULT = Double.NaN;
  public static final String MEASUREMENT_STRING = "Measurement";

  private MrsTileReader<VectorTile> reader; // The reader for fetching the tiles
  private MrsVectorPyramidMetadata metadata = null; // vector metadata
  
  private String name;

  private int zoomlevel = -1; // current zoom level of this vector
  private int tilesize = -1; // size of a tile (here for convenience, it is in the metadata)

  // TODO: This constructor should take an instance of a vector provider,
  // but that doesn't exist yet.
  private MrsVector(final String vector)
  {
    name = vector;
    
    try
    {
      // TODO: This should use a vector provider
      reader = new HdfsMrsVectorReader(vector);
    }
    catch (IOException e)
    {
      String msg = "Error creating vector reader for " + vector;
      log.error(msg, e);
      throw new MrsTileException(msg);
    }

    if (reader == null)
    {
      throw new MrsTileException("Error Reading Vector " + vector);
    }

    if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
    {
      LeakChecker.instance().add(
        this,
        ExceptionUtils.getStackTrace(new Throwable(
          "MrsVector creation stack(ignore the Throwable...)")));
    }
  }

  private MrsVector(final String vector, final MrsVectorPyramidMetadata meta)
  {
    this(vector);
    metadata = meta;
  }

  public static MrsVector open(final MrsVectorPyramidMetadata meta, final int zoomlevel)
  {
    try
    {
      final String name = meta.getZoomName(zoomlevel);
      if (name != null)
      {
        final MrsVector vector = new MrsVector(name, meta);
        return vector;
      }
    }
    // TODO this seems weird to catch and eat these here...
    catch (final MrsTileException e)
    {
      e.printStackTrace();
    }
    catch (final NullPointerException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  public static MrsVector open(final String name)
  {
    try
    {
      if (name != null)
      {
        final MrsVector vector = new MrsVector(name);
        return vector;
      }
    }
    // TODO this seems weird to catch and eat these here...
    catch (final MrsTileException e)
    {
      // e.printStackTrace();
    }
    catch (final NullPointerException e)
    {
      // e.printStackTrace();
    }

    return null;
  }

  /**
   * Releases any resources held by this vector, in particular its reader. Be sure to call this after
   * done using an MrsVector, and if necessary, in a finally block of a try/catch/finally
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

  public VectorTile getAnyTile()
  {
    final Iterator<VectorTile> it = reader.get(new TileIdWritable(0),
      new TileIdWritable(Long.MAX_VALUE));
    return it.next();
  }

  /**
   * @return
   */
  public Bounds getBounds()
  {
    return getMetadata().getBounds();
  }

  public Bounds getBounds(final int tx, final int ty)
  {
    final double bounds[] = TMSUtils.tileSWNEBoundsArray(tx, ty, getZoomlevel(), getTilesize());

    return new Bounds(bounds[0], bounds[1], bounds[2], bounds[3]);
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

  public MrsVectorPyramidMetadata getMetadata()
  {
    if (metadata == null)
    {
      if (reader instanceof MrsVectorPyramidMetadataProvider)
      {
        metadata = ((MrsVectorPyramidMetadataProvider)reader).loadMetadata();
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
    return name;
  }
  
  public long getNumXTiles()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getWidth();
  }

  public long getNumYTiles()
  {
    return getMetadata().getTileBounds(getZoomlevel()).getHeight();
  }

  public VectorTile getTile(final long tx, final long ty) throws TileNotFoundException
  {
    if (tx < getMinTileX() || tx > getMaxTileX() || ty < getMinTileY() || ty > getMaxTileY())
    {
      // throw new IllegalArgumentException("x/y out of range. (" + String.valueOf(tx) + ", " +
      // String.valueOf(ty) + ") range: " + String.valueOf(getMinTileX()) + ", " +
      // String.valueOf(getMinTileY()) + " to " + String.valueOf(getMaxTileX()) + ", " +
      // String.valueOf(getMaxTileY()) + " (inclusive)");

      final String msg = String.format(
        "Tile x/y out of range. (%d, %d) range: (%d, %d) to (%d, %d) (inclusive)", tx, ty,
        getMaxTileX(), getMinTileY(), getMaxTileX(), getMaxTileY());
      throw new TileNotFoundException(msg);
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

  public KVIterator<TileIdWritable, VectorTile> getTiles(final LongRectangle tileBounds)
  {
    return reader.get(tileBounds);
  }

  public KVIterator<TileIdWritable, VectorTile> getTiles(final TileIdWritable start,
    final TileIdWritable end)
  {
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

  public int getTileWidth()
  {
    return getTilesize();
  }

  public int getZoomlevel()
  {
    if (zoomlevel < 0)
    {
      zoomlevel = reader.getZoomlevel();
    }
    return zoomlevel;
  }

  public boolean isTileEmpty(final long tx, final long ty)
  {
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
        log.info("MrsVector.finalize(): looks like close() was not called. This is a potential " +
          "memory leak. Please call close(). The MrsVector points to \"" + metadata.getPyramid() +
          "\" with zoom level " + getZoomlevel());
      }
    }
    finally
    {
      super.finalize();
    }
  }
}
