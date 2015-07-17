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

package org.mrgeo.hdfs.tile;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.mrgeo.data.KVIterator;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;

/**
 * HdfsResultScanner is for pulling items from the data store / MapFiles in HDFS in a Iterator
 * format.
 */
public abstract class HdfsTileResultScanner<T, TWritable extends Writable> implements KVIterator<TileIdWritable, T>
{
  // reader used for pulling items
  private final HdfsMrsTileReader<T, TWritable> reader;

  // hdfs specific reader
  private MapFile.Reader mapfile;

  // keep track of where the reader is
  private int curPartition;

  // return item
  private TWritable currentValue;

  // keep track of where things are
  private TileIdWritable currentKey;

  // stop condition
  private TileIdWritable endKey;

  private final long rowStart;
  private final long rowEnd;
  private final int zoom;

  // private final TileIdPartitioner partitioner;

  // workaround for MapFile.Reader.seek behavior
  private boolean readFirstKey;

  // For image: return RasterWritable.toRaster(currentValue)
  protected abstract T toNonWritable(TWritable val) throws IOException;
  
  public HdfsTileResultScanner(final LongRectangle bounds,
      final HdfsMrsTileReader<T, TWritable> reader)
  {
    this.reader = reader;

    rowStart = bounds.getMinX();
    rowEnd = bounds.getMaxX();
    zoom = reader.getZoomlevel();

    primeScanner(TMSUtils.tileid(bounds.getMinX(), bounds.getMinY(), zoom),
        TMSUtils.tileid(bounds.getMaxX(), bounds.getMaxY(), zoom));
  }

  /**
   * Constructor will initialize the conditions for pulling tiles
   * 
   * @param startKey
   *          start of the list of tiles to pull
   * @param endKey
   *          end (inclusive) of tile to pull
   * @param reader
   *          the reader being used
   */
  public HdfsTileResultScanner(final TileIdWritable startKey, final TileIdWritable endKey,
    final HdfsMrsTileReader<T, TWritable> reader)
  {
    // this.partitions = partitions;
    this.reader = reader;

    // initialize startKey
    long startTileId = (startKey == null) ? Long.MIN_VALUE : startKey.get();
    long endTileId = (endKey == null) ? Long.MAX_VALUE : endKey.get();

    rowStart = Long.MIN_VALUE;
    rowEnd = Long.MAX_VALUE;
    zoom = -1;

    primeScanner(startTileId, endTileId);

  }

  @Override
  public TileIdWritable currentKey()
  {
    // don't reuse the tileidwritable, spark persist() doesn't like it...
    return new TileIdWritable(currentKey);
  }

  @Override
  public T currentValue()
  {
    try
    {
      return toNonWritable(currentValue);
    }
    catch (final IOException e)
    {
      throw new MrsImageException(e);
    }
  }

  /**
   * Keeping track of what is being sent back and possibly finish using one MapFile and then
   * opening another MapFile if needed.
   */
  @Override
  public boolean hasNext()
  {
    try
    {
      if (currentKey == null)
      {
        return false;
      }

      if (readFirstKey)
      {
        readFirstKey = false;
        // handle boundary cases: startKey >= endKey
        if (currentKey.compareTo(endKey) <= 0)
        {
          return true;
        }
        return false;
      }

      /*
       * 1. found = readers[curPartition].next(currentKey, value) 2. if !found increment
       * curPartition, ensure that its within limits, and run 1. again. if its not within limits,
       * return false 3. if currentKey <= endKey return true, else return false;
       */
      while (true)
      {
        final boolean found = mapfile.next(currentKey, currentValue);
        if (found)
        {
          if (currentKey.compareTo(endKey) <= 0)
          {
            // only need to check start/end tx if we've set the zoom...
            if (zoom > 0)
            {
              // if we fall within the boundries return positive, otherwise slerp up the tile and
              // try the next one.
              final TMSUtils.Tile t = TMSUtils.tileid(currentKey.get(), zoom);
              if (t.tx >= rowStart && t.tx <= rowEnd)
              {
                return true;
              }
            }
            else
            {
              return true;
            }
          }
          else
          {
            return false;
          }
        }
        else
        {
          if (++curPartition >= reader.getMaxPartitions())
          {
            return false;
          }
          if (!reader.isCachingEnabled() && mapfile != null)
          {
            mapfile.close();
          }
          mapfile = reader.getReader(curPartition);
        }
      }
    }
    catch (final IOException e)
    {
      throw new MrsImageException(e);
    }
  }

  /**
   * Get the value from the MapFile and prepares the Raster output.
   */
  @Override
  public T next()
  {
    try
    {
      return toNonWritable(currentValue);
    }
    catch (final IOException e)
    {
      throw new MrsImageException(e);
    }
  }

  /**
   * The remove method does nothing
   */
  @Override
  public void remove()
  {
    throw new UnsupportedOperationException("iterator is read-only");
  }

  private boolean inRange(TileIdWritable key)
  {
    if (currentKey.compareTo(endKey) <= 0)
    {
      // only need to check start/end tx if we've set the zoom...
      if (zoom > 0)
      {
        // if we fall within the boundries return positive, otherwise slerp up the tile and
        // try the next one.
        final TMSUtils.Tile t = TMSUtils.tileid(currentKey.get(), zoom);
        if (t.tx >= rowStart && t.tx <= rowEnd)
        {
          return true;
        }
      }
      else
      {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private void primeScanner(final long startTileId, final long endTileId)
  {
    /*
     * Workaround for MapFile.Reader.seek ----------------------------------- Normally,
     * Reader.seek() should position the pointer at the beginning of a row, so that Reader.next()
     * should return that row. Instead, this seek() positions it at the end. Since Reader.next()
     * would return the row after, we lose the first row. As a workaround, we call getClosest to
     * position the pointer at the end of the first row and also return the first row in the
     * next() method below.
     */
    try
    {
      // find the partition containing the first key in the range
      // if found, set curPartition to its partition
      curPartition = -1;
      int partition = reader.getPartition(new TileIdWritable(startTileId));
      TileIdWritable startKey = null;
      while (curPartition == -1 && partition < reader.getMaxPartitions())
      {
        if (!reader.isCachingEnabled() && mapfile != null)
        {
          mapfile.close();
        }
        mapfile = reader.getReader(partition);
        try
        {
          // We need the mapfile in order to do the following, but we only
          // need to do it once.
          if (startKey == null)
          {
            startKey = (TileIdWritable)mapfile.getKeyClass().newInstance();
            startKey.set(startTileId);
            endKey = (TileIdWritable)mapfile.getKeyClass().newInstance();
            endKey.set(endTileId);
            // Because package names for some of our Writable value classes changed,
            // we need to create the 
            currentValue = (TWritable)mapfile.getValueClass().newInstance();
          }
        }
        catch (InstantiationException e)
        {
          throw new MrsImageException(e);
        }
        catch (IllegalAccessException e)
        {
          throw new MrsImageException(e);
        }
        currentKey = (TileIdWritable) mapfile.getClosest(startKey, currentValue);
        if (currentKey != null && inRange(currentKey))
        {
          readFirstKey = true;
          curPartition = partition;
        }
        else
        {
          partition++;
        }
      }
    }
    // if the tiles are out of bounds, this exception will be thrown.
    catch (Splits.SplitException se)
    {
      currentKey = null;
    }
    catch (final IOException e)
    {
      throw new MrsImageException(e);
    }
  }
}
