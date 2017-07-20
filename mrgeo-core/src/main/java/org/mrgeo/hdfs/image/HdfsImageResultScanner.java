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

package org.mrgeo.hdfs.image;

import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.Splits;
import org.mrgeo.hdfs.tile.Splits.SplitException;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils.MapFileReaderWrapper;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * HdfsResultScanner is for pulling items from the data store / MapFiles in HDFS in a Iterator
 * format.
 */
class HdfsImageResultScanner implements CloseableKVIterator<TileIdWritable, MrGeoRaster>
{
private static final Logger log = LoggerFactory.getLogger(HdfsImageResultScanner.class);

// reader used for pulling items
private final HdfsMrsImageReader reader;
private final long rowStart;
private final long rowEnd;
private final int zoom;
// hdfs specific reader
private MapFileReaderWrapper mapfile;
// keep track of where the reader is
private int curPartitionIndex;
// return item
private RasterWritable currentValue;
// keep track of where things are
private TileIdWritable currentKey;
// stop condition
private TileIdWritable endKey;

// private final TileIdPartitioner partitioner;
// workaround for MapFile.Reader.seek behavior
private boolean readFirstKey;

HdfsImageResultScanner(final LongRectangle bounds,
    final HdfsMrsImageReader reader)
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
 * @param startKey start of the list of tiles to pull
 * @param endKey   end (inclusive) of tile to pull
 * @param reader   the reader being used
 */
HdfsImageResultScanner(final TileIdWritable startKey, final TileIdWritable endKey,
    final HdfsMrsImageReader reader)
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
public void close() throws IOException
{
  if (mapfile != null)
  {
    mapfile.close();
    mapfile = null;
  }
}

@Override
public TileIdWritable currentKey()
{
  // TODO eaw Should probably have a null check, especially because key will be null if the tile was not found
  // don't reuse the tileidwritable, spark persist() doesn't like it...
  return new TileIdWritable(currentKey);
}

@Override
public MrGeoRaster currentValue()
{
  try
  {
    return RasterWritable.toMrGeoRaster(currentValue);
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
    if (mapfile == null)
    {
      throw new MrsImageException("Mapfile.Reader has been closed");
    }

    if (currentKey == null)
    {
      return false;
    }

    // TODO eaw - This causes the first call to hasNext after the primeScanner to not advance the key.  This means 2 calls
    //            to has next are needed in order to get past the first element
    if (readFirstKey)
    {
      readFirstKey = false;
      // handle boundary cases: startKey >= endKey
      return (currentKey.compareTo(endKey) <= 0);
    }

      /*
       * 1. found = readers[curPartitionIndex].next(currentKey, value) 2. if !found increment
       * curPartitionIndex, ensure that its within limits, and run 1. again. if its not within limits,
       * return false 3. if currentKey <= endKey return true, else return false;
       */
    while (true)
    {
      // TODO eaw - The contract on java.util.Iterator requires that this method implementation not advance the iterator.
      //            This code should be on next()
      final boolean found = mapfile.getReader().next(currentKey, currentValue);
      if (found)
      {
        if (currentKey.compareTo(endKey) <= 0)
        {
          // only need to check start/end tx if we've set the zoom...
          if (zoom > 0)
          {
            // if we fall within the boundries return positive, otherwise slerp up the tile and
            // try the next one.
            final Tile t = TMSUtils.tileid(currentKey.get(), zoom);
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
        if (++curPartitionIndex >= reader.getMaxPartitions())
        {
          return false;
        }
        if (!reader.canBeCached())
        {
          mapfile.close();
        }

        mapfile = reader.getReaderWrapper(curPartitionIndex);
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
public MrGeoRaster next()
{
  if (currentKey == null)
  {
    throw new NoSuchElementException();
  }

  try
  {
    return RasterWritable.toMrGeoRaster(currentValue);
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
  if (key.compareTo(endKey) <= 0)
  {
    // only need to check start/end tx if we've set the zoom...
    if (zoom > 0)
    {
      // if we fall within the boundries return positive, otherwise slerp up the tile and
      // try the next one.
      final Tile t = TMSUtils.tileid(key.get(), zoom);
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

@SuppressWarnings({"unchecked", "squid:S1166"}) // Splits.SplitException is caught and handled
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
  log.debug("start tile id: " + startTileId);
  try
  {
    if (mapfile != null)
    {
      mapfile.close();
      mapfile = null;
    }

    // find the partition containing the first key in the range
    // if found, set curPartitionIndex to its partition
    curPartitionIndex = -1;
    int partitionIndex = reader.getPartitionIndex(new TileIdWritable(startTileId));
    TileIdWritable startKey = null;
    while (curPartitionIndex == -1 && partitionIndex < reader.getMaxPartitions())
    {
      if (mapfile != null)
      {
        mapfile.close();
      }
      mapfile = reader.getReaderWrapper(partitionIndex);
      try
      {
        // We need the mapfile in order to do the following, but we only
        // need to do it once.
        if (startKey == null)
        {
          startKey = (TileIdWritable) mapfile.getReader().getKeyClass().newInstance();
          startKey.set(startTileId);
          endKey = (TileIdWritable) mapfile.getReader().getKeyClass().newInstance();
          endKey.set(endTileId);
          // Because package names for some of our Writable value classes changed,
          // we need to create the
          currentValue = (RasterWritable) mapfile.getReader().getValueClass().newInstance();
        }
      }
      catch (InstantiationException | IllegalAccessException e)
      {
        throw new MrsImageException(e);
      }
      currentKey = (TileIdWritable) mapfile.getReader().getClosest(startKey, currentValue);
      if (currentKey != null)
      {
        // Did we get a key and have we not run past the end key
        // TODO eaw - inRange does the first part of this check, so the first condition can be dropped
        if ((currentKey.compareTo(endKey) <= 0) && inRange(currentKey))
        {
          readFirstKey = true;
          curPartitionIndex = partitionIndex;
        }
        else
        {
          currentKey = null;
          return;
        }
      }
      else
      {
        partitionIndex++;
      }
    }
  }
  // if the tiles are out of bounds, this exception will be thrown.
  catch (SplitException se)
  {
    currentKey = null;
  }
  catch (final IOException e)
  {
    throw new MrsImageException(e);
  }
}
}
