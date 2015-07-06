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

import com.google.common.cache.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.tile.MrsTileException;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * HdfsMrsTileReader is an instance of the MrsTileReader class for communication with an instance of the
 * a pyramid in HDFS scheme. This
 * class must be instantiated from the factory method of getReader().
 */
public abstract class HdfsMrsTileReader<T, TWritable extends Writable> extends MrsTileReader<T>
{
  static final Logger LOG = LoggerFactory.getLogger(HdfsMrsTileReader.class);
  
  private final static int READER_CACHE_SIZE = 100;
  private final static int READER_CACHE_EXPIRE = 10; // minutes

  private final LoadingCache<Integer, MapFile.Reader> readerCache = CacheBuilder.newBuilder()
      .maximumSize(READER_CACHE_SIZE)
     .expireAfterAccess(READER_CACHE_EXPIRE, TimeUnit.SECONDS)
     .removalListener(
     new RemovalListener<Integer, MapFile.Reader>()
     {
       @Override
       public void onRemoval(final RemovalNotification<Integer, MapFile.Reader> notification)
       {
         try
        {
          notification.getValue().close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
       }
     }).build(new CacheLoader<Integer, MapFile.Reader>()
   {

     @Override
     public MapFile.Reader load(final Integer partition) throws IOException
     {
       final FileSplit.FileSplitInfo part =
           (FileSplit.FileSplitInfo) splits.getSplits()[partition];

       final Path path = new Path(imagePath, part.getName());
       final FileSystem fs = path.getFileSystem(conf);

       MapFile.Reader reader = new MapFile.Reader(path, conf);

       if (profile)
       {
         LeakChecker.instance().add(
           reader,
           ExceptionUtils.getFullStackTrace(new Throwable(
             "HdfsMrsVectorReader creation stack(ignore the Throwable...)")));
       }
       LOG.debug("opening (not in cache) partition: {} file: {}", partition, part);
     

     return reader;

     }
   });

  /*
   * HdfsReader globals
   */

  public class BoundsResultScanner implements KVIterator<Bounds, T>
  {
    private KVIterator<TileIdWritable, T> tileIterator;
    private int zoomLevel;
    private int tileSize;

    public BoundsResultScanner(KVIterator<TileIdWritable, T> tileIterator,
        int zoomLevel, int tileSize)
    {
      this.tileIterator = tileIterator;
      this.zoomLevel = zoomLevel;
      this.tileSize = tileSize;
    }

    @Override
    public boolean hasNext()
    {
      return tileIterator.hasNext();
    }

    @Override
    public T next()
    {
      return tileIterator.next();
    }

    @Override
    public void remove()
    {
      tileIterator.remove();
    }

    @Override
    public Bounds currentKey()
    {
      TileIdWritable key = tileIterator.currentKey();
      TMSUtils.Tile tile = TMSUtils.tileid(key.get(), zoomLevel);
      TMSUtils.Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoomLevel, tileSize);
      return TMSUtils.Bounds.convertNewToOldBounds(bounds);
    }

    @Override
    public T currentValue()
    {
      return tileIterator.currentValue();
    }
  }

  final private FileSplit splits = new FileSplit();

  // location of data
  final Path imagePath;

  // Hadoop Configuration for connection to HDFS
  final Configuration conf = HadoopUtils.createConfiguration();

  final boolean profile;

  protected Path getTilePath()
  {
    return imagePath;
  }

  /**
   * The HdfsReader constructor will instantiate the connection to HDFS
   * 
   * @param path
   *          is the location of the data in HDFS
   * @throws IOException 
   */
  public HdfsMrsTileReader(final String path, final int zoom) throws IOException
  {
      // get the Hadoop configuration

      String modifiedPath = path;

      final File file = new File(path);
      if (file.exists())
      {
        modifiedPath = "file://" + file.getAbsolutePath();
      }

      imagePath = new Path(modifiedPath);
      
      FileSystem fs = HadoopFileUtils.getFileSystem(conf, imagePath);
      if (!fs.exists(imagePath))
      {
        throw new IOException("Cannot open HdfsMrsTileReader for " + modifiedPath + ".  Path does not exist." );
      }

      readSplits(modifiedPath);

      // set the profile
      if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
      {
        profile = true;
      }
      else
      {
        profile = false;
      }
  } // end constructor

  @Override
  public long calculateTileCount()
  {
    int count = 0;
    try
    {
      final FileSystem fs = imagePath.getFileSystem(conf);
      final Path[] names = FileUtil.stat2Paths(fs.listStatus(imagePath));
      Arrays.sort(names);
      final DataOutputBuffer key = new DataOutputBuffer();

      for (final Path name : names)
      {
        final FileStatus[] dirFiles = fs.listStatus(name);
        for (final FileStatus dirFile : dirFiles)
        {
          if (dirFile.getPath().getName().equals("index"))
          {
            SequenceFile.Reader index = new SequenceFile.Reader(fs, dirFile.getPath(), conf);
            try
            {
              while (index.nextRawKey(key) >= 0)
              {
                count++;
              }
            }
            finally
            {
              index.close();
            }
          }
        }
      }
      return count;
    }
    catch (final IOException e)
    {
      throw new MrsTileException(e);
    }
  }

  /**
   * This will go through the items in the cache and close all the readers.
   */
  @Override
  public void close()
  {
    readerCache.invalidateAll();
  } // end close

  /**
   * Check if a tile exists in the data.
   */
  @Override
  public boolean exists(final TileIdWritable key)
  {
    // check for a successful retrieval
    return get(key) != null;
  } // end exists

  @Override
  public KVIterator<TileIdWritable, T> get()
  {
    return get(null, null);
  }

  @Override
  public abstract KVIterator<TileIdWritable, T> get(final LongRectangle tileBounds);

  protected abstract int getWritableSize(TWritable val);
  protected abstract T toNonWritable(TWritable val) throws IOException;

  /**
   * Retrieve a tile from the data.
   * 
   * @param key
   *          is the tile to get from the max zoom level
   * @return the data for the tile requested
   */
  @SuppressWarnings("unchecked")
  @Override
  public T get(final TileIdWritable key)
  {
    try
    {
      // get the reader that handles the partition/map file
      final MapFile.Reader reader = getReader(getPartition(key));

      // return object
      TWritable val = (TWritable)reader.getValueClass().newInstance();

      // get the tile from map file from HDFS
      try
      {
        // log.debug("getting " + key);
        reader.get(key, val);
        if (getWritableSize(val) > 0)
        {
          // return the data
          return toNonWritable(val);
        }
      }
      catch (final IllegalStateException e)
      {
        // no-op. Accumulo's Value class will return an IllegalStateException if the reader
        // returned no data, but you try to do a get or getSize. We'll trap it here and return
        // a null for the tile.
      }

      // nothing came back from the map file
      return null;

    }
    catch (final IOException e)
    {
      throw new MrsTileException(e);
    }
    catch (InstantiationException e)
    {
      throw new MrsTileException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new MrsTileException(e);
    }
  } // end get

  /**
   * This will retrieve tiles in a specified range.
   * 
   * @param startKey
   *          the start of the range of tiles to get
   * @param endKey
   *          the end (inclusive) of the range of tiles to get
   * 
   * @return An Iterable object of tile data for the range requested
   */
  @Override
  public abstract KVIterator<TileIdWritable, T> get(final TileIdWritable startKey,
    final TileIdWritable endKey);

  @Override
  public KVIterator<Bounds, T> get(final Bounds bounds)
  {
    TMSUtils.Bounds newBounds = TMSUtils.Bounds.convertOldToNewBounds(bounds);
    TMSUtils.TileBounds tileBounds = TMSUtils.boundsToTile(newBounds, getZoomlevel(),
        getTileSize());
    return new BoundsResultScanner(get(new LongRectangle(
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n)),
        getZoomlevel(), getTileSize());
  }

  private void readSplits(final String parent) throws IOException
  {
    try
    {
      splits.readSplits(new Path(parent));
      return;
    }
    catch(FileNotFoundException fnf)
    {
      // When there is no splits file, the whole image is a single split
    }

    splits.generateSplits(new Path(parent), conf);
  }

  /**
   * Get the number of directories with imagery files
   * 
   * @return the number of data directories
   */
  public int getMaxPartitions()
  {
    return splits.length();
  }

  /**
   * This will return the partition for the tile requested.
   *
   * @param key
   *          the item to find the range for
   * @return the partition of the requested key
   */
  public int getPartition(final TileIdWritable key) throws IOException
  {
    return splits.getSplit(key.get()).getPartition();
  }

  /**
   * This method will Get a MapFile.Reader for the partition specified
   * 
   * @param partition
   *          is the particular reader being accessed
   * @return the reader for the partition specified
   * @throws IOException
   */
  public MapFile.Reader getReader(final int partition) throws IOException
  {
    try
    {
      return readerCache.get(partition);
    }
    catch (ExecutionException e)
    {
      if (e.getCause() instanceof IOException)
      {
        throw (IOException)e.getCause();
      }
      throw new IOException(e);
    }
  } // end getReader

} // end HdfsReader

