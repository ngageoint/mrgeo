/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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
import com.google.common.primitives.Longs;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
       final String part = partitions.get(partition);
       final Path path = new Path(imagePath, part);
       final FileSystem fs = path.getFileSystem(conf);

       MapFile.Reader reader = new MapFile.Reader(fs, path.toString(), conf);

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

//  private static class ReaderLRUCache extends LRUMap
//  {
//
//    // keep track of the version
//    private static final long serialVersionUID = 1L;
//
//    /**
//     * Creates a LRUCache of specified size
//     * 
//     * @param cacheSize
//     *          size of cache to create
//     */
//    public ReaderLRUCache(final int cacheSize)
//    {
//      super(cacheSize);
//    } // end constructor
//
//    /**
//     * Remove an item from the cache. If the reader removed is not null it is closed.
//     * 
//     * @param entry
//     *          is the object to remove from the cache
//     * @return success or failure of operation
//     */
//    @Override
//    protected boolean removeLRU(final AbstractLinkedMap.LinkEntry entry)
//    {
//
//      // get the reader from the entry
//      final MapFile.Reader reader = (Reader) entry.getValue();
//
//      // close the reader
//      if (reader != null)
//      {
//        try
//        {
//          LOG.debug("removing partition: {} file: {}", entry.getKey(), reader.toString());
//
//          reader.close();
//
//          return true; // success
//        }
//        catch (final IOException e)
//        {
//          e.printStackTrace();
//
//          return false; // failure
//        }
//      }
//
//      return false; // failure because reader was null
//
//    } // end removeLRU
//
//  } // end ReaderLRUCache

  /*
   * HdfsReader globals
   */

//  // default cache size
//  private static final int CACHE_SIZE = 20;

  // create the cache
//  @SuppressWarnings("unchecked")
//  private final Map<Integer, MapFile.Reader> readers = Collections
//    .synchronizedMap(new ReaderLRUCache(CACHE_SIZE));

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

  // object for keeping track of zoom levels and directories
  List<String> partitions;
  private long[] splits;

  // private final MapFile.Reader[] readers;

  // location of data
  final Path imagePath;

  //
//  private final TileIdPartitioner<?, ?> partitioner;

  // Hadoop Configuration for connection to HDFS
  final Configuration conf;

  //
  final boolean profile;
  private SplitFile sf;

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
  public HdfsMrsTileReader(final String path) throws IOException
  {
      // get the Hadoop configuration
      conf = HadoopUtils.createConfiguration();
      sf = new SplitFile(conf);

      String modifiedPath = path;

      // TODO - this seems dangerous because a local file might have the same name as an hdfs file
      final File file = new File(path);
      if (file.exists())
      {
        modifiedPath = "file://" + file.getAbsolutePath();
      }

      imagePath = new Path(modifiedPath);
      
      FileSystem fs = HadoopFileUtils.getFileSystem(conf, imagePath);
      if (!fs.exists(imagePath))
      {
        throw new IOException("Cannot open HdfsMrsTileReader for: " + modifiedPath );
      }

      // get the path to the splits file
      final Path splitFilePath = new Path(sf.findSplitFile(imagePath.toString()));

      initPartitions(splitFilePath);

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
//    // close the readers
//    try
//    {
//      for (final MapFile.Reader reader : readers.values())
//      {
//        try
//        {
//          LOG.debug("removing reader: {}", reader.toString());
//
//          if (profile)
//          {
//            LeakChecker.instance().remove(reader);
//          }
//
//          reader.close();
//
//        }
//        catch (final IOException e)
//        {
//          throw new MrsTileException(e);
//        }
//      }
//    }
//    finally
//    {
//      // clear out the
//      readers.clear();
//    }

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

    // TODO: determine if the zoom level is needed for the request - right now it
    // is the default zoom level

    try
    {

      // determine the partition where the tile is
      final int partition = getPartition(key);

      // get the reader that handles the partition/map file
      final MapFile.Reader reader = getReader(partition);

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

  private void initPartitions(final Path splitsFilePath)
  {
    final List<Long> splitsA = new ArrayList<Long>();
    partitions = new ArrayList<String>();

    sf.readSplits(splitsFilePath.toString(), splitsA, partitions);

    // because of the way splits work, there should be 1 more partition name than splits
    if (splitsA.size() > 0 && partitions.size() != splitsA.size() + 1)
    {
      // This is kinda hacky, but if this is an old style splits file, we need to upgrade it.
      // This _should_ be the only place it is necessary.
      try
      {
        final Path tmp = new Path(HadoopFileUtils.getTempDir(conf), HadoopUtils.createRandomString(4) + "_splits");
        
        FileUtil.copy(HadoopFileUtils.getFileSystem(conf, splitsFilePath), splitsFilePath, 
          HadoopFileUtils.getFileSystem(conf, tmp), tmp, false, true, conf);
        
        sf.copySplitFile(tmp.toString(), splitsFilePath.getParent().toString(), true);
        sf.readSplits(splitsFilePath.toString(), splitsA, partitions);
      }
      catch (final IOException e)
      {
        e.printStackTrace();
      }
    }

    // if we have no partitions, it probably means no splits file, look for mapfiles...
    if (partitions.size() == 0)
    {
      try
      {
        // get a Hadoop file system handle
        final FileSystem fs = splitsFilePath.getFileSystem(conf);

        // get the list of paths of the subdirectories of the parent
        final Path[] paths = FileUtil.stat2Paths(fs.listStatus(splitsFilePath.getParent()));

        Arrays.sort(paths);

        // look inside each subdirectory for a data dir and keep track
        for (final Path p : paths)
        {
          boolean isMapFileDir = false;
          final FileStatus[] dirFiles = fs.listStatus(p);
          for (final FileStatus dirFile : dirFiles)
          {
            if (dirFile.getPath().getName().equals("data"))
            {
              isMapFileDir = true;
              break;
            }
          }

          if (isMapFileDir)
          {
            final String name = p.toString();
            partitions.add(name);
          }
        }

        if (partitions.size() == 1)
        {
          splitsA.clear();
          splitsA.add(Long.MAX_VALUE);
        }
      }
      catch (final IOException e)
      {

      }
    }

    splits = Longs.toArray(splitsA);
  } // end initPartitions

  /**
   * Get the number of directories with imagery files
   * 
   * @return the number of data directories
   */
  public int getMaxPartitions()
  {
    return partitions.size();
  } // end getMaxPartitions

  /**
   * This will return the partition for the tile requested.
   * 
   * @param key
   *          the item to find the range for
   * @return the partition of the requested key
   */
  public int getPartition(final TileIdWritable key)
  {
    return SplitFile.findPartition(key.get(), splits);
  } // end getPartition

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
//    // get the reader from the LRU cache
//    MapFile.Reader reader = readers.get(partition);
//
//    // set the cache if needed
//    if (reader == null)
//    {
//      final String part = partitions.get(partition);
//      final Path path = new Path(imagePath, part);
//      final FileSystem fs = path.getFileSystem(conf);
//
//      reader = new MapFile.Reader(fs, path.toString(), conf);
//      readers.put(partition, reader);
//
//      if (profile)
//      {
//        LeakChecker.instance().add(
//          reader,
//          ExceptionUtils.getFullStackTrace(new Throwable(
//            "HdfsMrsVectorReader creation stack(ignore the Throwable...)")));
//      }
//      LOG.debug("opening (not in cache) partition: {} file: {}", partition, part);
//    }
//
//    return reader;
  } // end getReader

} // end HdfsReader

