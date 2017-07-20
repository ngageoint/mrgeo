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

import com.google.common.cache.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.image.MrsImageException;
import org.mrgeo.data.image.MrsImageReader;
import org.mrgeo.data.image.MrsPyramidReaderContext;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils.MapFileReaderWrapper;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@SuppressFBWarnings(value = "SIC_INNER_SHOULD_BE_STATIC_ANON", justification = "'readerCache' - Needs refactoring to remove")
public class HdfsMrsImageReader extends MrsImageReader
{
private final static int READER_CACHE_SIZE = 100;
private final static int READER_CACHE_EXPIRE = 10; // minutes
@SuppressWarnings("unused")
private static Logger log = LoggerFactory.getLogger(HdfsMrsImageReader.class);
final int tileSize;
// location of data
final Path imagePath;
// Hadoop Configuration for connection to HDFS
final Configuration conf = HadoopUtils.createConfiguration();
final boolean profile;
//final private HdfsMrsImageDataProvider provider;
final private MrsPyramidReaderContext context;
final private FileSplit splits = new FileSplit();
private final LoadingCache<Integer, MapFileReaderWrapper> readerCache = CacheBuilder.newBuilder()
    .maximumSize(READER_CACHE_SIZE)
    .expireAfterAccess(READER_CACHE_EXPIRE, TimeUnit.SECONDS)
    .removalListener(
        new RemovalListener<Integer, MapFileReaderWrapper>()
        {
          @Override
          public void onRemoval(RemovalNotification<Integer, MapFileReaderWrapper> notification)
          {
            try
            {
              notification.getValue().close();
            }
            catch (IOException e)
            {
              log.error("IOException removing HdfsMrsImageReader from cache {} ", e);
            }
          }
        }).build(new CacheLoader<Integer, MapFileReaderWrapper>()
    {

      @Override
      public MapFileReaderWrapper load(Integer partitionIndex) throws IOException
      {
        return loadReader(partitionIndex);
      }
    });
private boolean canBeCached = true;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "check will only be used for reading valid MrsPyramids")
public HdfsMrsImageReader(HdfsMrsImageDataProvider provider,
    MrsPyramidReaderContext context) throws IOException
{
  String path = new Path(provider.getResourcePath(true), "" + context.getZoomlevel()).toString();

//    this.provider = provider;
  this.context = context;
  tileSize = provider.getMetadataReader().read().getTilesize();

  String modifiedPath = path;

  File file = new File(path);
  if (file.exists())
  {
    modifiedPath = "file://" + file.getAbsolutePath();
  }

  imagePath = new Path(modifiedPath);
  FileSystem fs = HadoopFileUtils.getFileSystem(conf, imagePath);
  if (!fs.exists(imagePath))
  {
    throw new IOException("Cannot open HdfsMrsTileReader for " + modifiedPath + ".  Path does not exist.");
  }

  // Do not perform caching when S3 is being used because it is accessed through
  // a REST interface using HTTP, and there is a connection pool that can be
  // filled and cause deadlock when too many readers are opened at once.
  Path qualifiedImagePath = imagePath.makeQualified(fs);
  URI imagePathUri = qualifiedImagePath.toUri();
  String imageScheme = imagePathUri.getScheme().toLowerCase();
  if ("s3".equals(imageScheme) || "s3n".equals(imageScheme))
  {
    canBeCached = false;
  }

  readSplits(modifiedPath);

  // set the profile
  profile = System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0;
}

@Override
public int getZoomlevel()
{
  return context.getZoomlevel();
}

@Override
public int getTileSize()
{
  return tileSize;
}

public KVIterator<TileIdWritable, MrGeoRaster> get(LongRectangle tileBounds)
{
  return new HdfsImageResultScanner(tileBounds, this);
}

/**
 * This will retrieve tiles in a specified range.
 *
 * @param startKey the start of the range of tiles to get
 * @param endKey   the end (inclusive) of the range of tiles to get
 * @return An Iterable object of tile data for the range requested
 */
public KVIterator<TileIdWritable, MrGeoRaster> get(TileIdWritable startKey,
    TileIdWritable endKey)
{
  return new HdfsImageResultScanner(startKey, endKey, this);
}

@Override
public boolean canBeCached()
{
  return canBeCached;
}

@Override
public long calculateTileCount()
{
  int count = 0;
  try
  {
    FileSystem fs = imagePath.getFileSystem(conf);
    Path[] names = FileUtil.stat2Paths(fs.listStatus(imagePath));
    Arrays.sort(names);
    try (DataOutputBuffer key = new DataOutputBuffer())
    {
      for (Path name : names)
      {
        FileStatus[] dirFiles = fs.listStatus(name);
        for (FileStatus dirFile : dirFiles)
        {
          if (dirFile.getPath().getName().equals("index"))
          {
            try (SequenceFile.Reader index = new SequenceFile.Reader(fs, dirFile.getPath(), conf))
            {
              while (index.nextRawKey(key) >= 0)
              {
                count++;
              }
            }
          }
        }
      }
    }
    return count;
  }
  catch (IOException e)
  {
    throw new MrsImageException(e);
  }
}

/**
 * This will go through the items in the cache and close all the readers.
 */
@Override
public void close()
{
  readerCache.invalidateAll();
}

/**
 * Check if a tile exists in the data.
 */
@Override
public boolean exists(TileIdWritable key)
{
  // check for a successful retrieval
  return get(key) != null;
}

@Override
public KVIterator<TileIdWritable, MrGeoRaster> get()
{
  return get(null, null);
}

/**
 * Retrieve a tile from the data.
 *
 * @param key is the tile to get from the max zoom level
 * @return the data for the tile requested
 */
@SuppressWarnings({"unchecked", "squid:S1166"}) // Exception caught and handled
@Override
public MrGeoRaster get(TileIdWritable key)
{
  MapFileReaderWrapper readerWrapper = null;
  try
  {
    // get the reader that handles the partition/map file
    readerWrapper = getReaderWrapper(getPartitionIndex(key));

    // return object
    RasterWritable val = (RasterWritable) readerWrapper.getReader().getValueClass().newInstance();

    // get the tile from map file from HDFS
    try
    {
      // log.debug("getting " + key);
      try {
        readerWrapper.getReader().get(key, val);
      }
      catch(java.io.EOFException e) {
        log.error("Got EOF exception trying to read " + readerWrapper.toString());
      }
      if (getWritableSize(val) > 0)
      {
        // return the data
        return toNonWritable(val);
      }
    }
    catch (IllegalStateException ignored)
    {
      // no-op. Accumulo's Value class will return an IllegalStateException if the reader
      // returned no data, but you try to do a get or getSize. We'll trap it here and return
      // a null for the tile.
    }

    // nothing came back from the map file
    return null;

  }
  catch (IOException e)
  {
    log.error("Got IOException when reading tile", e);
    throw new MrsImageException(e);
  }
  catch (InstantiationException | IllegalAccessException e)
  {
    throw new MrsImageException(e);
  }
  finally
  {
    if (readerWrapper != null && !canBeCached())
    {
      try
      {
        readerWrapper.close();
      }
      catch (IOException e)
      {
        log.error("Unable to close reader for " + imagePath, e);
      }
    }
  }
}

@Override
public KVIterator<Bounds, MrGeoRaster> get(Bounds bounds)
{
  TileBounds tileBounds = TMSUtils.boundsToTile(bounds, getZoomlevel(), getTileSize());
  return new BoundsResultScanner(get(new LongRectangle(tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n)),
      getZoomlevel(), getTileSize());
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
 * @param key the item to find the range for
 * @return the partition of the requested key
 */
public int getPartitionIndex(TileIdWritable key) throws IOException
{
  return splits.getSplitIndex(key.get());
}

/**
 * This method will get a MapFile.Reader for the partition specified.
 * Before closing the returned reader, the caller should make sure it
 * is not cached in this class by calling isCachingEnabled(). If that
 * method returns true, the caller should not close the reader. It will
 * be automatically closed when it drops out of the cache.
 *
 * @param partitionIndex is the particular reader being accessed
 * @return the reader for the partition specified
 * @throws IOException
 */
@SuppressWarnings("squid:S1166") // Exception caught and handled
@SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "We _are_ checking!")
public MapFileReaderWrapper getReaderWrapper(int partitionIndex) throws IOException
{
  try
  {
    if (canBeCached)
    {
      log.info("Loading reader for partitionIndex " + partitionIndex + " through the cache");
      MapFileReaderWrapper reader = readerCache.get(partitionIndex);
      try
      {
        // there is a slim chance the cached reader was closed, this will check, close the reader,
        // then reopen it if needed
        TileIdWritable key = (TileIdWritable)reader.getReader().getKeyClass().newInstance();
        reader.getReader().finalKey(key);
        reader.getReader().reset();
        return reader;
      }
      catch (IOException | IllegalAccessException | InstantiationException e)
      {
        log.info("Reader had been previously closed, opening a new one");
        reader = loadReader(partitionIndex);
        readerCache.put(partitionIndex, reader);
        return reader;
      }
    }
    else
    {
      log.info("Loading reader for partitionIndex " + partitionIndex + " without the cache");
      return loadReader(partitionIndex);
    }
  }
  catch (ExecutionException e)
  {
    if (e.getCause() instanceof IOException)
    {
      throw (IOException) e.getCause();
    }
    throw new IOException(e);
  }
}

protected int getWritableSize(RasterWritable val)
{
  return val.getSize();
}

@Deprecated
protected MrGeoRaster toNonWritable(RasterWritable val) throws IOException
{
  return RasterWritable.toMrGeoRaster(val);
}

protected Path getTilePath()
{
  return imagePath;
}

private MapFileReaderWrapper loadReader(int partitionIndex) throws IOException
{
  FileSplitInfo part =
      (FileSplitInfo) splits.getSplits()[partitionIndex];

  Path path = new Path(imagePath, part.getName());
  return new MapFileReaderWrapper(path, conf);

//    if (profile)
//    {
//      LeakChecker.instance().add(
//              reader,
//              ExceptionUtils.getStackTrace(new Throwable(
//                      "MapFile.Reader creation stack(ignore the Throwable...)")));
//    }
//    return reader;
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
private void readSplits(String parent) throws IOException
{
  try
  {
    splits.readSplits(new Path(parent));
    return;
  }
  catch (FileNotFoundException ignored)
  {
    // When there is no splits file, the whole image is a single split
  }

  splits.generateSplits(new Path(parent), conf);
}

public static class BoundsResultScanner implements KVIterator<Bounds, MrGeoRaster>
{
  private KVIterator<TileIdWritable, MrGeoRaster> tileIterator;
  private int zoomLevel;
  private int tileSize;

  public BoundsResultScanner(KVIterator<TileIdWritable, MrGeoRaster> tileIterator,
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
  public MrGeoRaster next()
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
    Tile tile = TMSUtils.tileid(key.get(), zoomLevel);
    return TMSUtils.tileBounds(tile.tx, tile.ty, zoomLevel, tileSize);
  }

  @Override
  public MrGeoRaster currentValue()
  {
    return tileIterator.currentValue();
  }
}
}
