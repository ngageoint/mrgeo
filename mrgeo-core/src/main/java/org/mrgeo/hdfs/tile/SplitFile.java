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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.mrgeo.image.MrsImage;
import org.mrgeo.tile.SplitGenerator;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class SplitFile
{
  private static final Logger log = LoggerFactory.getLogger(SplitFile.class);
  protected static final long HAS_PARTITION_NAMES = -12345; // negative magic
                                                          // number telling if
                                                          // the
  public final static String SPLIT_FILE = "splits";
  public final static String OLD_SPLIT_FILE = "splits.txt";
  protected Configuration conf;

  public SplitFile(Configuration conf)
  {
    this.conf = conf;
  }

  public void readSplits(final MrsImage image, final List<Long> splits,
      final List<String> partitions)
  {
    try
    {
      final InputStream stream = openSplitFile(image.getMetadata().getPyramid(),
          image.getZoomlevel());
      readSplits(stream, splits, partitions);
    }
    catch (final FileNotFoundException e)
    {
    }
    catch (final IOException e)
    {
    }
  }

  public void readSplits(final InputStream stream, final List<Long> splits,
      final List<String> partitions)
  {
    splits.clear();
    partitions.clear();

    final Scanner in = new Scanner(new BufferedReader(new InputStreamReader(stream)));
    try
    {
      boolean firstline = true;
      boolean hasPartitionNames = false;
      while (in.hasNextLine())
      {
        final long split = ByteBuffer.wrap(Base64.decodeBase64(in.nextLine().getBytes())).getLong();
        if (firstline)
        {
          firstline = false;
          if (split == HAS_PARTITION_NAMES)
          {
            hasPartitionNames = true;
          }
          else
          {
            splits.add(split);
          }
        }
        else
        {
          if (split != HAS_PARTITION_NAMES)
          {
            splits.add(split);
          }
          if (hasPartitionNames)
          {
            final String partition = new String(Base64.decodeBase64(in.nextLine().getBytes()));
            partitions.add(partition);
          }
        }
      }
    }
    finally
    {
      in.close();
      try
      {
        stream.close();
      }
      catch (final IOException e)
      {
        log.error("Exception while closing stream in readSplits", e);
      }
    }
  }

  public TileIdWritable[] readSplitsAsTileId(final MrsImage image)
  {
    try
    {
      final InputStream stream = openSplitFile(image.getMetadata().getPyramid(),
          image.getZoomlevel());
      return readSplitsAsTileId(stream);
    }
    catch (final FileNotFoundException e)
    {
      return new TileIdWritable[0];
    }
    catch (final IOException e)
    {
    }
    return null;
  }

  protected TileIdWritable[] readSplitsAsTileId(final InputStream stream)
  {
    final List<Long> splits = new ArrayList<Long>();
    final List<String> partitions = new ArrayList<String>();

    readSplits(stream, splits, partitions);

    final TileIdWritable[] tiles = new TileIdWritable[splits.size()];
    int cnt = 0;
    for (final Long split : splits)
    {
      tiles[cnt++] = new TileIdWritable(split);
    }

    return tiles;
  }

  public synchronized TileIdWritable[] readSplitsAsTileId(final String splitFileName)
  {
    InputStream stream = null;
    try
    {
      stream = HadoopFileUtils.open(conf, new Path(splitFileName));
  
      return readSplitsAsTileId(stream);
    }
    catch (final FileNotFoundException e)
    {
      return new TileIdWritable[0];
    }
    catch (final IOException e)
    {
    }
    finally
    {
      if (stream != null)
      {
        try
        {
          stream.close();
        }
        catch (IOException e)
        {
          log.error("Exception while closing stream in readSplitsAsTileId", e);
        }
      }
    }
    return null;
  }

  public void copySplitFile(final String splitFileFrom, final String splitFileToDir)
      throws IOException
  {
    copySplitFile(splitFileFrom, splitFileToDir, null, true);
  }

  public void copySplitFile(final String splitFileFrom, final String splitFileToDir,
      final boolean deleteSource) throws IOException
  {
    copySplitFile(splitFileFrom, splitFileToDir, null, deleteSource);
  }

  public void copySplitFile(final String splitFileFrom, final String splitFileToDir,
      final int[] partitionsUsed) throws IOException
  {
    copySplitFile(splitFileFrom, splitFileToDir, partitionsUsed, true);
  }

  public void copySplitFile(final String splitFileFrom, final String splitFileToDir, final int[] partitionsUsed, final boolean deleteSource)
      throws IOException
  {
    // move split file into the output directory
    if (!HadoopUtils.isLocal(conf))
    {
      final Path splitFileTo = new Path(splitFileToDir, SPLIT_FILE);
      final FileSystem fsTo = splitFileTo.getFileSystem(conf);
      Path splitFileFromPath = new Path(splitFileFrom);
      final FileSystem fsFrom = splitFileFromPath.getFileSystem(conf);
      if (fsFrom.exists(splitFileFromPath))
      {
        final FileStatus status = fsFrom.getFileStatus(splitFileFromPath);
  
        // if the splits file is empty, no need to copy it...
        if (status.getLen() > 0)
        {
          // if we have partition names already, just copy the file...
          if (hasPartitionNames(splitFileFrom))
          {
            FileUtil.copy(fsFrom, splitFileFromPath, fsTo, splitFileTo, deleteSource, true, conf);
          }
          else
          {
            // no partitions in the split file, make one...
            fsTo.delete(splitFileTo, false);
  
            String[] partitions = findPartitions(splitFileToDir);
            List<Long> splits = readSplits(splitFileFrom);
  
            if ((splits.size() + 1) > partitions.length)
            {
  
              if (partitionsUsed != null)
              {
                final List<Long> tmpSplits = new ArrayList<Long>();
  
                // make sure the array is sorted...
                Arrays.sort(partitionsUsed);
                for (final int used : partitionsUsed)
                {
                  if (used < splits.size())
                  {
                    tmpSplits.add(splits.get(used));
                  }
                }
  
                splits = tmpSplits;
              }
            }
            else if ((splits.size() + 1) < partitions.length)
            {
              if (log.isDebugEnabled())
              {
                log.debug("original splits:");
                for (Long split : splits)
                {
                  log.debug("  " + split);
                }
  
                log.debug("partitions found:");
                for (String part : partitions)
                {
                  log.debug("  " + part);
                  FileStatus st = fsTo.getFileStatus(new Path(splitFileToDir, part + "/index"));
                  log.debug("  index size: " + st.getLen());
                  FileStatus st2 = fsTo.getFileStatus(new Path(splitFileToDir, part + "/data"));
                  log.debug("  data size: " + st2.getLen());
                }
  
                List<String> tmpPartitions = new ArrayList<String>();
  
                for (String part : partitions)
                {
  
                  MapFile.Reader reader = null;
  
                  try
                  {
                    reader = new MapFile.Reader(fsTo, (new Path(splitFileToDir, part)).toString(),
                        conf);
  
                    TileIdWritable key = new TileIdWritable();
                    RasterWritable val = new RasterWritable();
                    if (reader.next(key, val))
                    {
                      tmpPartitions.add(part);
                    }
                  }
                  finally
                  {
                    if (reader != null)
                    {
                      reader.close();
                    }
                  }
                }
  
                log.debug("partitions having records:");
                for (String p : tmpPartitions)
                {
                  log.debug("  " + p);
                }
              }
            }
  
            if (splits.size() + 1 != partitions.length)
            {
              throw new IOException(
                  "splits file and file partitions mismatch (splits should be 1 less than partitions)!  Splits length: "
                      + splits.size() + " number of partitions: " + partitions.length);
            }
  
            writeSplits(splits, partitions, splitFileTo.toString());
  
            if (deleteSource)
            {
              fsFrom.delete(splitFileFromPath, false);
            }
          }
        }
        else
        {
          fsFrom.delete(splitFileFromPath, false);
        }
      }
    }
  }

  protected boolean hasPartitionNames(final InputStream splitFileInputStream)
  {
    final Scanner in = new Scanner(new BufferedReader(new InputStreamReader(splitFileInputStream)));
    try
    {
      while (in.hasNextLine())
      {
        final long split = ByteBuffer.wrap(Base64.decodeBase64(in.nextLine().getBytes())).getLong();
        return (split == HAS_PARTITION_NAMES);
      }
    }
    finally
    {
      in.close();
    }
    return false;
  }

  public int numSplits(final String file) throws IOException
  {
    final InputStream fdis = HadoopFileUtils.open(conf, new Path(file));
    try
    {
      return numSplits(fdis);
    }
    finally
    {
      fdis.close();
    }
  }

  public int numSplits(final InputStream splitFileInputStream) throws IOException
  {
    final java.util.Scanner in = new java.util.Scanner(new BufferedReader(new InputStreamReader(
        splitFileInputStream)));
    int numSplits = 0;
    try
    {
      boolean firstline = true;
      boolean hasPartitionNames = false;
      while (in.hasNextLine())
      {
        if (firstline)
        {
          firstline = false;
          final long split = ByteBuffer.wrap(Base64.decodeBase64(in.nextLine().getBytes()))
              .getLong();
          if (split == HAS_PARTITION_NAMES)
          {
            hasPartitionNames = true;
          }
          else
          {
            numSplits++;
          }
        }
        else
        {
          in.nextLine();
          numSplits++;
          if (hasPartitionNames)
          {
            in.nextLine();
          }
        }
      }
    }
    finally
    {
      in.close();
    }
    return numSplits;
  }

  public int writeSplits(final SplitGenerator splitGenerator, final String splitFile) throws IOException
  {
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, new Path(splitFile));
    final FSDataOutputStream fdos = fs.create(new Path(splitFile));
    try
    {
      return writeSplits(splitGenerator, fdos);
    }
    finally
    {
      fdos.close();
    }
  }

  private int writeSplits(final List<Long> splits, final String[] partitions, final String splitFile) throws IOException
  {
    final Path splitFilePath = new Path(splitFile);
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, splitFilePath);
    final FSDataOutputStream fdos = fs.create(splitFilePath);
    try
    {
      return writeSplits(splits, partitions, fdos);
    }
    finally
    {
      fdos.close();
    }
  }

  public int writeSplits(final SplitGenerator splitGenerator,
      final OutputStream splitFileOutputStream) throws IOException
  {
    final List<Long> splits = splitGenerator.getSplits();
    final PrintWriter out = new PrintWriter(splitFileOutputStream);
    for (final long split : splits)
    {
      final byte[] encoded = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(split).array());
      out.println(new String(encoded));
    }
    out.close();

    return splits.size();
  }

  private int writeSplits(final List<Long> splits, final String[] partitions,
      final OutputStream splitFileOutputStream) throws IOException
  {
    final PrintWriter out = new PrintWriter(splitFileOutputStream);

    try
    {
      // write the magic number
      final byte[] magic = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(HAS_PARTITION_NAMES)
          .array());
      out.println(new String(magic));

      int cnt = 0;
      for (final long split : splits)
      {
        final byte[] id = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(split).array());

        out.println(new String(id));

        final byte[] part = Base64.encodeBase64(partitions[cnt++].getBytes());
        out.println(new String(part));
      }
      // need to write the name of the last partition, we can use magic number
      // for the tileid...
      out.println(new String(magic));
      final byte[] part = Base64.encodeBase64(partitions[cnt].getBytes());
      out.println(new String(part));
    }
    finally
    {
      out.close();
    }

    return splits.size();
  }

  protected InputStream openSplitFile(final String pyramidName, final int zoomLevel) throws IOException
  {
    InputStream stream = null;
  
    Path splits = new Path(new Path(pyramidName, "" + zoomLevel), SPLIT_FILE);
    try
    {
      stream = HadoopFileUtils.open(conf, splits);
    }
    catch (final FileNotFoundException e)
    {
      splits = new Path(new Path(pyramidName, "" + zoomLevel), OLD_SPLIT_FILE);
      stream = HadoopFileUtils.open(conf, splits);
    }
  
    return stream;
  }

  public List<Long> readSplits(final String splitFile)
  {
    try
    {
      final Path splitFilePath = new Path(splitFile);
      final InputStream stream = HadoopFileUtils.open(conf, splitFilePath);
      final List<Long> splits = new ArrayList<Long>();
      final List<String> partitions = new ArrayList<String>();
  
      readSplits(stream, splits, partitions);
      return splits;
    }
    catch (final FileNotFoundException e)
    {
      return new ArrayList<Long>();
    }
    catch (final IOException e)
    {
    }
    return null;
  }

  public void readSplits(final String splitFile, final List<Long> splits, final List<String> partitions)
  {
    try
    {
      final InputStream stream = HadoopFileUtils.open(conf, new Path(splitFile));
  
      readSplits(stream, splits, partitions);
    }
    catch (final FileNotFoundException e)
    {
    }
    catch (final IOException e)
    {
    }
  }

  public String findSplitFile(final String imageName) throws IOException
  {
    final Path imagePath = new Path(imageName);
  
    final FileSystem fs = imagePath.getFileSystem(conf);
  
    Path splits = new Path(imagePath, SPLIT_FILE);
    if (!fs.exists(splits))
    {
      if (fs.exists(new Path(imagePath, OLD_SPLIT_FILE)))
      {
        splits = new Path(imagePath, OLD_SPLIT_FILE);
      }
    }
  
    return splits.toString();
  }

  protected boolean hasPartitionNames(final String splitFile)
  {
    try
    {
      final InputStream stream = HadoopFileUtils.open(conf, new Path(splitFile));
      try
      {
        return hasPartitionNames(stream);
      }
      finally
      {
        try
        {
          stream.close();
        }
        catch (final IOException e)
        {
        }
      }
    }
    catch (final FileNotFoundException e)
    {
    }
    catch (final IOException e)
    {
      log.error("Exception while checking for partition names in split file " + splitFile, e);
    }
    return false;
  
  }

  private String[] findPartitions(final String splitFileDir) throws IOException
  {
  
    Path path = new Path(splitFileDir);
    final ArrayList<String> partitions = new ArrayList<String>();
  
    // get a Hadoop file system handle
    final FileSystem fs = path.getFileSystem(conf);
  
    // get the list of paths of the subdirectories of the parent
    final Path[] paths = FileUtil.stat2Paths(fs.listStatus(path));
  
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
        // need to be relative to the path, so we can just use getName()
        partitions.add(p.getName());
      }
    }
  
    return partitions.toArray(new String[0]);
  }

  public static int findPartition(final long key, final long[] array)
  {
    // find the bin for the range, and guarantee it is positive
    int index = Arrays.binarySearch(array, key);
    index = index < 0 ? (index + 1) * -1 : index;

    return index;
  }

  public static int findPartition(final TileIdWritable key, final TileIdWritable[] array)
  {
    // find the bin for the range, and guarantee it is positive
    int index = Arrays.binarySearch(array, key);
    index = index < 0 ? (index + 1) * -1 : index;

    return index;
  }

//  private static int findPartition(final TileIdZoomWritable key, final TileIdWritable[] array)
//  {
//    // get zoom
//    // get array for zoom
//
//    // return findPartition(key, array)
//    return -1;
//  }
}
