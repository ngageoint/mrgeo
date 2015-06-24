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
import org.apache.hadoop.io.SequenceFile;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.tile.SplitGenerator;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class SplitFile
{
  private static final Logger log = LoggerFactory.getLogger(SplitFile.class);
  protected static final long HAS_PARTITION_NAMES = -12345; // negative magic
                                                          // number telling if
                                                          // the
  private static final String SPLITS_VERSION_3 = "Version3";
  private static final String SPLIT_DELIMITER = " ";

  public final static String SPLIT_FILE = "splits";
  public final static String OLD_SPLIT_FILE = "splits.txt";

  public static class SplitInfo
  {
    private String partitionName;
    private long startTileId;
    private long endTileId;

    public SplitInfo(final int zoom)
    {
      partitionName = null;
      startTileId = 0;
      endTileId = TMSUtils.maxTileId(zoom);
    }

    public SplitInfo(String partitionName, long startTileId, long endTileId)
    {
      this.partitionName = partitionName;
      this.startTileId = startTileId;
      this.endTileId = endTileId;
    }

    public String getPartitionName()
    {
      return partitionName;
    }

    public long getStartTileId()
    {
      return startTileId;
    }

    public long getEndTileId()
    {
      return endTileId;
    }

    public String toString()
    {
      StringBuilder sb = new StringBuilder();
      sb.append("startTileId = ");
      sb.append(startTileId);
      sb.append(", endTileId = ");
      sb.append(endTileId);
      sb.append(", partitionName = ");
      sb.append(partitionName);
      return sb.toString();
    }
  }

  protected Configuration conf;

  public SplitFile(Configuration conf)
  {
    this.conf = conf;
  }

  public void readSplits(final MrsImage image, final List<SplitInfo> splits) throws IOException
  {
    int imageZoom = image.getZoomlevel();
    try
    {
      final InputStream stream = openSplitFile(image.getMetadata().getPyramid(), imageZoom);
      readSplits(stream, splits, image.getZoomlevel());
    }
    catch (final FileNotFoundException e)
    {
      // When there is no splits file, return a single split for the entire image.
      LongRectangle tb = image.getMetadata().getTileBounds(image.getZoomlevel());
      splits.add(new SplitFile.SplitInfo(null,
                                         TMSUtils.tileid(tb.getMinX(), tb.getMinY(), imageZoom),
                                         TMSUtils.tileid(tb.getMaxX(), tb.getMaxY(), imageZoom)));
    }
  }

  private void readSplits(final InputStream stream, final List<SplitInfo> splits,
                          int zoom) throws IOException
  {
    splits.clear();

    final Scanner in = new Scanner(new BufferedReader(new InputStreamReader(stream)));
    try
    {
      boolean hasPartitionNames = false;
      if (in.hasNextLine())
      {
        String firstLine = in.nextLine();
        // Version 3 of the splits file format is not base 64 encoded, but the
        // earlier versions are.
        if (firstLine.equals(SPLITS_VERSION_3))
        {
          readSplitsV3(in, splits);
        }
        else
        {
          final long value = ByteBuffer.wrap(Base64.decodeBase64(firstLine.getBytes())).getLong();
          if (value == HAS_PARTITION_NAMES)
          {
            readSplitsV2(in, splits, zoom);
          }
          else
          {
            splits.add(new SplitInfo(null, 0, value));
            readSplitsV1(in, splits, zoom);
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

  /**
   * Version 3 of the splits file is not base 64 encoded. The first line contains the
   * literal SPLITS_VERSION_3 string. Each line after that is a space delimited list
   * of fields that can contain either one or three fields. When it is one field, it
   * will be the end tile id of the split. If it is three fields, then it is the
   * start tile id, then end tile id, then partition name for the split.
   *
   * @param in
   * @param splits
   */
  private void readSplitsV3(final Scanner in, final List<SplitInfo> splits) throws IOException
  {
    while (in.hasNextLine())
    {
      String entry = in.nextLine();
      // This version of the splits file can contain a single field (just the end tile id)
      // or three fields that are space delimited.
      if (entry.indexOf(' ') > 0)
      {
        String[] fields = entry.split(SPLIT_DELIMITER);
        if (fields.length != 3)
        {
          throw new IOException("Invalid splits file. Expected space delimited set of three fields instead of: " + entry);
        }
        long startTileId = -1;
        try
        {
          startTileId = Long.parseLong(fields[0]);
        }
        catch (NumberFormatException nfe)
        {
          throw new IOException("Invalid splits file. Expected start tile id in field 1: " + entry);
        }
        long endTileId = -1;
        try
        {
          endTileId = Long.parseLong(fields[1]);
        }
        catch (NumberFormatException nfe)
        {
          throw new IOException("Invalid splits file. Expected end tile id in field 2: " + entry);
        }
        String partition = fields[2];
        splits.add(new SplitInfo(partition, startTileId, endTileId));
      }
      else
      {
        try
        {
          long endTileId = Long.parseLong(entry);
          splits.add(new SplitInfo(null, -1, endTileId));
        }
        catch(NumberFormatException nfe)
        {
          throw new IOException("Invalid splits file, expected a tile id instead of: " + entry);
        }
      }
    }
  }

  /**
   * Read version 2 of the splits file. All of the values in the file are base 64
   * encoded. The first line contains a magic number that identifies the file as
   * version 2 (HAS_PARTITION_NAMES). After that first line, there are repeating
   * sequences of end tile id then partition name for each partition. There will be
   * one entry in the splits file for each partition.
   *
   * @param in
   * @param splits
   */
  private void readSplitsV2(final Scanner in, List<SplitInfo> splits, final int zoom)
  {
    while (in.hasNextLine())
    {
      long endTileId = ByteBuffer.wrap(Base64.decodeBase64(in.nextLine().getBytes())).getLong();
      if (endTileId == HAS_PARTITION_NAMES)
      {
        endTileId = TMSUtils.maxTileId(zoom);
      }
      final String partition = new String(Base64.decodeBase64(in.nextLine().getBytes()));
      splits.add(new SplitInfo(partition, -1, endTileId));
    }
  }

  /**
   * Reads version 1 of the splits file format. In this case, there is no "magic number"
   * indicating a version number. Each line in the file is a base 64 encoding of the
   * ending tile id for the split. There is an entry in the splits file for all the
   * partitions except the last one, so we add an entry for that.
   *
   * @param in
   * @param splits
   */
  private void readSplitsV1(final Scanner in, final List<SplitInfo> splits, final int zoom)
  {
    // The caller already processed the 0 partition
    int partitionNumber = 1;
    while (in.hasNextLine())
    {
      final long endTileId = ByteBuffer.wrap(Base64.decodeBase64(in.nextLine().getBytes())).getLong();
      splits.add(new SplitInfo(null, -1, endTileId));
      partitionNumber++;
    }
    splits.add(new SplitInfo(null, -1, TMSUtils.maxTileId(zoom)));
  }

  public void copySplitFile(final String splitFileFrom, final String splitFileToDir, final int zoomlevel)
      throws IOException
  {
    copySplitFile(splitFileFrom, splitFileToDir, true, zoomlevel);
  }

  public void copySplitFile(final String splitFileFrom, final String splitFileToDir,
                            final boolean deleteSource, final int zoomlevel)
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
  
            List<SplitInfo> partitions = findPartitions(splitFileToDir);
            List<SplitInfo> splits = readSplits(splitFileFrom, zoomlevel);
  
            if (splits.size() != partitions.size())
            {
              // Debugging output only
              if (log.isDebugEnabled())
              {
                log.debug("original splits:");
                for (SplitInfo split : splits)
                {
                  log.debug("  " + split);
                }
  
                log.debug("partitions found:");
                for (SplitInfo part : partitions)
                {
                  log.debug("  " + part);
                  FileStatus st = fsTo.getFileStatus(new Path(splitFileToDir, part + "/index"));
                  log.debug("  index size: " + st.getLen());
                  FileStatus st2 = fsTo.getFileStatus(new Path(splitFileToDir, part + "/data"));
                  log.debug("  data size: " + st2.getLen());
                }
  
                List<String> tmpPartitions = new ArrayList<String>();
  
                for (SplitInfo part : partitions)
                {
  
                  MapFile.Reader reader = null;
  
                  try
                  {
                    reader = new MapFile.Reader(fsTo, (new Path(splitFileToDir, part.getPartitionName())).toString(),
                        conf);
  
                    TileIdWritable key = new TileIdWritable();
                    RasterWritable val = new RasterWritable();
                    if (reader.next(key, val))
                    {
                      tmpPartitions.add(part.getPartitionName());
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
  
            if (splits.size() + 1 != partitions.size())
            {
              throw new IOException(
                  "splits file and file partitions mismatch (splits should be 1 less than partitions)!  Splits length: "
                      + splits.size() + " number of partitions: " + partitions.size());
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
      if (in.hasNextLine())
      {
        String line = in.nextLine();
        if (line.equals(SPLITS_VERSION_3))
        {
          if (in.hasNextLine())
          {
            line = in.nextLine();
            String[] fields = line.split(SPLIT_DELIMITER);
            if (fields.length == 3)
            {
              return true;
            }
          }
        }
      }
    }
    finally
    {
      in.close();
    }
    return false;
  }

  public int numSplitsCurrentVersion(final String file) throws IOException
  {
    List<SplitFile.SplitInfo> splits = readSplitsCurrentVersion(file);
    return splits.size();
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

  private int writeSplits(final List<SplitInfo> splits, final List<SplitInfo> partitions,
                          final String splitFile) throws IOException
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
    // Only write the version header if there are actually splits to write
    if (splits.size() > 0)
    {
      out.println(SPLITS_VERSION_3);
    }
    for (final long split : splits)
    {
      out.println("" + split);
    }
    out.close();

    return splits.size();
  }

  /**
   * The list of splits contains the end tile id for each of the splits from the splits
   * file. The list of partitions contains the start tile id and partition name for each
   * of the partitions found within the image file structure. We combine these two sets
   * of data to write out a new splits file that contains all three fields.
   *
   * There should always be one more partition found in the image's directory structure
   * than there are splits from the splits file. This is because no entry is written to
   * the splits file for the last
   * @param splits
   * @param partitions
   * @param splitFileOutputStream
   * @return
   * @throws IOException
   */
  private int writeSplits(final List<SplitInfo> splits, final List<SplitInfo> partitions,
      final OutputStream splitFileOutputStream) throws IOException
  {
    if (splits.size() != partitions.size())
    {
      throw new IOException("The number of splits (" + splits.size() +
                                    ") must be equal to the number of partitions found (" +
                                    partitions.size() + ")");
    }
    final PrintWriter out = new PrintWriter(splitFileOutputStream);

    try
    {
      // write the version string
      out.println(SPLITS_VERSION_3);

      int cnt = 0;
      for (int i=0; i < splits.size(); i++)
      {
        SplitInfo split = splits.get(i);
        SplitInfo partition = partitions.get(i);
        String entry = partition.getStartTileId() + SPLIT_DELIMITER + split.getEndTileId() + SPLIT_DELIMITER + partition.getPartitionName();
        out.println(entry);
      }
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

  /**
   * This method should only be called for reading splits that are guaranteed to be
   * in the current format because no zoom level is included. In order to read splits
   * files that *may* be in an older format, call the readSplits method that takes
   * a zoom level as an argument as well.
   *
   * @param splitFile
   * @return
   * @throws IOException
   */
  public List<SplitInfo> readSplitsCurrentVersion(final String splitFile) throws IOException
  {
    return readSplits(splitFile, -1);
  }

  /**
   * This method can read splits files in older formats as well as the current format. The
   * difference is that the older splits files do not contain the last split, so it has
   * to be computed using the max tile id for the zoom level. As a result, the zoom level
   * has to be passed in.
   *
   * @param splitFile
   * @param zoom
   * @return
   * @throws IOException
   */
  public List<SplitInfo> readSplits(final String splitFile, final int zoom) throws IOException
  {
    try
    {
      final Path splitFilePath = new Path(splitFile);
      final InputStream stream = HadoopFileUtils.open(conf, splitFilePath);
      final List<SplitInfo> splits = new ArrayList<SplitInfo>();

      readSplits(stream, splits, zoom);
      return splits;
    }
    catch (final FileNotFoundException e)
    {
      // If there is no splits file, that means that one split includes the entire image
      List<SplitFile.SplitInfo> result = new ArrayList<SplitInfo>();
      result.add(new SplitFile.SplitInfo(zoom));
      return result;
    }
  }

  /**
   * Read the content of the specified splits file. The caller must properly handle both
   * the FileNotFoundException and IOException.
   *
   * @param splitFile
   * @param splits
   * @throws IOException
   */
  public void readSplits(final String splitFile, final List<SplitInfo> splits, int zoom) throws IOException
  {
    final Path splitsPath = new Path(splitFile);
    final InputStream stream = HadoopFileUtils.open(conf, splitsPath);
    readSplits(stream, splits, zoom);
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

  private boolean hasPartitionNames(final String splitFile)
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

  /**
   * Scans the diratory Path passed in for any directory that contains a "data" file.
   * This will correspond to a "part" directory for the image.
   *
   * @param splitFileDir
   * @return
   * @throws IOException
   */
  private List<SplitInfo> findPartitions(final String splitFileDir) throws IOException
  {
  
    Path path = new Path(splitFileDir);
    final ArrayList<SplitInfo> partitions = new ArrayList<SplitInfo>();
  
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
        // We found a map file directory. Now we need to open it as a MapFile
        // and get the first key value which is the start tile id of that partition.
        Path indexPath = new Path(p, "index");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, indexPath, conf);
        TileIdWritable key;
        try
        {
          key = (TileIdWritable) reader.getKeyClass().newInstance();
          reader.next(key);
        }
        catch (InstantiationException e)
        {
          throw new IOException(e);
        }
        catch (IllegalAccessException e)
        {
          throw new IOException(e);
        }
        finally
        {
          if (reader != null)
          {
            reader.close();
          }
        }
        long startTileId = key.get();
        partitions.add(new SplitInfo(p.getName(), startTileId, -1));
      }
    }
    return partitions;
  }

  public static int findPartition(final long key, final List<SplitInfo> partitions)
  {
    Comparator<SplitInfo> c = new Comparator<SplitInfo>()
    {
      public int compare(SplitInfo s1, SplitInfo s2)
      {
        long e1 = s1.getEndTileId();
        long e2 = s2.getEndTileId();
        if (e1 == e2)
        {
          return 0;
        }
        else if (e1 > e2)
        {
          return 1;
        }
        return -1;
      }
    };
    SplitInfo search = new SplitInfo(null, -1, key);
    // find the bin for the range, and guarantee it is positive
    int index = Collections.binarySearch(partitions, search, c);
    index = index < 0 ? (index + 1) * -1 : index;

    return index;
  }
}
