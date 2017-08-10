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

package org.mrgeo.hdfs.tile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.partitioners.SplitGenerator;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class FileSplit extends Splits
{
public static final String SPLIT_FILE = "splits";
public static final String OLD_SPLIT_FILE = "splits.txt";
private static final String SPACER = " ";
private static final String VERSION = "v3";
// negative magic number telling if the split is an old (version 2) splitfile.  v1 is deprecated
private static final long VERSION_2 = -12345;

public void generateSplits(FileSplitInfo[] splits)
{
  this.splits = new FileSplitInfo[splits.length];
  System.arraycopy(splits, 0, this.splits, 0, splits.length);
}

public void generateSplits(long[] startIds, long[] endIds, String[] names)
{
  splits = new SplitInfo[names.length];

  for (int i = 0; i < names.length; i++)
  {
    splits[i] = new FileSplitInfo(startIds[i], endIds[i], names[i], i);
  }
}

public void generateSplits(Path parent, Configuration conf) throws IOException
{
  List<FileSplitInfo> list = new ArrayList<>();

  // get a Hadoop file system handle
  FileSystem fs = getFileSystem(parent);

  // get the list of paths of the subdirectories of the parent
  Path[] paths = FileUtil.stat2Paths(fs.listStatus(parent));

  Arrays.sort(paths);

  int partition = 0;
  // look inside each subdirectory for a data dir and keep track
  for (Path p : paths)
  {
    Path mapfile = null;
    FileStatus[] dirFiles = fs.listStatus(p);
    for (FileStatus dirFile : dirFiles)
    {
      if (dirFile.getPath().getName().equals("data"))
      {
        mapfile = dirFile.getPath().getParent();
        break;
      }
    }

    if (mapfile != null)
    {
      RasterWritable val = new RasterWritable();
      MapFile.Reader reader = createMapFileReader(conf, mapfile);
      TileIdWritable firstKey = (TileIdWritable) reader.getClosest(new TileIdWritable(0), val);
      TileIdWritable lastKey = (TileIdWritable) reader.getClosest(new TileIdWritable(Long.MAX_VALUE), val, true);
      if (firstKey != null && lastKey != null)
      {
        list.add(new FileSplitInfo(firstKey.get(), lastKey.get(), mapfile.getName(), partition));
      }

      partition++;
    }
  }

  splits = list.toArray(new FileSplitInfo[list.size()]);
}

@Override
public String findSplitFile(Path parent) throws IOException
{
  Path file = new Path(parent, SPLIT_FILE);
  try
  {
    if (fileExists(file))
    {
      return file.toString();
    }
    else
    {
      file = new Path(parent, OLD_SPLIT_FILE);
      if (fileExists(file))
      {
        return file.toString();
      }
    }
  }
  catch (IOException e)
  {
    throw new IOException("Error opening split file: " + file, e);
  }

  throw new IOException("Split file not found: " + file);
}

@Override
public void generateSplits(SplitGenerator generator)
{
  splits = generator.getSplits();
}

public void readSplits(InputStream stream) throws SplitException
{
  try (Scanner reader = new Scanner(stream))
  {
    String first = reader.nextLine();

    if (first.equals(VERSION))
    {
      readSplits(reader);
    }
    else
    {
      long split = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(first)).getLong();
      if (split == VERSION_2)
      {
        throw new SplitException("Old version 2 splits file, you need to convert it to version 3, " +
            "this can be done by calling readSplits(path) instead of readSplits(stream)");
      }
      else
      {
        throw new SplitException("Unrecognized splits");
      }
    }
  }
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
public boolean isVersion2(Path splitsfile) throws IOException
{
  if (!fileExists(splitsfile))
  {
    // version 2 can have no splits file, meaning only 1 partition
    return true;
  }
  try (InputStream stream = getInputStream(splitsfile))
  {
    Scanner reader = new Scanner(stream);

    String line = reader.nextLine();
    long split = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(line)).getLong();
    return split == VERSION_2;
  }
  catch (BufferUnderflowException e)
  {
    return false;
  }
}

public void readSplits(Path parent) throws IOException
{
  if (isVersion2(new Path(parent, SPLIT_FILE)))
  {
    generateSplits(parent, HadoopUtils.createConfiguration());
    writeSplits(parent);
  }
  try (InputStream stream = getInputStream(new Path(parent, SPLIT_FILE)))
  {
    readSplits(stream);
  }
}

public void writeSplits(OutputStream stream) throws SplitException
{
  if (splits == null)
  {
    throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
  }

  PrintWriter writer = new PrintWriter(stream);
  writer.println(VERSION);
  writer.println(splits.length);
  for (SplitInfo split : splits)
  {
    writer.print(((FileSplitInfo) split).getStartId());
    writer.print(SPACER);
    writer.print(((FileSplitInfo) split).getEndId());
    writer.print(SPACER);
    writer.print(((FileSplitInfo) split).getName());
    writer.print(SPACER);
    writer.println(split.getPartition());
  }
  writer.close();
}

public void writeSplits(Path parent) throws IOException
{
  try (OutputStream stream = getOutputStream(new Path(parent, SPLIT_FILE)))
  {
    writeSplits(stream);
  }
}

@Override
public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
{
  int count = in.readInt();
  splits = new FileSplitInfo[count];

  for (int i = 0; i < splits.length; i++)
  {
    splits[i] = new FileSplitInfo(in.readLong(), in.readLong(), in.readUTF(), in.readInt());
  }

}

protected MapFile.Reader createMapFileReader(Configuration conf, Path mapfile) throws IOException
{
  return new MapFile.Reader(mapfile, conf);
}

protected FileSystem getFileSystem(Path parent) throws IOException
{
  return HadoopFileUtils.getFileSystem(parent);
}

protected boolean fileExists(Path file) throws IOException
{
  return HadoopFileUtils.exists(file);
}

protected InputStream getInputStream(Path path) throws IOException
{
  return getFileSystem(path).open(path);
}

protected OutputStream getOutputStream(Path path) throws IOException
{
  return getFileSystem(path).create(path);
}

private void readSplits(Scanner reader)
{
  int count = Integer.parseInt(reader.nextLine());
  List<FileSplitInfo> splitsList = new ArrayList<>(count);

  for (int i = 0; i < count; i++)
  {
    long startTileId = reader.nextLong();
    long endTileId = reader.nextLong();
    String name = reader.next();
    int partition = reader.nextInt();
    // There may be partitions with no tiles in them when ingested imagery has
    // no data within the region of that particular split. We account for that
    // by ignoring those empty partitions when reading the data so as not to
    // waste time processing empty partitions. When there are no tiles in the
    // partition, the startTileId will be greater than the endTileId.
    if (startTileId <= endTileId)
    {
      splitsList.add(new FileSplitInfo(startTileId, endTileId, name, partition));
    }
  }
  splits = new FileSplitInfo[splitsList.size()];
  splitsList.toArray(splits);
}

public static class FileSplitInfo extends SplitInfo
{
  private String name;
  private long startId;
  private long endId;
  private int partition;

  // constructor for serialization
  public FileSplitInfo()
  {
  }

  public FileSplitInfo(long startId, long endId, String name, int partition)
  {
    this.name = name;
    this.partition = partition;
    this.startId = startId;
    this.endId = endId;
  }

  public long getTileId()
  {
    return endId;
  }

  public int getPartition()
  {
    return partition;
  }

  public String getName()
  {
    return name;
  }

  public long getStartId()
  {
    return startId;
  }

  public long getEndId()
  {
    return endId;
  }

  public String toString()
  {
    return "startId = " + startId +
        ", endId = " + endId +
        ", name = " + name +
        ", partition = " + partition;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException
  {
    out.writeLong(startId);
    out.writeLong(endId);
    out.writeUTF(name);
    out.writeInt(partition);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
  {
    startId = in.readLong();
    endId = in.readLong();
    name = in.readUTF();
    partition = in.readInt();
  }

  @Override
  boolean compareEQ(long tileId)
  {
    return tileId == endId;
  }

  @Override
  boolean compareLE(long tileId)
  {
    return tileId <= endId;
  }

  @Override
  boolean compareLT(long tileId)
  {
    return tileId < endId;
  }

  @Override
  boolean compareGE(long tileId)
  {
    return tileId >= endId;
  }

  @Override
  boolean compareGT(long tileId)
  {
    return tileId > endId;
  }
}

}
