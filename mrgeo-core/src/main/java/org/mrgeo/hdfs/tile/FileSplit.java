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

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class FileSplit extends Splits
{
  private static final String SPACER = " ";
  public static final String SPLIT_FILE = "splits";
  public static final String OLD_SPLIT_FILE = "splits.txt";


  public static class FileSplitInfo extends SplitInfo
  {
    private String name;
    private long startId;
    private long endId;
    private int partition;

    public FileSplitInfo(long startId, long endId, String name, int partition)
    {
      this.name = name;
      this.partition = partition;
      this.startId = startId;
      this.endId = endId;
    }

    @Override
    boolean compareEQ(long tileId)
    {
      return tileId == this.endId;
    }

    @Override
    boolean compareLE(long tileId)
    {
      return tileId <= this.endId;
    }

    @Override
    boolean compareLT(long tileId)
    {
      return tileId < this.endId;
    }

    @Override
    boolean compareGE(long tileId)
    {
      return tileId >= this.endId;
    }

    @Override
    boolean compareGT(long tileId)
    {
      return tileId > this.endId;
    }

    public long getTileId()
    {
      return endId;
    }
    public int  getPartition() { return partition; }


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
          ", name = " + name;
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
  }

  public void generateSplits(Path parent, Configuration conf) throws IOException
  {
    List<FileSplitInfo> list = new ArrayList<FileSplitInfo>();

    // get a Hadoop file system handle
    final FileSystem fs = HadoopFileUtils.getFileSystem(parent);

    // get the list of paths of the subdirectories of the parent
    final Path[] paths = FileUtil.stat2Paths(fs.listStatus(parent));

    Arrays.sort(paths);

    int partition = 0;
    // look inside each subdirectory for a data dir and keep track
    for (final Path p : paths)
    {
      Path mapfile = null;
      final FileStatus[] dirFiles = fs.listStatus(p);
      for (final FileStatus dirFile : dirFiles)
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
        TileIdWritable first;
        TileIdWritable last = new TileIdWritable();

        MapFile.Reader reader = new MapFile.Reader(mapfile, conf);
        first = (TileIdWritable) reader.getClosest(new TileIdWritable(0), val);

        try
        {
          reader.finalKey(last);
        }
        catch (IOException e)
        {
          // this could be an old file, try with the old writable
          org.mrgeo.core.mapreduce.formats.TileIdWritable old = new org.mrgeo.core.mapreduce.formats.TileIdWritable();
          reader.finalKey(old);
          last.set(old.get());
        }

        list.add(new FileSplit.FileSplitInfo(
            first.get(), last.get(), mapfile.getName(), partition++));
      }
    }

    splits = list.toArray(new FileSplit.FileSplitInfo[list.size()]);
  }

  final public SplitInfo getSplitByName(String name) throws SplitException
  {
    if (splits == null)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }
    for (SplitInfo split: splits)
    {
      if (((FileSplitInfo)split).getName().equals(name))
      {
        return split;
      }
    }

    throw new SplitException("Split not found (" + name + ")");
  }

  @Override
  public String findSpitFile(Path parent) throws IOException
  {
    Path file = new Path(parent, SPLIT_FILE);
    try
    {
      if (HadoopFileUtils.exists(file))
      {
        return file.toString();
      }
      else
      {
        file = new Path(parent, OLD_SPLIT_FILE);
        if (HadoopFileUtils.exists(file))
        {
          return file.toString();
        }
      }
    }
    catch (IOException e)
    {
      throw new IOException("Error opening split file: " + file.toString(), e);
    }

    throw new IOException("Split file not found: " + file.toString());
  }

  @Override
  public void generateSplits(SplitGenerator generator)
  {
    splits = generator.getSplits();
  }

  @Override
  public void readSplits(InputStream stream)
  {
    Scanner reader = new Scanner(stream);
    int count = reader.nextInt();
    splits = new FileSplitInfo[count];

    for (int i = 0; i < splits.length; i++)
    {
      splits[i] = new FileSplitInfo(reader.nextLong(), reader.nextLong(), reader.next(), reader.nextInt());
    }

    reader.close();
  }

  @Override
  public void readSplits(Path parent) throws IOException
  {
    super.readSplits(new Path(parent, SPLIT_FILE));
  }

  @Override
  public void writeSplits(OutputStream stream)
  {
    if (splits == null)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }

    PrintWriter writer = new PrintWriter(stream);
    writer.println(splits.length);
    for (SplitInfo split: splits)
    {
      writer.print(((FileSplitInfo) split).getStartId());
      writer.print(SPACER);
      writer.print(((FileSplitInfo)split).getEndId());
      writer.print(SPACER);
      writer.print(((FileSplitInfo)split).getName());
      writer.print(SPACER);
      writer.println(split.getPartition());
    }
    writer.close();
  }

  @Override
  public void writeSplits(Path parent) throws IOException
  {
    super.writeSplits(new Path(parent, SPLIT_FILE));
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

}
