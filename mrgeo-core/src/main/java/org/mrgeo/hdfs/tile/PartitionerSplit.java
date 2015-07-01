package org.mrgeo.hdfs.tile;

import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.partitioners.SplitGenerator;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.*;
import java.util.Scanner;

public class PartitionerSplit extends Splits
{
  private static final String SPACER = " ";
  public static final String SPLIT_FILE = "partitions";


  public static class PartitionerSplitInfo extends SplitInfo
  {
    private int partition;
    private long tileid;

    public PartitionerSplitInfo(long tileid, int partition)
    {
      this.tileid = tileid;
      this.partition = partition;
    }

    @Override
    boolean compareEQ(long tileId)
    {
      return tileId == this.tileid;
    }

    @Override
    boolean compareLE(long tileId)
    {
      return tileId <= this.tileid;
    }

    @Override
    boolean compareLT(long tileId)
    {
      return tileId < this.tileid;
    }

    @Override
    boolean compareGE(long tileId)
    {
      return tileId >= this.tileid;
    }

    @Override
    boolean compareGT(long tileId)
    {
      return tileId > this.tileid;
    }

    @Override
    public long getTileId()
    {
      return tileid;
    }

    @Override
    public int  getPartition() { return partition; }

    public String toString()
    {
      return "tile id = " + tileid + ", partition = " + partition;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
      out.writeLong(tileid);
      out.writeInt(partition);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
      tileid = in.readLong();
      partition = in.readInt();
    }
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
    splits = generator.getPartitions();
  }

  @Override
  public void readSplits(InputStream stream)
  {
    Scanner reader = new Scanner(stream);
    int count = reader.nextInt();
    splits = new PartitionerSplitInfo[count];

    for (int i = 0; i < splits.length; i++)
    {
      splits[i] = new PartitionerSplitInfo(reader.nextLong(), reader.nextInt());
    }

    reader.close();
  }

  @Override
  public void readSplits(Path parent) throws IOException
  {
    super.readSplits(new Path(parent, SPLIT_FILE));
  }

  @Override
  public void writeSplits(OutputStream stream) throws SplitException
  {
    if (splits == null)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }

    PrintWriter writer = new PrintWriter(stream);
    writer.println(splits.length);
    for (SplitInfo split: splits)
    {
      writer.print(split.getTileId());
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
    splits = new PartitionerSplitInfo[count];

    for (int i = 0; i < splits.length; i++)
    {
      splits[i] = new PartitionerSplitInfo(in.readLong(), in.readInt());
    }

  }

}
