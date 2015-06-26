package org.mrgeo.hdfs.tile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.partitioners.SplitGenerator;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.*;

public abstract class Splits implements Externalizable
{
  SplitInfo[] splits = null;

  public class SplitException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public SplitException(final Exception e)
    {
      this.origException = e;
      printStackTrace();
    }

    public SplitException(final String msg)
    {
      final Exception e = new Exception(msg);
      this.origException = e;
    }

    @Override
    public void printStackTrace()
    {
      origException.printStackTrace();
    }

  }

  public abstract String findSpitFile(Path parent) throws IOException;
  public abstract void generateSplits(SplitGenerator generator);

  public SplitInfo[] getSplits()
  {
    return splits;
  }

  public int length()
  {
    if (splits != null)
    {
      return splits.length;
    }

    return 0;
  }

  final public SplitInfo getSplitByPartition(int partition) throws SplitException
  {
    if (splits == null)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }
    else if (partition < 0 || partition >= splits.length)
    {
      throw new SplitException("Partition " + partition +
          " out of bounds. range 0 - " + (splits.length -1));
    }
    return splits[partition];
  }

  final public SplitInfo getSplit(long tileId) throws SplitException
  {
    if (splits == null)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }

    // lots of splits, use binary search
    if (splits.length > 1000)
    {
      return findSplit(tileId);
    }
    // few splits, brute force search
    for (SplitInfo split: splits)
    {
      if (split.compareLE(tileId))
      {
        return split;
      }
    }

    throw new SplitException("TileId out of range.  tile id: " + tileId +
        ".  splits: min: " + splits[0].getTileId() + ", max: " + splits[splits.length - 1].getTileId());
  }

  private SplitInfo findSplit(long tileId)
  {
    // First check the min and max values before binary searching.
    if (splits[0].compareLE(tileId))
    {
      return splits[0];
    }
    if (splits[splits.length - 1].compareGT(tileId))
    {
      throw new SplitException("TileId out of range.  tile id: " + tileId +
          ".  splits: min: " + splits[0].getTileId() + ", max: " + splits[splits.length - 1].getTileId());
    }

    // The target does not fall in the minimum or maximum split, so let's
    // binary search to find it.
    return binarySearch(tileId, 0, splits.length - 1);
  }

  private SplitInfo binarySearch(long target, int start, int end)
  {
    int mid = start + (end - start + 1) / 2;

    if (splits[mid - 1].compareGT(target) && splits[mid].compareLE(target))
    {
      return splits[mid];
    }
    else if (splits[mid].compareLT(target))
    {
      return binarySearch(target, start, mid - 1);
    }
    else {
      return binarySearch(target, mid, end);
    }
  }

  public abstract void readSplits(InputStream stream);
  public void readSplits(Path parent) throws IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(parent);
    InputStream stream =  fs.open(parent);

    readSplits(stream);
    stream.close();
  }


  public abstract void writeSplits(OutputStream stream);
  public void writeSplits(Path parent) throws IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(parent);
    OutputStream stream =  fs.create(parent, true);

    writeSplits(stream);
    stream.close();
  }

  @Override
  final public void writeExternal(ObjectOutput out) throws IOException
  {
    out.writeInt(splits.length);
    for (SplitInfo split: splits)
    {
      split.writeExternal(out);
    }
  }

}

