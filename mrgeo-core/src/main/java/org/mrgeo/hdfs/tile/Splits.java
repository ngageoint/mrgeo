package org.mrgeo.hdfs.tile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.partitioners.SplitGenerator;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.*;

public abstract class Splits implements Externalizable
{
  SplitInfo[] splits = null;

  public class SplitException extends IOException
  {
    static final long serialVersionUID = 7818375828146090155L;

    public SplitException() {
      super();
    }
    public SplitException(String message) {
      super(message);
    }
    public SplitException(String message, Throwable cause) {
      super(message, cause);
    }
    public SplitException(Throwable cause) {
      super(cause);
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
    if (splits == null || splits.length == 0)
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

  private SplitInfo findSplit(long tileId) throws SplitException
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

  public abstract void readSplits(InputStream stream) throws SplitException;
  public void readSplits(Path parent) throws IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(parent);
    try (InputStream stream = fs.open(parent))
    {
      readSplits(stream);
    }
  }


  public abstract void writeSplits(OutputStream stream) throws SplitException;
  public void writeSplits(Path parent) throws IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(parent);
    try (OutputStream stream =  fs.create(parent, true))
    {
      writeSplits(stream);
    }
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

