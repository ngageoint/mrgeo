/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

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

  public class SplitNotFoundException extends IOException
  {
    public SplitNotFoundException() {
      super();
    }
    public SplitNotFoundException(String message) {
      super(message);
    }
    public SplitNotFoundException(String message, Throwable cause) {
      super(message, cause);
    }
    public SplitNotFoundException(Throwable cause) {
      super(cause);
    }
  }

  public abstract String findSplitFile(Path parent) throws IOException;
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

  final public SplitInfo getSplitByPartitionIndex(int partitionIndex) throws SplitException
  {
    if (splits == null)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }
    else if (partitionIndex < 0 || partitionIndex >= splits.length)
    {
      throw new SplitException("Partition " + partitionIndex +
          " out of bounds. range 0 - " + (splits.length -1));
    }
    return splits[partitionIndex];
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
      return splits[findSplitIndex(tileId)];
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

  final public int getSplitIndex(long tileId) throws SplitException
  {
    if (splits == null || splits.length == 0)
    {
      throw new SplitException("Splits not generated, call readSplits() or generateSplits() first");
    }

    // If a tile before the start of the first tile is requested,
    // return the first split.
    if (splits[0].compareLT(tileId))
    {
      return 0;
    }
    // If a tile after the end of the last tile is requested,
    // return the last split.
    if (splits[splits.length-1].compareGT(tileId))
    {
      return splits.length - 1;
    }

    // lots of splits, use binary search
    if (splits.length > 1000)
    {
      return findSplitIndex(tileId);
    }
    // few splits, brute force search
    for (int i=0; i < splits.length; i++)
    {
      if (splits[i].compareLE(tileId))
      {
        return i;
      }
    }
    return splits.length - 1;
//    throw new SplitException("TileId out of range.  tile id: " + tileId +
//                             ".  splits: min: " + splits[0].getTileId() + ", max: " + splits[splits.length - 1].getTileId());
  }

  private int findSplitIndex(long tileId) throws SplitException
  {
    // First check the min and max values before binary searching.
    if (splits[0].compareLE(tileId))
    {
      return 0;
    }
    else if (splits[splits.length - 1].compareGT(tileId))
    {
      throw new SplitException("TileId out of range.  tile id: " + tileId +
          ".  splits: min: " + splits[0].getTileId() + ", max: " + splits[splits.length - 1].getTileId());
    }

    // The target does not fall in the minimum or maximum split, so let's
    // binary search to find it.
    return binarySearch(tileId, 0, splits.length - 1);
  }

  private int binarySearch(long target, int start, int end)
  {
    int mid = start + (end - start + 1) / 2;

    if (splits[mid - 1].compareGT(target) && splits[mid].compareLE(target))
    {
      return mid;
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

