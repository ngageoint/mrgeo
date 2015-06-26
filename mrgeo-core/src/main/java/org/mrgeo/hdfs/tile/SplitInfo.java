package org.mrgeo.hdfs.tile;

import java.io.Externalizable;

abstract public class SplitInfo implements Externalizable
{
  abstract boolean compareEQ(long tileId);
  abstract boolean compareLE(long tileId);
  abstract boolean compareLT(long tileId);
  abstract boolean compareGE(long tileId);
  abstract boolean compareGT(long tileId);

  public abstract long getTileId();
  public abstract int getPartition();
}
