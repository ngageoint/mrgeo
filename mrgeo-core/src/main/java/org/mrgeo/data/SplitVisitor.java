package org.mrgeo.data;

import org.mrgeo.mapreduce.splitters.TiledInputSplit;

public interface SplitVisitor
{
  public boolean accept(TiledInputSplit split);
}
