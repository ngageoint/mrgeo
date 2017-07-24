package org.mrgeo.hdfs.utils;

import org.apache.hadoop.mapreduce.InputSplit;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/10/16.
 */
public class TiledInputSplitBuilder
{
private TiledInputSplit tiledInputSplit;
private InputSplit wrappedSplit;

public TiledInputSplitBuilder()
{
  tiledInputSplit = mock(TiledInputSplit.class);
}

public TiledInputSplitBuilder wrappedSplit(InputSplit inputSplit)
{
  wrappedSplit = inputSplit;

  return this;
}

public TiledInputSplit build()
{
  when(tiledInputSplit.getWrappedSplit()).thenReturn(wrappedSplit);

  return tiledInputSplit;
}
}
