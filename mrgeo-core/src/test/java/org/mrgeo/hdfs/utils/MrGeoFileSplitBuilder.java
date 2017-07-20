package org.mrgeo.hdfs.utils;

import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/17/16.
 */
public class MrGeoFileSplitBuilder
{
private FileSplit fileSplit;
private List<FileSplitInfo> splits = new ArrayList<>();

public MrGeoFileSplitBuilder()
{
  this.fileSplit = mock(FileSplit.class);
}

public MrGeoFileSplitBuilder split(FileSplitInfo split)
{
  this.splits.add(split);

  return this;
}

public FileSplit build()
{
  FileSplitInfo[] splitsArray = new FileSplitInfo[splits.size()];
  when(fileSplit.getSplits()).thenReturn(splits.toArray(splitsArray));
  return fileSplit;
}
}
