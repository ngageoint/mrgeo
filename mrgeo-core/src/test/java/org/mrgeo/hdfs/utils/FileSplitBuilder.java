package org.mrgeo.hdfs.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/13/16.
 */
public class FileSplitBuilder
{
private FileSplit fileSplit;
private Path path;

public FileSplitBuilder()
{
  this.fileSplit = mock(FileSplit.class);
}

public FileSplitBuilder path(Path path)
{
  this.path = path;

  return this;
}

public FileSplit build()
{
  when(fileSplit.getPath()).thenReturn(path);

  return fileSplit;
}
}
