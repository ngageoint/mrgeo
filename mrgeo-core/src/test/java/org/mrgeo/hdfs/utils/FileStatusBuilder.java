package org.mrgeo.hdfs.utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/22/16.
 */
public class FileStatusBuilder
{
private FileStatus fileStatus;
private Path path;

public FileStatusBuilder()
{
  this.fileStatus = mock(FileStatus.class);
}

public FileStatusBuilder path(Path path)
{
  this.path = path;

  return this;
}

public FileStatus build()
{
  when(fileStatus.getPath()).thenReturn(path);

  return fileStatus;
}


}
