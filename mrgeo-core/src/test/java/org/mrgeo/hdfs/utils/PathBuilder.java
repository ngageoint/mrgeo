package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/10/16.
 */
public class PathBuilder
{

private Path path;
private FileSystem fileSystem;

public PathBuilder()
{
  this.path = mock(Path.class);
}

public PathBuilder fileSystem(FileSystem fileSystem)
{
  this.fileSystem = fileSystem;

  return this;
}

public Path build() throws IOException
{
  when(path.getFileSystem(any(Configuration.class))).thenReturn(fileSystem);

  return path;
}
}
