package org.mrgeo.hdfs.utils;

import org.apache.hadoop.fs.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/10/16.
 */
public class FileSystemBuilder
{

private final FileSystem fileSystem;
// Use Path name in case the same instance is not passed when listStatus is called
private Map<String, List<FileStatus>> fileStatusMap = new HashMap<>();
private Map<String, InputStream> inputStreamMap = new HashMap<>();
private Map<String, OutputStream> outputStreamMap = new HashMap<>();

public FileSystemBuilder()
{
  this.fileSystem = mock(FileSystem.class);
}

public FileSystemBuilder fileStatus(Path parent, FileStatus fileStatus)
{
  List<FileStatus> fileStatusList = fileStatusMap.get(parent.getName());
  if (fileStatusList == null)
  {
    fileStatusList = new ArrayList<>();
    fileStatusMap.put(parent.getName(), fileStatusList);
  }
  fileStatusList.add(fileStatus);

  return this;
}

// Allows an input stream to be connected to a path
public FileSystemBuilder inputStream(Path file, InputStream fileInputStream)
{
  inputStreamMap.put(file.getName(), fileInputStream);

  return this;
}

// Allows an output stream to be connected to a path
public FileSystemBuilder outputStream(Path file, OutputStream fileOutputStream)
{
  outputStreamMap.put(file.getName(), fileOutputStream);

  return this;
}

public FileSystem build() throws IOException
{
  when(fileSystem.listStatus(any(Path.class))).thenAnswer(new Answer<FileStatus[]>()
  {

    @Override
    public FileStatus[] answer(InvocationOnMock invocationOnMock) throws Throwable
    {
      Path p = (Path) invocationOnMock.getArguments()[0];
      List<FileStatus> fileStatusList = fileStatusMap.get(p.getName());
      return fileStatusList != null ? fileStatusList.toArray(new FileStatus[fileStatusList.size()]) : new FileStatus[0];
    }
  });

  when(fileSystem.open(any(Path.class))).thenAnswer(new Answer<InputStream>()
  {
    @Override
    public InputStream answer(InvocationOnMock invocationOnMock) throws Throwable
    {
      Path p = (Path) invocationOnMock.getArguments()[0];
      // The FileSystem.open method returns a subclass of InputStream that cannot wrap any arbitrary stream
      // The solution is to make the FSDataInputStream and pass through any calls to the input stream
      // associated with the path
      return new FSDataInputStream(new MockInputStream(inputStreamMap.get(p.getName())));
    }
  });

  when(fileSystem.create(any(Path.class))).thenAnswer(new Answer<OutputStream>()
  {
    @Override
    public OutputStream answer(InvocationOnMock invocationOnMock) throws Throwable
    {
      Path p = (Path) invocationOnMock.getArguments()[0];
      // The FileSystem.open method returns a subclass of InputStream that cannot wrap any arbitrary stream
      // The solution is to make the FSDataInputStream and pass through any calls to the input stream
      // associated with the path
      return new FSDataOutputStream(new MockOutputStream(outputStreamMap.get(p.getName())));
    }
  });

  return fileSystem;
}

private static class MockInputStream extends InputStream implements Seekable, PositionedReadable
{

  private final InputStream inputStream;

  public MockInputStream(InputStream inputStream)
  {
    this.inputStream = inputStream;
  }

  // Mock implmentation of unused methods
  @Override
  public int read() throws IOException
  {
    return 0;
  }

  @Override
  public int read(long l, byte[] bytes, int i, int i1) throws IOException
  {
    return 0;
  }

  @Override
  public void readFully(long l, byte[] bytes, int i, int i1) throws IOException
  {
  }

  @Override
  public void readFully(long l, byte[] bytes) throws IOException
  {
  }

  @Override
  public void seek(long l) throws IOException
  {
  }

  @Override
  public long getPos() throws IOException
  {
    return 0;
  }

  @Override
  public boolean seekToNewSource(long l) throws IOException
  {
    return false;
  }

  public int read(byte[] buffer, int offset, int length) throws IOException
  {
    return inputStream.read(buffer, offset, length);
  }

}

private class MockOutputStream extends OutputStream
{
  private final OutputStream outputStream;

  public MockOutputStream(OutputStream outputStream)
  {
    this.outputStream = outputStream;
  }

  @Override
  public void write(int b) throws IOException
  {
    outputStream.write(b);
  }
}
}
