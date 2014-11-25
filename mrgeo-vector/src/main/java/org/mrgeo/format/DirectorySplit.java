package org.mrgeo.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DirectorySplit extends InputSplit implements Writable
{
  private Path _subdir;
  private String[] _hosts;

  // Not allowed
  @SuppressWarnings("unused")
  private DirectorySplit()
  {
  }

  public DirectorySplit(Path subdir, String[] hosts)
  {
    this._subdir = subdir;
    this._hosts = hosts;
    if (this._hosts == null)
    {
      this._hosts = new String[0];
    }
  }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return _hosts;
  }
  
  public Path getPath()
  {
    return _subdir;
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    String strSubdir = in.readUTF();
    _subdir = new Path(strSubdir);
    int hostCount = in.readInt();
    _hosts = new String[hostCount];
    for (int ii=0; ii < hostCount; ii++)
    {
      _hosts[ii] = in.readUTF();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(_subdir.toString());
    out.writeInt(_hosts.length);
    for (String host : _hosts)
    {
      out.writeUTF(host);
    }
  }
}
