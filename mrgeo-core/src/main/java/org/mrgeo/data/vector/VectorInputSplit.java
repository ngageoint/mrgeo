package org.mrgeo.data.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.HadoopUtils;

public class VectorInputSplit extends InputSplit implements Writable
{
  private String vectorName;
  private InputSplit wrappedInputSplit;

  public VectorInputSplit(String vectorName, InputSplit wrappedInputSplit)
  {
    this.vectorName = vectorName;
    this.wrappedInputSplit = wrappedInputSplit;
  }

  public String getVectorName()
  {
    return vectorName;
  }

  public InputSplit getWrappedInputSplit()
  {
    return wrappedInputSplit;
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(vectorName);
    if (wrappedInputSplit instanceof Writable)
    {
      out.writeBoolean(true);
      out.writeUTF(wrappedInputSplit.getClass().getName());
      ((Writable)wrappedInputSplit).write(out);
    }
    else
    {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    vectorName = in.readUTF();
    boolean hasWrapped = in.readBoolean();
    if (hasWrapped)
    {
      String wrappedSplitClassName = in.readUTF();
      try
      {
        Class<?> splitClass = Class.forName(wrappedSplitClassName);
        wrappedInputSplit = (TiledInputSplit)ReflectionUtils.newInstance(splitClass, HadoopUtils.createConfiguration());
        ((Writable)wrappedInputSplit).readFields(in);
      }
      catch (ClassNotFoundException e)
      {
        throw new IOException(e);
      }
    }
  }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return wrappedInputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return wrappedInputSplit.getLocations();
  }
}
