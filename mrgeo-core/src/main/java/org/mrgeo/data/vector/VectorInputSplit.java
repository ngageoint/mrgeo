/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.data.vector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VectorInputSplit extends InputSplit implements Writable
{
  private String vectorName;
  private InputSplit wrappedInputSplit;

  public VectorInputSplit()
  {
  }

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
        wrappedInputSplit = (InputSplit)ReflectionUtils.newInstance(splitClass, HadoopUtils.createConfiguration());
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
