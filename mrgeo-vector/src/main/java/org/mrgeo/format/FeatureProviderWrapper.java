/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.featurefilter.SimplifiedFeatureIterator;
import org.mrgeo.data.FeatureIterator;
import org.mrgeo.data.FeatureProvider;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.HadoopUtils;

import java.io.*;
import java.util.LinkedList;

public class FeatureProviderWrapper implements FeatureProvider
{
  private static final long serialVersionUID = 1L;

  InputFormat<LongWritable, Geometry> format;
  transient TaskAttemptContext context;

  public FeatureProviderWrapper(InputFormat<LongWritable, Geometry> format,
      TaskAttemptContext context)
  {
    this.format = format;
    this.context = context;
  }

  private void readObject(ObjectInputStream aStream) throws IOException, ClassNotFoundException
  {
    aStream.defaultReadObject();
    
    format = (InputFormat<LongWritable, Geometry>)aStream.readObject();
    byte[] configByte = (byte[]) aStream.readObject();
    ByteArrayInputStream bais = new ByteArrayInputStream(configByte);
    Configuration conf = HadoopUtils.createConfiguration();
    conf.addResource(bais);
    context = HadoopUtils.createTaskAttemptContext(conf, new TaskAttemptID());
  }

  private void writeObject(ObjectOutputStream oStream) throws IOException
  {
    oStream.defaultWriteObject();

    oStream.writeObject(format);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    context.getConfiguration().writeXml(baos);
    oStream.writeObject(baos.toByteArray());
  }

  @Override
  public FeatureIterator iterator()
  {
    try
    {
      return new FeatureProviderWrapperIterator(format, context);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  public class FeatureProviderWrapperIterator extends SimplifiedFeatureIterator
  {
    RecordReader<LongWritable, Geometry> reader;
    LinkedList<InputSplit> splits;
    //TaskAttemptContext context;
    boolean primed = false;
    //InputFormat<LongWritable, Feature> format;

    FeatureProviderWrapperIterator(InputFormat<LongWritable, Geometry> inputFormat,
        TaskAttemptContext taskContext) throws IOException, InterruptedException
    {
      format = inputFormat;
      context = taskContext;
      splits = new LinkedList<InputSplit>(format.getSplits(context));
      reader = format.createRecordReader(splits.pop(), context);
    }

    @Override
    public Geometry simpleNext()
    {
      try
      {
        while (reader.nextKeyValue() == false)
        {
          reader.close();
          if (splits.size() > 0)
          {
            reader = format.createRecordReader(splits.pop(), context);
          }
          else
          {
            return null;
          }
        }
        return reader.getCurrentValue();
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws IOException
    {
      reader.close();
    }
  }
}
