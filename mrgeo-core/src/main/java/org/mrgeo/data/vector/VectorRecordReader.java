/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class VectorRecordReader extends RecordReader<FeatureIdWritable, Geometry>
{
private RecordReader<FeatureIdWritable, Geometry> delegate;

@Override
public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException
{
  if (inputSplit instanceof VectorInputSplit)
  {
    VectorInputSplit vis = (VectorInputSplit) inputSplit;
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(vis.getVectorName(),
        DataProviderFactory.AccessMode.READ,
        context.getConfiguration());
    delegate = dp.getRecordReader();
    delegate.initialize(vis.getWrappedInputSplit(), context);
  }
  else
  {
    throw new IOException("Input split not a VectorInputSplit");
  }
}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException
{
  return delegate.nextKeyValue();
}

@Override
public FeatureIdWritable getCurrentKey() throws IOException, InterruptedException
{
  return delegate.getCurrentKey();
}

@Override
public Geometry getCurrentValue() throws IOException, InterruptedException
{
  return delegate.getCurrentValue();
}

@Override
public float getProgress() throws IOException, InterruptedException
{
  return delegate.getProgress();
}

@Override
public void close() throws IOException
{
  delegate.close();
}
}
