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

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class ShapefileVectorInputFormatProvider extends VectorInputFormatProvider
{
  public ShapefileVectorInputFormatProvider(VectorInputFormatContext context)
  {
    super(context);
  }

  @Override
  public InputFormat<LongWritable, Geometry> getInputFormat(String input)
  {
    return new ShpInputFormat();
  }

  @Override
  public void setupJob(Job job, ProviderProperties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    for (String input: getContext().getInputs())
    {
      try
      {
        // Set up native input format
        TextInputFormat.addInputPath(job, new Path(input));
      }
      catch (IOException e)
      {
        throw new DataProviderException(e);
      }
    }
  }
}
