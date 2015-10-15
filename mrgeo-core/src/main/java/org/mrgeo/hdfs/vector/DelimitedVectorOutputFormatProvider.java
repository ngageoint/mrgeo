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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorOutputFormatContext;
import org.mrgeo.data.vector.VectorOutputFormatProvider;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class DelimitedVectorOutputFormatProvider implements VectorOutputFormatProvider
{
  private HdfsVectorDataProvider provider;
  private VectorOutputFormatContext context;

  public DelimitedVectorOutputFormatProvider(HdfsVectorDataProvider provider,
                                             VectorOutputFormatContext context)
  {
    this.provider = provider;
    this.context = context;
  }

  @Override
  public OutputFormat<FeatureIdWritable, Geometry> getOutputFormat(String input)
  {
    return new CsvOutputFormat();
  }

  @Override
  public void setupJob(Job job) throws DataProviderException, IOException
  {
    job.setOutputKeyClass(FeatureIdWritable.class);
    job.setOutputValueClass(Geometry.class);
    job.setOutputFormatClass(CsvOutputFormat.class);
    CsvOutputFormat.setup(new Path(provider.getResolvedResourceName(false)), job);
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }
}
