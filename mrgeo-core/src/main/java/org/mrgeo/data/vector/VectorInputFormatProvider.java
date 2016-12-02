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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.geometry.Geometry;

public abstract class VectorInputFormatProvider
{
private VectorInputFormatContext context;

public VectorInputFormatProvider(VectorInputFormatContext context)
{
  this.context = context;
}

/**
 * Returns an instance of an InputFormat for the data provider that
 * is responsible for translating the keys and values from the native
 * InputFormat to FeatureIdWritable keys and Geometry values.
 *
 * @return
 */
public abstract InputFormat<FeatureIdWritable, Geometry> getInputFormat(String input);

/**
 * For performing all Hadoop job setup required to use this data source
 * as an InputFormat for a Hadoop map/reduce job, including the input
 * format format class.
 *
 * @param job
 */
public void setupJob(final Job job,
    final ProviderProperties providerProperties) throws DataProviderException
{
  setup(job.getConfiguration(), providerProperties);
}

public Configuration setupSparkJob(final Configuration conf,
    final ProviderProperties providerProperties) throws DataProviderException
{
  setup(conf, providerProperties);
  return conf;
}

/**
 * Perform any processing required after the map/reduce has completed.
 *
 * @param job
 */
public void teardown(final Job job) throws DataProviderException
{
}

protected VectorInputFormatContext getContext()
{
  return context;
}

private void setup(final Configuration conf, final ProviderProperties providerProperties)
{
  DataProviderFactory.saveProviderPropertiesToConfig(providerProperties,
      conf);
  context.save(conf);
}
}
