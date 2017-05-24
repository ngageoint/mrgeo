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

package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class GeoWaveVectorInputFormatProvider extends VectorInputFormatProvider
{
private GeoWaveVectorDataProvider dataProvider;

public GeoWaveVectorInputFormatProvider(VectorInputFormatContext context,
    GeoWaveVectorDataProvider dataProvider)
{
  super(context);
  this.dataProvider = dataProvider;
}

@Override
public InputFormat<FeatureIdWritable, Geometry> getInputFormat(String input)
{
  return new GeoWaveVectorInputFormat();
}

@Override
public void setupJob(Job job, ProviderProperties providerProperties) throws DataProviderException
{
  super.setupJob(job, providerProperties);
  Configuration conf = job.getConfiguration();
  GeoWaveConnectionInfo connectionInfo = GeoWaveVectorDataProvider.getConnectionInfo();
  try
  {
    DataStorePluginOptions dspOptions = dataProvider.getDataStorePluginOptions();
    GeoWaveInputFormat.setDataStoreName(conf, dspOptions.getType());
    GeoWaveInputFormat.setStoreConfigOptions(conf, dspOptions.getFactoryOptionsAsMap());
    connectionInfo.writeToConfig(conf);

    // Configure CQL filtering if specified in the data provider
    String cql = dataProvider.getCqlFilter();
    if (cql != null && !cql.isEmpty())
    {
      conf.set(GeoWaveVectorRecordReader.CQL_FILTER, cql);
    }
  }
  catch (IOException e)
  {
    throw new DataProviderException(e);
  }
}

@Override
public void teardown(Job job) throws DataProviderException
{
}
}
