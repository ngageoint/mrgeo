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

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class GeoWaveVectorDataProviderFactory implements VectorDataProviderFactory
{
static Logger log = LoggerFactory.getLogger(GeoWaveVectorDataProviderFactory.class);

@Override
public boolean isValid(Configuration conf)
{
  return GeoWaveVectorDataProvider.isValid(conf);
}

@Override
@SuppressWarnings("squid:S2696") // Exception caught and handled
public void initialize(Configuration config) throws DataProviderException
{
}

@Override
public String getPrefix()
{
  return "geowave";
}

@Override
public Map<String, String> getConfiguration()
{
  GeoWaveConnectionInfo connInfo = GeoWaveVectorDataProvider.getConnectionInfo();
  if (connInfo != null)
  {
    return connInfo.toMap();
  }
  return null;
}

@Override
public void setConfiguration(Map<String, String> settings)
{
  log.error("GeoWave classpath is: " + System.getProperty("java.class.path"));
  GeoWaveConnectionInfo connInfo = GeoWaveConnectionInfo.fromMap(settings);
  GeoWaveVectorDataProvider.setConnectionInfo(connInfo);
}

@Override
public VectorDataProvider createVectorDataProvider(final String prefix,
                                                   final String input,
                                                   final Configuration conf,
                                                   final ProviderProperties providerProperties)
{
  return new GeoWaveVectorDataProvider(conf, prefix, input, providerProperties);
}

@Override
public String[] listVectors(final Configuration conf,
                            final ProviderProperties providerProperties) throws IOException
{
  return GeoWaveVectorDataProvider.listVectors(providerProperties);
}

@Override
public boolean canOpen(final String input,
                       final Configuration conf,
                       final ProviderProperties providerProperties) throws IOException
{
  return GeoWaveVectorDataProvider.canOpen(input, providerProperties);
}

@Override
public boolean canWrite(final String input,
                        final Configuration conf,
                        final ProviderProperties providerProperties) throws IOException
{
  throw new IOException("GeoWave provider does not support writing vectors");
}

@Override
public boolean exists(final String name,
                      final Configuration conf,
                      final ProviderProperties providerProperties) throws IOException
{
  return canOpen(name, conf, providerProperties);
}

@Override
public void delete(final String name,
                   final Configuration conf,
                   final ProviderProperties providerProperties) throws IOException
{
  throw new IOException("GeoWave provider does not support deleting vectors");
}
}
