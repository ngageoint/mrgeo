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

package org.mrgeo.data.vector.pg;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;

import java.io.IOException;
import java.util.Map;

public class PgVectorDataProviderFactory implements VectorDataProviderFactory
{
  @Override
  public boolean isValid(Configuration conf)
  {
    return true;
  }

  @Override
  public void initialize(Configuration config) throws DataProviderException
  {
  }

  @Override
  public String getPrefix() {
    return "pg";
  }

  @Override
  public Map<String, String> getConfiguration()
  {
    // All configuration settings are included in the resource name
    // itself. Nothing to do here.
    return null;
  }

  @Override
  public void setConfiguration(Map<String, String> properties)
  {
    // All configuration settings are included in the resource name
    // itself. Nothing to do here.
  }

  @Override
  public VectorDataProvider createVectorDataProvider(
          final String prefix,
          final String input,
          final Configuration conf,
          final ProviderProperties providerProperties)
  {
    return new PgVectorDataProvider(conf, prefix, input, providerProperties);
  }

  @Override
  public String[] listVectors(final Configuration conf,
                              final ProviderProperties providerProperties) throws IOException
  {
    // We cannot give results for this method because all of the connection
    // information is contained in the resource name. So it is assumed that
    // the user knows all of the vectors available in the data source.
    return new String[0];
  }

  @Override
  public boolean canOpen(
          final String input,final Configuration conf,
          final ProviderProperties providerProperties) throws IOException
  {
    return PgVectorDataProvider.canOpen(input, providerProperties);
  }

  @Override
  public boolean canWrite(
          final String input,
          final Configuration conf,
          final ProviderProperties providerProperties) throws IOException
  {
    // We do not write vector data to Postgres yet
    return false;
  }

  @Override
  public boolean exists(
          final String name,
          final Configuration conf,
          final ProviderProperties providerProperties) throws IOException
  {
    return false;
  }

  @Override
  public void delete(final String name,
                     final Configuration conf,
                     final ProviderProperties providerProperties) throws IOException
  {
    throw new IOException("Cannot delete Postgres vector sources");
  }
}
