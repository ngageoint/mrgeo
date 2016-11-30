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

package org.mrgeo.hdfs.adhoc;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.adhoc.AdHocDataProviderFactory;

import java.io.IOException;
import java.util.Map;

public class HdfsAdHocDataProviderFactory implements AdHocDataProviderFactory
{
  private static Configuration conf = null;

  @Override
  public boolean isValid()
  {
    return true;
  }

  @Override
  public void initialize(Configuration config)
  {
    if (conf == null)
    {
      conf = config;
    }
  }

  @Override
  public String getPrefix()
  {
    return "hdfs";
  }

  @Override
  public Map<String, String> getConfiguration()
  {
    return null;
  }

  @Override
  public void setConfiguration(Map<String, String> properties)
  {
  }

  @Override
  public AdHocDataProvider createAdHocDataProvider(String name,
      final ProviderProperties providerProperties) throws IOException
  {
    return new HdfsAdHocDataProvider(getConf(), name, providerProperties);
  }  

  @Override
  public AdHocDataProvider createAdHocDataProvider(final ProviderProperties providerProperties) throws IOException
  {
    return new HdfsAdHocDataProvider(getConf(), providerProperties);
  }

  @Override
  public boolean canOpen(final String name, final ProviderProperties providerProperties) throws IOException
  {
    return HdfsAdHocDataProvider.canOpen(getConf(), name, providerProperties);
  }

  @Override
  public boolean canWrite(final String name, final ProviderProperties providerProperties) throws IOException
  {
    return HdfsAdHocDataProvider.canWrite(name, getConf(), providerProperties);
  }

  @Override
  public boolean exists(String name, final ProviderProperties providerProperties) throws IOException
  {
    return HdfsAdHocDataProvider.exists(getConf(), name, providerProperties);
  }

  @Override
  public void delete(String name, final ProviderProperties providerProperties) throws IOException
  {
    HdfsAdHocDataProvider.delete(getConf(), name, null);
  }

  private static Configuration getConf()
  {
    if (conf == null)
    {
      throw new IllegalArgumentException("The configuration was not initialized");
    }
    return conf;
  }
}
