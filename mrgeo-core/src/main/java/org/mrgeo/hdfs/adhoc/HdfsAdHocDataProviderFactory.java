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

package org.mrgeo.hdfs.adhoc;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.adhoc.AdHocDataProviderFactory;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.Properties;

public class HdfsAdHocDataProviderFactory implements AdHocDataProviderFactory
{
  private static Configuration basicConf;

  @Override
  public boolean isValid()
  {
    return true;
  }

  @Override
  public String getPrefix()
  {
    return "hdfs";
  }

  @Override
  public AdHocDataProvider createAdHocDataProvider(String name,
      final Configuration conf) throws IOException
  {
    return new HdfsAdHocDataProvider(conf, name, null);
  }  

  @Override
  public AdHocDataProvider createAdHocDataProvider(String name,
      final Properties providerProperties) throws IOException
  {
    return new HdfsAdHocDataProvider(getBasicConf(), name, providerProperties);
  }  

  @Override
  public AdHocDataProvider createAdHocDataProvider(final Configuration conf) throws IOException
  {
    return new HdfsAdHocDataProvider(conf, null);
  }

  @Override
  public AdHocDataProvider createAdHocDataProvider(final Properties providerProperties) throws IOException
  {
    return new HdfsAdHocDataProvider(getBasicConf(), providerProperties);
  }

  @Override
  public boolean canOpen(final String name, final Configuration conf) throws IOException
  {
    return HdfsAdHocDataProvider.canOpen(conf, name, null);
  }

  @Override
  public boolean canOpen(final String name, final Properties providerProperties) throws IOException
  {
    return HdfsAdHocDataProvider.canOpen(getBasicConf(), name, providerProperties);
  }

  @Override
  public boolean canWrite(final String name, final Configuration conf) throws IOException
  {
    return HdfsAdHocDataProvider.canWrite(name, conf, null);
  }

  @Override
  public boolean canWrite(final String name, final Properties providerProperties) throws IOException
  {
    return HdfsAdHocDataProvider.canWrite(name, getBasicConf(), providerProperties);
  }

  @Override
  public boolean exists(String name, final Configuration conf) throws IOException
  {
    return HdfsAdHocDataProvider.exists(conf, name, null);
  }

  @Override
  public boolean exists(String name, final Properties providerProperties) throws IOException
  {
    return HdfsAdHocDataProvider.exists(getBasicConf(), name, providerProperties);
  }

  @Override
  public void delete(String name, final Configuration conf) throws IOException
  {
    HdfsAdHocDataProvider.delete(conf, name, null);
  }

  @Override
  public void delete(String name, final Properties providerProperties) throws IOException
  {
    HdfsAdHocDataProvider.delete(getBasicConf(), name, null);
  }

  private static Configuration getBasicConf()
  {
    if (basicConf == null)
    {
      basicConf = HadoopUtils.createConfiguration();
    }
    return basicConf;
  }
}
