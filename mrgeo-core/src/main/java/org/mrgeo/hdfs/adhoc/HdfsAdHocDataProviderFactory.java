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

package org.mrgeo.hdfs.adhoc;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.adhoc.AdHocDataProviderFactory;

import java.io.IOException;
import java.util.Map;

public class HdfsAdHocDataProviderFactory implements AdHocDataProviderFactory
{
@Override
public boolean isValid(Configuration conf)
{
  return true;
}

@Override
@SuppressWarnings("squid:S2696") // need to keep the conf static, but want to only set it with the object.  yuck!
public void initialize(Configuration config)
{
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
                                                 Configuration conf,
                                                 ProviderProperties providerProperties) throws IOException
{
  return new HdfsAdHocDataProvider(conf, name, providerProperties);
}

@Override
public AdHocDataProvider createAdHocDataProvider(Configuration conf,
                                                 ProviderProperties providerProperties) throws IOException
{
  return new HdfsAdHocDataProvider(conf, providerProperties);
}

@Override
public boolean canOpen(String name,
                       Configuration conf,
                       ProviderProperties providerProperties) throws IOException
{
  return HdfsAdHocDataProvider.canOpen(conf, name, providerProperties);
}

@Override
public boolean canWrite(String name,
                        Configuration conf,
                        ProviderProperties providerProperties) throws IOException
{
  return HdfsAdHocDataProvider.canWrite(name, conf, providerProperties);
}

@Override
public boolean exists(String name,
                      Configuration conf,
                      ProviderProperties providerProperties) throws IOException
{
  return HdfsAdHocDataProvider.exists(conf, name, providerProperties);
}

@Override
public void delete(String name,
                   Configuration conf,
                   ProviderProperties providerProperties) throws IOException
{
  HdfsAdHocDataProvider.delete(conf, name, null);
}
}
