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

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsVectorDataProviderFactory implements VectorDataProviderFactory
{
private static Configuration conf;

private static Configuration getConf()
{
  if (conf == null)
  {
    throw new IllegalArgumentException("The configuration was not initialized");
  }
  return conf;
}

@Override
public boolean isValid(Configuration conf)
{
  return true;
}

@Override
@SuppressWarnings("squid:S2696") // need to keep the conf static, but want to only set it with the object.  yuck!
public void initialize(Configuration config)
{
  if (conf == null)
  {
    conf = config;
  }
}

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
public Map<String, String> getConfiguration()
{
  return null;
}

@Override
public void setConfiguration(Map<String, String> properties)
{
}

@Override
public VectorDataProvider createVectorDataProvider(String prefix, String input, ProviderProperties providerProperties)
{
  return new HdfsVectorDataProvider(getConf(), prefix, input, providerProperties);
}

@Override
public String[] listVectors(ProviderProperties providerProperties) throws IOException
{
  Path usePath = getBasePath();
  Configuration conf = getConf();
  FileSystem fs = HadoopFileUtils.getFileSystem(conf, usePath);
  FileStatus[] fileStatuses = fs.listStatus(usePath);
  if (fileStatuses != null)
  {
    List<String> results = new ArrayList<String>(fileStatuses.length);
    for (FileStatus status : fileStatuses)
    {
      if (canOpen(status.getPath().toString(), providerProperties))
      {
        results.add(status.getPath().getName());
      }
    }
    String[] retVal = new String[results.size()];
    return results.toArray(retVal);
  }
  return new String[0];
}

@Override
public boolean canOpen(String input, ProviderProperties providerProperties) throws IOException
{
  return HdfsVectorDataProvider.canOpen(getConf(), input, providerProperties);
}

@Override
public boolean canWrite(String input, ProviderProperties providerProperties) throws IOException
{
  return HdfsVectorDataProvider.canWrite(getConf(), input, providerProperties);
}

@Override
public boolean exists(String name, ProviderProperties providerProperties) throws IOException
{
  return HdfsVectorDataProvider.exists(getConf(), name, providerProperties);
}

@Override
public void delete(String name, ProviderProperties providerProperties) throws IOException
{
  if (exists(name, providerProperties))
  {
    HdfsVectorDataProvider.delete(getConf(), name, providerProperties);
  }
}

private Path getBasePath()
{
  return HdfsVectorDataProvider.getBasePath(getConf());
}
}
