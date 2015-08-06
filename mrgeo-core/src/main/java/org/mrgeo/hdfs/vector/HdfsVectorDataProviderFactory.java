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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

public class HdfsVectorDataProviderFactory implements VectorDataProviderFactory
{
  private static Configuration basicConf;

  @Override
  public boolean isValid(Configuration conf)
  {
    return true;
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
  public VectorDataProvider createVectorDataProvider(String prefix, String input, Configuration conf)
  {
    return new HdfsVectorDataProvider(conf, prefix, input, null);
  }

  @Override
  public VectorDataProvider createVectorDataProvider(String prefix, String input, Properties providerProperties)
  {
    return new HdfsVectorDataProvider(getBasicConf(), prefix, input, providerProperties);
  }

  @Override
  public String[] listVectors(Properties providerProperties) throws IOException
  {
    Path usePath = getBasePath();
    Configuration conf = getBasicConf();
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, usePath);
    FileStatus[] fileStatuses = fs.listStatus(usePath);
    if (fileStatuses != null)
    {
      List<String> results = new ArrayList<String>(fileStatuses.length);
      for (FileStatus status : fileStatuses)
      {
        if (canOpen(status.getPath().toString(), conf))
        {
          results.add(status.getPath().getName());
        }
      }
      String[] retVal = new String[results.size()];
      return results.toArray(retVal);
    }
    return new String[0];
  }

  private Path getBasePath()
  {
    return HdfsVectorDataProvider.getBasePath(getBasicConf());
  }

  @Override
  public boolean canOpen(String input, Configuration conf) throws IOException
  {
    return HdfsVectorDataProvider.canOpen(conf, input, null);
  }

  @Override
  public boolean canOpen(String input, Properties providerProperties) throws IOException
  {
    return HdfsVectorDataProvider.canOpen(getBasicConf(), input, providerProperties);
  }

  @Override
  public boolean canWrite(String input, Configuration conf) throws IOException
  {
    return HdfsVectorDataProvider.canWrite(conf, input, null);
  }

  @Override
  public boolean canWrite(String input, Properties providerProperties) throws IOException
  {
    return HdfsVectorDataProvider.canWrite(getBasicConf(), input, providerProperties);
  }

  @Override
  public boolean exists(String name, Configuration conf) throws IOException
  {
    return HdfsVectorDataProvider.exists(conf, name, null);
  }

  @Override
  public boolean exists(String name, Properties providerProperties) throws IOException
  {
    return HdfsVectorDataProvider.exists(getBasicConf(), name, providerProperties);
  }

  @Override
  public void delete(String name, Configuration conf) throws IOException
  {
    if (exists(name, conf))
    {
      HdfsVectorDataProvider.delete(conf, name, null);
    }
  }

  @Override
  public void delete(String name, Properties providerProperties) throws IOException
  {
    if (exists(name, providerProperties))
    {
      HdfsVectorDataProvider.delete(getBasicConf(), name, providerProperties);
    }
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
