/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.hdfs.image;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageDataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

public class HdfsMrsImageDataProviderFactory implements MrsImageDataProviderFactory
{
  private static Configuration basicConf;

  private class OnlyDirectoriesFilter implements PathFilter
  {
    private FileSystem fs;

    public OnlyDirectoriesFilter(FileSystem fs)
    {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path item)
    {
      try
      {
        if (fs.exists(item) && fs.isDirectory(item))
        {
          return true;
        }
      }
      catch(IOException e)
      {
      }
      return false;
    }
  }

  @Override
  public String getPrefix()
  {
    return "hdfs";
  }

  @Override
  public MrsImageDataProvider createMrsImageDataProvider(final String input,
      final Configuration conf)
  {
    return new HdfsMrsImageDataProvider(conf, input, null);
  }

  @Override
  public MrsImageDataProvider createMrsImageDataProvider(final String input,
      final Properties providerProperties)
  {
    return new HdfsMrsImageDataProvider(getBasicConf(), input, providerProperties);
  }

  @Override
  public boolean canOpen(final String input,
      final Configuration conf) throws IOException
  {
    return HdfsMrsImageDataProvider.canOpen(conf, input, null);
  }

  @Override
  public boolean canOpen(final String input,
      final Properties providerProperties) throws IOException
  {
    return HdfsMrsImageDataProvider.canOpen(getBasicConf(), input, providerProperties);
  }

  @Override
  public boolean canWrite(final String input,
      final Configuration conf) throws IOException
  {
    return HdfsMrsImageDataProvider.canWrite(conf, input, null);
  }

  @Override
  public boolean canWrite(final String input,
      final Properties providerProperties) throws IOException
  {
    return HdfsMrsImageDataProvider.canWrite(getBasicConf(), input, providerProperties);
  }

  private String[] listImages(final Configuration conf, Path usePath, String userName,
      String[] authorizations) throws IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, usePath);
    FileStatus[] fileStatuses = fs.listStatus(usePath, new OnlyDirectoriesFilter(fs));
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

  @Override
  public String[] listImages(final Properties providerProperties) throws IOException
  {
    // TODO: Extract user name and authorizations from providerProperties
    // and pass them along.
    Path usePath = getBasePath();
    return listImages(HadoopUtils.createConfiguration(), usePath, "", new String[0]);
  }

  private Path getBasePath()
  {
    return HdfsMrsImageDataProvider.getBasePath(getBasicConf());
  }

  @Override
  public boolean exists(final String input,
      final Configuration conf) throws IOException
  {
    return HdfsMrsImageDataProvider.exists(conf, input, null);
  }

  @Override
  public boolean exists(final String input,
      final Properties providerProperties) throws IOException
  {
    return HdfsMrsImageDataProvider.exists(getBasicConf(), input, providerProperties);
  }

  @Override
  public void delete(final String name,
      final Configuration conf) throws IOException
  {
    if (exists(name, conf))
    {
      HdfsMrsImageDataProvider.delete(conf, name, null);
    }
  }

  @Override
  public void delete(final String name,
      final Properties providerProperties) throws IOException
  {
    if (exists(name, providerProperties))
    {
      HdfsMrsImageDataProvider.delete(getBasicConf(), name, providerProperties);
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
