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

package org.mrgeo.hdfs.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageDataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsMrsImageDataProviderFactory implements MrsImageDataProviderFactory
{
  private static Configuration conf;

  private static class OnlyDirectoriesFilter implements PathFilter
  {
    private FileSystem fs;

    public OnlyDirectoriesFilter(FileSystem fs)
    {
      this.fs = fs;
    }

    @Override
    @SuppressWarnings("squid:S1166") // Exception caught and ignored
    public boolean accept(Path item)
    {
      try
      {
        if (fs.exists(item) && fs.isDirectory(item))
        {
          return true;
        }
      }
      catch (IOException ignored)
      {
      }
      return false;
    }
  }

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
  public MrsImageDataProvider createMrsImageDataProvider(final String input,
                                                         final ProviderProperties providerProperties)
  {
    return new HdfsMrsImageDataProvider(getConf(), input, providerProperties);
  }

  @Override
  public MrsImageDataProvider createTempMrsImageDataProvider(final ProviderProperties providerProperties) throws IOException
  {
    return createMrsImageDataProvider(HadoopFileUtils.createUniqueTmpPath().toUri().toString(), providerProperties);
  }

  @Override
  public boolean canOpen(final String input,
                         final ProviderProperties providerProperties) throws IOException
  {
    return HdfsMrsImageDataProvider.canOpen(getConf(), input, providerProperties);
  }

  @Override
  public boolean canWrite(final String input,
                          final ProviderProperties providerProperties) throws IOException
  {
    return HdfsMrsImageDataProvider.canWrite(getConf(), input, providerProperties);
  }

  private String[] listImages(final Configuration conf, Path usePath, String userName,
                              String[] authorizations,
                              ProviderProperties providerProperties) throws IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, usePath);
    FileStatus[] fileStatuses = fs.listStatus(usePath, new OnlyDirectoriesFilter(fs));
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
  public String[] listImages(final ProviderProperties providerProperties) throws IOException
  {
    // TODO: Extract user name and authorizations from providerProperties
    // and pass them along.
    Path usePath = getBasePath();
    return listImages(getConf(), usePath, "", new String[0], providerProperties);
  }

  private Path getBasePath()
  {
    return HdfsMrsImageDataProvider.getBasePath(getConf());
  }

  @Override
  public boolean exists(final String input,
                        final ProviderProperties providerProperties) throws IOException
  {
    return HdfsMrsImageDataProvider.exists(getConf(), input, providerProperties);
  }

  @Override
  public void delete(final String name,
                     final ProviderProperties providerProperties) throws IOException
  {
    if (exists(name, providerProperties))
    {
      HdfsMrsImageDataProvider.delete(getConf(), name, providerProperties);
    }
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
