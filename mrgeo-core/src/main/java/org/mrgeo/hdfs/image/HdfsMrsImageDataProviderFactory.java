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
@Override
public boolean isValid()
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
public MrsImageDataProvider createMrsImageDataProvider(final String input,
    final Configuration conf,
    final ProviderProperties providerProperties)
{
  return new HdfsMrsImageDataProvider(conf, input, providerProperties);
}

@Override
public MrsImageDataProvider createTempMrsImageDataProvider(final Configuration conf, final ProviderProperties providerProperties)
    throws IOException
{
  return createMrsImageDataProvider(HadoopFileUtils.createUniqueTmpPath().toUri().toString(),
          conf, providerProperties);
}

@Override
public boolean canOpen(final String input,
                       final Configuration conf,
                       final ProviderProperties providerProperties) throws IOException
{
  return HdfsMrsImageDataProvider.canOpen(conf, input, providerProperties);
}

@Override
public boolean canWrite(final String input,
                        final Configuration conf,
                        final ProviderProperties providerProperties) throws IOException
{
  return HdfsMrsImageDataProvider.canWrite(conf, input, providerProperties);
}

@Override
public String[] listImages(final Configuration conf,
                           final ProviderProperties providerProperties) throws IOException
{
  // TODO: Extract user name and authorizations from providerProperties
  // and pass them along.
  Path usePath = getBasePath(conf);
  return listImages(conf, usePath, "", new String[0], providerProperties);
}

@Override
public boolean exists(final String input,
                      final Configuration conf,
                      final ProviderProperties providerProperties) throws IOException
{
  return HdfsMrsImageDataProvider.exists(conf, input, providerProperties);
}

@Override
public void delete(final String name,
                   final Configuration conf,
                   final ProviderProperties providerProperties) throws IOException
{
  if (exists(name, conf, providerProperties))
  {
    HdfsMrsImageDataProvider.delete(conf, name, providerProperties);
  }
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
      if (canOpen(status.getPath().toString(), conf, providerProperties))
      {
        results.add(status.getPath().getName());
      }
    }
    String[] retVal = new String[results.size()];
    return results.toArray(retVal);
  }
  return new String[0];
}

private Path getBasePath(final Configuration conf)
{
  return HdfsMrsImageDataProvider.getBasePath(conf);
}

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
}
