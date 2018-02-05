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

package org.mrgeo.data.vector.mbvectortiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.HdfsVectorDataProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MbVectorTilesDataProviderFactory implements VectorDataProviderFactory
{
  static Logger log = LoggerFactory.getLogger(MbVectorTilesDataProviderFactory.class);

  @Override
  public boolean isValid(Configuration conf)
  {
    return true;
  }

  @Override
  @SuppressWarnings("squid:S2696") // Exception caught and handled
  public void initialize(Configuration config) throws DataProviderException
  {
  }

  @Override
  public String getPrefix()
  {
    return "mbvt";
  }

  @Override
  public Map<String, String> getConfiguration()
  {
    return null;
  }

  @Override
  public void setConfiguration(Map<String, String> settings)
  {
  }

  @Override
  public VectorDataProvider createVectorDataProvider(final String prefix,
                                                     final String input,
                                                     final Configuration conf,
                                                     final ProviderProperties providerProperties)
  {
    return new MbVectorTilesDataProvider(conf, prefix, input, providerProperties);
  }

  @Override
  public String[] listVectors(final Configuration conf,
                              final ProviderProperties providerProperties) throws IOException
  {
    Path usePath = HdfsVectorDataProviderFactory.getBasePath(conf);
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, usePath);
    FileStatus[] fileStatuses = fs.listStatus(usePath);
    if (fileStatuses != null)
    {
      List<String> results = new ArrayList<>(fileStatuses.length);
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

  @Override
  public boolean canOpen(final String input,
                         final Configuration conf,
                         final ProviderProperties providerProperties) throws IOException
  {
    return MbVectorTilesDataProvider.canOpen(conf, input, providerProperties);
  }

  @Override
  public boolean canWrite(final String input,
                          final Configuration conf,
                          final ProviderProperties providerProperties) throws IOException
  {
    throw new IOException("MB vector tiles provider does not support writing vectors");
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
    throw new IOException("MB vector tiles provider does not support deleting vectors");
  }
}
