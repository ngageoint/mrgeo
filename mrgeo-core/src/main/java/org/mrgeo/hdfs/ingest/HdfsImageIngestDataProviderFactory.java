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

package org.mrgeo.hdfs.ingest;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestDataProviderFactory;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.Properties;

public class HdfsImageIngestDataProviderFactory implements ImageIngestDataProviderFactory
{
  private Configuration conf;
  @SuppressWarnings("unused")
  private Properties props;

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
  public ImageIngestDataProvider createImageIngestDataProvider(String input)
  {
    return new HdfsImageIngestDataProvider(conf, input);
  }

  @Override
  public boolean canOpen(String name) throws IOException
  {
    return HdfsImageIngestDataProvider.canOpen(conf, name);
  }

  @Override
  public boolean canWrite(String name) throws IOException
  {
    return HdfsImageIngestDataProvider.canWrite(conf, name);
  }

  @Override
  public boolean exists(String name) throws IOException
  {
    return HdfsImageIngestDataProvider.exists(conf, name);
  }

  @Override
  public void delete(String name) throws IOException
  {
    HdfsImageIngestDataProvider.delete(conf, name);
  }

  @Override
  public void configure(final Configuration conf)
  {
    this.conf = conf;
    this.props = null;
  }

  @Override
  public void configure(final Properties props)
  {
    // This is called when the factory is used from the
    // name node side, so there is no Hadoop job in progress.
    // However, we still need a Hadoop configuration in order
    // to access HDFS.
    this.conf = HadoopUtils.createConfiguration();
    this.props = props;
  }
}
