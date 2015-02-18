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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestDataProviderFactory;
import org.mrgeo.utils.HadoopUtils;

public class HdfsImageIngestDataProviderFactory implements ImageIngestDataProviderFactory
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
  public ImageIngestDataProvider createImageIngestDataProvider(String input)
  {
    return new HdfsImageIngestDataProvider(getBasicConf(), input);
  }

  @Override
  public boolean canOpen(String name) throws IOException
  {
    return HdfsImageIngestDataProvider.canOpen(getBasicConf(), name);
  }

  @Override
  public boolean canWrite(String name) throws IOException
  {
    return HdfsImageIngestDataProvider.canWrite(getBasicConf(), name);
  }

  @Override
  public boolean exists(String name) throws IOException
  {
    return HdfsImageIngestDataProvider.exists(getBasicConf(), name);
  }

  @Override
  public void delete(String name) throws IOException
  {
    HdfsImageIngestDataProvider.delete(getBasicConf(), name);
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
