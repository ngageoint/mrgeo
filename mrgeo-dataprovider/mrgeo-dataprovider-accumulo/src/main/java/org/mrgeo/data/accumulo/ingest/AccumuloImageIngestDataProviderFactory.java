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

package org.mrgeo.data.accumulo.ingest;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestDataProviderFactory;

import java.io.IOException;
import java.util.Properties;

public class AccumuloImageIngestDataProviderFactory implements ImageIngestDataProviderFactory
{
  @Override
  public boolean isValid()
  {
    // TODO: This is an initial guess at how this method should be
    // implemented. We need to revisit.
    Properties props = AccumuloConnector.getAccumuloProperties();
    if (props == null)
    {
      return false;
    }
    return true;
  }

  @Override
  public String getPrefix()
  {
    return MrGeoAccumuloConstants.MRGEO_ACC_PREFIX_NC;
  }
  
  @Override
  public ImageIngestDataProvider createImageIngestDataProvider(String input)
  {
    return null;
    //return new AccumuloImageIngestDataProvider(input);
  }

  @Override
  public boolean canOpen(String name) throws IOException
  {
    return false;
    //return AccumuloImageIngestDataProvider.canOpen(name);
  }
  
  @Override
  public boolean canWrite(String name) throws IOException
  {
    return false;
    //return AccumuloImageIngestDataProvider.canWrite(name);
  }

  @Override
  public boolean exists(String name) throws IOException
  {
    return false;
    //return AccumuloImageIngestDataProvider.exists(name);
  }

  @Override
  public void delete(String name) throws IOException
  {
    AccumuloImageIngestDataProvider.delete(name);
  }

  @Override
  public void configure(Configuration conf)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void configure(Properties p)
  {
    // TODO Auto-generated method stub
    
  }
} // end AccumuloImageIngestDataProviderFactory
