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

package org.mrgeo.data.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public interface VectorDataProviderFactory
{
  /**
   * This function is called by MrGeo so that providers can make sure they have
   * the necessary information to be used within MrGeo. For example, if login
   * or connection data must be configured properly in order for this provider
   * to work properly, that should be verified within this function. If this
   * function returns false, the provider will not be used within MrGeo.
   * 
   * If this method is called from within map/reduce task execution, the
   * Configuration passed in will be that from the mapper or reducer
   * job context. A typical scenario for a data provider is to include settings
   * required for validating the data provider in the Configuration during
   * job setup so that it will be available in the mapper and reducer.
   */
  public boolean isValid(final Configuration conf);

  /**
   * This function is called by MrGeo so that providers can make sure they have
   * the necessary information to be used within MrGeo. For example, if login
   * or connection data must be configured properly in order for this provider
   * to work properly, that should be verified within this function. If this
   * function returns false, the provider will not be used within MrGeo.
   * 
   * This particular signature is invoked by MrGeo when providers are needed
   * outside the context of a map/reduce task, for example while setting up
   * a map/reduce job before running it, or any time a provider is used outside
   * of map.reduce altogether. In this case, the provider should look for its
   * configuration on the server side (e.g. in MrGeoProperties if those settings
   * are configured in mrgeo.conf).
   */
  public boolean isValid();

  public String getPrefix();

  public VectorDataProvider createVectorDataProvider(final String prefix,
                                                     final String input,
                                                     final Configuration conf);
  public VectorDataProvider createVectorDataProvider(final String prefix,
                                                     final String input,
                                                     final Properties providerProperties);

  public String[] listVectors(final Properties providerProperties) throws IOException;
  
  public boolean canOpen(final String input,
      final Configuration conf) throws IOException;
  public boolean canOpen(final String input,
      final Properties providerProperties) throws IOException;

  public boolean canWrite(final String input,
      final Configuration conf) throws IOException;
  public boolean canWrite(final String input,
      final Properties providerProperties) throws IOException;

  public boolean exists(final String name,
      final Configuration conf) throws IOException;
  public boolean exists(final String name,
      final Properties providerProperties) throws IOException;

  public void delete(final String name,
      final Configuration conf) throws IOException;
  public void delete(final String name,
      final Properties providerProperties) throws IOException;
}
