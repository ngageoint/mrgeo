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

package org.mrgeo.data.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public interface VectorDataProviderFactory
{
  /**
   * Provider implementations should perform any needed checks within this method
   * to determine if the other functions defined in this interface can be called
   * reliably. For example, if the implementation requires some configuration
   * settings in order to work properly, it can use this method to determine if
   * those settings have been defined. Returning false from this method will prevent
   * the provider from being called thereafter.
   * @return
   */
  public boolean isValid();

  public String getPrefix();

  public VectorDataProvider createVectorDataProvider(final String input);

  public String[] listVectors() throws IOException;
  
  public boolean canOpen(final String input) throws IOException;
  public boolean canWrite(final String input) throws IOException;
  public boolean exists(final String name) throws IOException;
  public void delete(final String name) throws IOException;
  public void configure(final Configuration conf);
  public void configure(final Properties p);
}
