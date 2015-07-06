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

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;

import java.io.IOException;

public interface VectorOutputFormatProvider
{
  /**
   * Providers must perform any required Hadoop job setup when outputting
   * vector data.
   * 
   * @param job
   * @throws IOException 
   */
  public void setupJob(final Job job) throws DataProviderException, IOException;

  /**
   * Perform any processing required after the map/reduce has completed.
   * 
   * @param job
   */
  public void teardown(final Job job) throws DataProviderException;
}
