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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.geometry.Geometry;

public interface VectorInputFormatProvider
{
  /**
   * Returns an instance of an InputFormat for the data provider that
   * is responsible for translating the keys and values from the native
   * InputFormat to LongWritable keys and Geometry values.
   * 
   * @return
   */
  public InputFormat<LongWritable, Geometry> getInputFormat(String input);

  /**
   * For performing all Hadoop job setup required to use this data source
   * as an InputFormat for a Hadoop map/reduce job, including the input
   * format format class.
   * 
   * @param job
   */
  public void setupJob(final Job job) throws DataProviderException;

  /**
   * Perform any processing required after the map/reduce has completed.
   * 
   * @param job
   */
  public void teardown(final Job job) throws DataProviderException;
}
