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

package org.mrgeo.data.tile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;

import java.io.IOException;

public interface TiledInputFormatProvider<V>
{
  /**
   * Return an input format to be used during map reduce jobs. This input
   * format however, will not be configured as the input format in the
   * job configuration. Instead a MrGeo input format like MrsImagePyramidInputFormat
   * will be used in the job itself. The class returned from this method
   * will be called by the MrsImagePyramidInputFormat class to perform
   * the actual input of data and then transform it into the actual inputs
   * passed to the mappers.
   * 
   * At a more detailed level, the input transformation referred to above
   * includes constructing a TileCollection which is emitted as the input
   * to the mappers. It includes surrounding tile neighborhood data (if
   * required) as well as corresponding tiles from other inputs (if required).
   * 
   * @param input
   * @return
   */
  public InputFormat<TileIdWritable, V> getInputFormat(final String input);

  /**
   * Perform all job setup required for your input format here. Typically that
   * involves job configuration settings for passing values to mappers and/or
   * reducers. Do not call setInputFormatClass. The core framework sets the
   * input format class to a core class like MrsImagePyramidInputFormat which
   * in turn uses your class returned from getInputFormat().
   * 
   * If you do call setInputFormatClass, you will likely get class incompatibility
   * exceptions when you run the job because mappers will expect to receive
   * TileCollections. Please see the description of getInputFormat() for more
   * information.
   * 
   * @param job
   */
  public void setupJob(final Job job,
      final MrsImageDataProvider provider) throws DataProviderException;

  /**
   * Perform all Spark job setup required for your input format here. Typically that
   * involves job configuration settings for passing values to mappers and/or
   * reducers. Do not call setInputFormatClass. The core framework sets the
   * input format class to a core class like MrsImagePyramidInputFormat which
   * in turn uses your class returned from getInputFormat().
   *
   * If you do call setInputFormatClass, you will likely get class incompatibility
   * exceptions when you run the job because mappers will expect to receive
   * TileCollections. Please see the description of getInputFormat() for more
   * information.
   *
   * @param conf
   * @param provider
   */
  public Configuration setupSparkJob(final Configuration conf,
                                     final MrsImageDataProvider provider)
          throws DataProviderException;

  /**
   * Perform any processing required after the map/reduce has completed.
   * 
   * @param job
   */
  public void teardown(final Job job) throws DataProviderException;
}
