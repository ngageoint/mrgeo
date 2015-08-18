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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProtectionLevelValidator;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataWriter;

import java.io.IOException;

public interface TiledOutputFormatProvider extends ProtectionLevelValidator
{
  public OutputFormat<WritableComparable<?>, Writable> getOutputFormat();

  /**
   * For any additional Hadoop job input initialization besides setting
   * the actual output format class (see getOutputFormatClass method in
   * this interface), place that initialization code in this method.
   * 
   * @param job
   * @throws IOException 
   */
  public void setupJob(final Job job) throws DataProviderException;

  /**
   * For any additional Spark configuration besides setting
   * the actual output format class (see getOutputFormatClass method in
   * this interface), place that initialization code in this method.
   *
   * @param conf
   * @throws IOException
   */
  public Configuration setupSparkJob(final Configuration conf) throws DataProviderException;

  /**
   * Perform any processing required after the map/reduce has completed.
   * 
   * @param job
   */
  public void teardown(final Job job) throws DataProviderException;

  /**
   * Perform any processing required after a Spark job has completed.
   *
   * @param conf
   */
  public void teardownForSpark(final Configuration conf) throws DataProviderException;

  public MrsImagePyramidMetadataWriter getMetadataWriter();
  public MrsImageDataProvider getImageProvider();
}
