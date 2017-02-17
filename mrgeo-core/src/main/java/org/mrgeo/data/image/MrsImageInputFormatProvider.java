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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The base class for data plugins to use when implementing their own
 * image pyramid input format providers. Data plugins need to provide
 * a sub-class of this class if they wish to provide storage for
 * image pyramids.
 */
public abstract class MrsImageInputFormatProvider
{
private static final Logger log = LoggerFactory.getLogger(MrsImageInputFormatProvider.class);
protected ImageInputFormatContext context;

public MrsImageInputFormatProvider(ImageInputFormatContext config)
{
  this.context = config;
}

/**
 * Return an input format to be used during map reduce jobs. This input
 * format however, will not be configured as the input format in the
 * job configuration. Instead a MrGeo input format like MrsImagePyramidInputFormat
 * will be used in the job itself. The class returned from this method
 * will be called by the MrsImagePyramidInputFormat class to perform
 * the actual input of data and then transform it into the actual inputs
 * passed to the mappers.
 *
 * @param input
 * @return
 */
public abstract InputFormat<TileIdWritable, RasterWritable> getInputFormat(final String input);

/**
 * Perform all Spark job setup required for your input format here. Typically that
 * involves job configuration settings for passing values to mappers and/or
 * reducers. Do not call setInputFormatClass. The core framework sets the
 * input format class to a core class like MrsImagePyramidInputFormat which
 * in turn uses your class returned from getInputFormat().
 * <p>
 * If you do call setInputFormatClass, you will likely get class incompatibility
 * exceptions when you run the job because mappers will expect to receive
 * TileCollections. Please see the description of getInputFormat() for more
 * information.
 * <p>
 * Sub-classes that override this method must call super.setupJob(job).
 *
 * @param conf
 * @param provider
 */
public Configuration setupSparkJob(Configuration conf, MrsImageDataProvider provider)
    throws DataProviderException
{
  try
  {
    Configuration conf1 = provider.setupSparkJob(conf);
    Job job = new Job(conf1);
    setupConfig(job, provider);
    return job.getConfiguration();
  }
  catch (IOException e)
  {
    throw new DataProviderException("Failure configuring map/reduce job " + context.toString(), e);
  }
}

/**
 * Perform all job setup required for your input format here. Typically that
 * involves job configuration settings for passing values to mappers and/or
 * reducers. Do not call setInputFormatClass. The core framework sets the
 * input format class to a core class like MrsImagePyramidInputFormat which
 * in turn uses your class returned from getInputFormat().
 * <p>
 * If you do call setInputFormatClass, you will likely get class incompatibility
 * exceptions when you run the job because mappers will expect to receive
 * TileCollections. Please see the description of getInputFormat() for more
 * information.
 * <p>
 * Any sub-classes that need to override this method to do additional setup
 * work should call super.setupJob(job) to perform default job setup too.
 *
 * @param job
 */
public void setupJob(Job job, final MrsImageDataProvider provider) throws DataProviderException
{
  provider.setupJob(job);
  setupConfig(job, provider);
}


/**
 * Perform any processing required after the map/reduce has completed.
 *
 * @param job
 */
public abstract void teardown(final Job job) throws DataProviderException;

private void setupConfig(final Job job,
    final MrsImageDataProvider provider)
    throws DataProviderException
{
  Configuration conf = job.getConfiguration();
  DataProviderFactory.saveProviderPropertiesToConfig(provider.getProviderProperties(), conf);
  context.save(conf);
}
}
