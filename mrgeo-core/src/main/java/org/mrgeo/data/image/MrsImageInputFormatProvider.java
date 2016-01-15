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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.data.tile.TiledInputFormatProvider;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The base class for data plugins to use when implementing their own
 * image pyramid input format providers. Data plugins need to provide
 * a sub-class of this class if they wish to provide storage for
 * image pyramids.
 */
public abstract class MrsImageInputFormatProvider implements TiledInputFormatProvider<RasterWritable>
{
  private static final Logger log = LoggerFactory.getLogger(MrsImageInputFormatProvider.class);
  protected TiledInputFormatContext context;

  public MrsImageInputFormatProvider(TiledInputFormatContext config)
  {
    this.context = config;
  }

  /**
   * Sub-classes that override this method must call super.setupJob(job).
   */
  @Override
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
   * Any sub-classes that need to override this method to do additional setup
   * work should call super.setupJob(job) to perform default job setup too.
   */
  @Override
  public void setupJob(Job job, final MrsImageDataProvider provider) throws DataProviderException
  {
    provider.setupJob(job);
    setupConfig(job, provider);
  }

  private void setupConfig(final Job job,
                           final MrsImageDataProvider provider)
          throws DataProviderException
  {
    Configuration conf = job.getConfiguration();
    DataProviderFactory.saveProviderPropertiesToConfig(provider.getProviderProperties(), conf);
    context.save(conf);
  }
}
