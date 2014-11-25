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

package org.mrgeo.data.ingest;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TiledInputFormatProvider;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.Properties;

public abstract class ImageIngestRawInputFormatProvider implements TiledInputFormatProvider<RasterWritable>
{
//  public abstract InputFormat<TileIdWritable, RasterWritable> getInputFormat();
//
  @Override
  public void setupJob(final Job job,
      final Properties providerProperties) throws DataProviderException
  {
    try
    {
      HadoopUtils.addJarCache(job, getClass());
      DataProviderFactory.saveProviderPropertiesToConfig(providerProperties,
          job.getConfiguration());
    }
    catch(IOException e)
    {
      throw new DataProviderException(e);
    }

  }
//  public abstract void teardown(final Job job) throws DataProviderException;

}
