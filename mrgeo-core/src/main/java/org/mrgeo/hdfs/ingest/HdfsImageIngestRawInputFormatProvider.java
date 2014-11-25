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

package org.mrgeo.hdfs.ingest;

import java.util.Properties;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ingest.ImageIngestRawInputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.ingest.format.IngestImageSplittingInputFormat;

public class HdfsImageIngestRawInputFormatProvider extends ImageIngestRawInputFormatProvider
{

  
  @Override
  public InputFormat<TileIdWritable, RasterWritable> getInputFormat(final String input)
  {
    return new IngestImageSplittingInputFormat();
  }

  @Override
  public void setupJob(Job job, final Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    job.setInputFormatClass(IngestImageSplittingInputFormat.class);
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }

}
