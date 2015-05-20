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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ingest.ImageIngestTiledInputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;
import java.util.Properties;

public class HdfsImageIngestTiledInputFormatProvider extends ImageIngestTiledInputFormatProvider
{
  private String input = null;

  @Override
  public InputFormat<TileIdWritable, RasterWritable> getInputFormat(final String input)
  {
    this.input = input;
    return new SequenceFileInputFormat<TileIdWritable, RasterWritable>();
  }

  @Override
  public void setupJob(Job job,
      final Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);

    try
    {
      // doing a set because we can only have 1 input
      SequenceFileInputFormat.setInputPaths(job, new Path(input));
      //FileInputFormat.setInputPaths(job, new Path(input));
    }
    catch (IOException e)
    {
      throw new DataProviderException("Error setting up job", e);
    }
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }

}
