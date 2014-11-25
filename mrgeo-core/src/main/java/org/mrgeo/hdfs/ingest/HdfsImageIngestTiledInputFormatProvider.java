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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ingest.ImageIngestTiledInputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;

public class HdfsImageIngestTiledInputFormatProvider extends ImageIngestTiledInputFormatProvider
{
  Path splitFile = null;

  @Override
  public InputFormat<TileIdWritable, RasterWritable> getInputFormat(final String input)
  {
    return new SequenceFileInputFormat<TileIdWritable, RasterWritable>();
  }

  @Override
  public void setupJob(Job job,
      final Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
//    Configuration conf = job.getConfiguration();
//
//    String outputWithZoom = output;
//
//
//    outputWithZoom += "/" + metadata.getMaxZoomLevel();
//
//    final FileSystem fs = HadoopFileUtils.getFileSystem(conf);
//
//    HadoopFileUtils.delete(output);
//    final Path outputPath = new Path(output);
//    if (fs.exists(outputPath))
//    {
//      fs.delete(outputPath, true);
//    }
//
//    org.mrgeo.utils.HadoopUtils.setupMrsPyramidOutputFormat(job, outputWithZoom);
//    job.setOutputKeyClass(TileIdWritable.class);
//    job.setOutputValueClass(RasterWritable.class);
//
//    // Set up partitioner
//    final LongRectangle tileBounds = metadata.getTileBounds(metadata.getMaxZoomLevel());
//    final int tileSizeBytes = metadata.getTilesize() * metadata.getTilesize() *
//        metadata.getBands() * RasterUtils.getElementSize(metadata.getTileType());
//
//    int increment = conf.getInt(TileIdPartitioner.INCREMENT_KEY, -1);
//    if(increment != -1) 
//    {
//      // if increment is provided, use it to setup the partitioner
//      splitFile = TileIdPartitioner.setup(job, 
//        new ImageSplitGenerator(tileBounds.getMinX(), tileBounds.getMinY(), 
//          tileBounds.getMaxX(), tileBounds.getMaxY(), 
//          metadata.getMaxZoomLevel(), increment));
//
//    } 
//    else 
//    {
//      // if increment is not provided, set up the partitioner using max partitions 
//      String strMaxPartitions = conf.get(TileIdPartitioner.MAX_PARTITIONS_KEY);
//      if (strMaxPartitions != null)
//      {
//        // We know the max partitions conf setting exists, let's go read it. The
//        // 1000 hard-coded default value is never used.
//        int maxPartitions = conf.getInt(TileIdPartitioner.MAX_PARTITIONS_KEY, 1000);
//        splitFile = TileIdPartitioner.setup(job,
//          new ImageSplitGenerator(tileBounds, metadata.getMaxZoomLevel(),
//            tileSizeBytes, fs.getDefaultBlockSize(), maxPartitions));
//      }
//      else
//      {
//        splitFile = TileIdPartitioner.setup(job,
//          new ImageSplitGenerator(tileBounds, metadata.getMaxZoomLevel(),
//            tileSizeBytes, fs.getDefaultBlockSize()));
//      }
//    }
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }

}
