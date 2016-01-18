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

package org.mrgeo.hdfs.output.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.image.MrsPyramidMetadataWriter;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.image.ImageOutputFormatContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.partitioners.HdfsSparkTileIdPartitioner;
import org.mrgeo.hdfs.partitioners.ImageSplitGenerator;
import org.mrgeo.hdfs.partitioners.SparkTileIdPartitioner;
import org.mrgeo.hdfs.partitioners.TileIdPartitioner;
import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;

public class HdfsMrsPyramidOutputFormatProvider extends MrsImageOutputFormatProvider
{
final HdfsMrsImageDataProvider provider;

Path splitFileTmp = null;

public HdfsMrsPyramidOutputFormatProvider(final HdfsMrsImageDataProvider provider,
                                          final ImageOutputFormatContext context)
{
  super(context);

  this.provider = provider;
}

@Override
public OutputFormat<WritableComparable<?>, Writable> getOutputFormat()
{
  return new HdfsMrsPyramidOutputFormat();
}

@Override
public void setupJob(final Job job) throws DataProviderException
{
  try
  {
    super.setupJob(job);
    setupConfig(job);
    setup(job.getConfiguration(), job);
  }
  catch (final IOException e)
  {
    throw new DataProviderException("Error running job setup", e);
  }
}

@Override
public Configuration setupSparkJob(Configuration conf) throws DataProviderException
{
  try
  {
    Configuration conf1 = super.setupSparkJob(conf);
    Job job = new Job(conf1);
    setupConfig(job);
    Configuration conf2 = job.getConfiguration();
    // Do not pass the job into the setup method. That will configure things like
    // a partitioner which is not needed/wanted for a spark job.
    setup(conf2, null);
    return conf2;
  }
  catch (final IOException e)
  {
    throw new DataProviderException("Error running spark job setup", e);
  }
}

private void setupConfig(final Job job) throws DataProviderException
{
  job.setOutputKeyClass(TileIdWritable.class);
  job.setOutputValueClass(RasterWritable.class);
}

private void setup(final Configuration conf, Job job) throws IOException
{
  // make sure the directory is empty

  final String outputWithZoom = provider.getResolvedResourceName(false) + "/" + context.getZoomlevel();

  final Path outputPath = new Path(outputWithZoom);
  final FileSystem fs = HadoopFileUtils.getFileSystem(conf, outputPath);
  if (fs.exists(outputPath))
  {
    fs.delete(outputPath, true);
  }

  setupSingleOutput(conf, job, outputWithZoom);
}

@Override
public void teardown(final Job job) throws DataProviderException
{
  performTeardown(job.getConfiguration());
}

@Override
public void teardownForSpark(final Configuration conf) throws DataProviderException
{
  // nothing to do
}

private void performTeardown(final Configuration conf) throws DataProviderException
{
  try
  {
    String imagePath = provider.getResolvedResourceName(true);
    if (splitFileTmp != null)
    {
      HadoopFileUtils.delete(splitFileTmp);
    }

    final Path outputWithZoom = new Path(imagePath + "/" + context.getZoomlevel());

    FileSplit split = new FileSplit();
    split.generateSplits(outputWithZoom, conf);

    split.writeSplits(outputWithZoom);
  }
  catch (final IOException e)
  {
    throw new DataProviderException("Error in teardown", e);
  }
}

@Override
public MrsPyramidMetadataWriter getMetadataWriter()
{
  return provider.getMetadataWriter();
}

@Override
public MrsImageDataProvider getImageProvider()
{
  return provider;
}

private void setupSingleOutput(final Configuration conf, final Job job, final String outputWithZoom) throws IOException
{
  if (job != null)
  {
    job.setOutputFormatClass(HdfsMrsPyramidOutputFormat.class);
  }

  HdfsMrsPyramidOutputFormat.setOutputInfo(conf, job, outputWithZoom);

  if (job != null)
  {
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf);

    // Set up partitioner
    final int tilesize = context.getTilesize();
    final int zoom = context.getZoomlevel();
    final Bounds bounds = context.getBounds();

    final LongRectangle tileBounds = TMSUtils.boundsToTile(bounds.getTMSBounds(), zoom, tilesize)
            .toLongRectangle();

    final int increment = conf.getInt(TileIdPartitioner.INCREMENT_KEY, -1);
    if (increment != -1)
    {
      // if increment is provided, use it to setup the partitioner
      splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds.getMinX(),
                                                                          tileBounds.getMinY(), tileBounds.getMaxX(),
                                                                          tileBounds.getMaxY(), zoom, increment));
    }
    else if (!context.isCalculatePartitions())
    {
      // can't calculate partitions on size, just use increment of 1 (1 row per partition)
      splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds.getMinX(),
                                                                          tileBounds.getMinY(), tileBounds.getMaxX(),
                                                                          tileBounds.getMaxY(), zoom, 1));
    }
    else
    {

      final int bands = context.getBands();
      final int tiletype = context.getTiletype();

      final int tileSizeBytes = tilesize * tilesize * bands * RasterUtils.getElementSize(tiletype);

      // if increment is not provided, set up the partitioner using max partitions
      final String strMaxPartitions = conf.get(TileIdPartitioner.MAX_PARTITIONS_KEY);
      if (strMaxPartitions != null)
      {
        // We know the max partitions conf setting exists, let's go read it. The
        // 1000 hard-coded default value is never used.
        final int maxPartitions = conf.getInt(TileIdPartitioner.MAX_PARTITIONS_KEY, 1000);
        splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds, zoom,
                                                                            tileSizeBytes,
                                                                            fs.getDefaultBlockSize(new Path("/")),
                                                                            maxPartitions));
      }
      else
      {
        splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds, zoom,
                                                                            tileSizeBytes,
                                                                            fs.getDefaultBlockSize(new Path("/"))));
      }
    }
  }
}

@Override
public boolean validateProtectionLevel(String protectionLevel)
{
  return true;
}

@Override
public SparkTileIdPartitioner getPartitionerForSpark(TMSUtils.TileBounds tileBounds, int zoom)
{
  int tileIncrement = 1;
  ImageSplitGenerator splitGenerator = new ImageSplitGenerator(tileBounds.w, tileBounds.s,
                                               tileBounds.e, tileBounds.n, zoom, tileIncrement);
  return new HdfsSparkTileIdPartitioner(splitGenerator);
}
}
