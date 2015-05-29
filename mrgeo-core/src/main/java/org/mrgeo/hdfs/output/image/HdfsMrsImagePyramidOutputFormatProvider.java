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

package org.mrgeo.hdfs.output.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataWriter;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledOutputFormatContext;
import org.mrgeo.hadoop.multipleoutputs.DirectoryMultipleOutputs;
import org.mrgeo.hadoop.multipleoutputs.MultipleOutputFormat;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.partitioners.ImageSplitGenerator;
import org.mrgeo.hdfs.partitioners.TileIdPartitioner;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;
import java.util.List;

public class HdfsMrsImagePyramidOutputFormatProvider extends MrsImageOutputFormatProvider
{
  final HdfsMrsImageDataProvider provider;

  Path splitFileTmp = null;

  public HdfsMrsImagePyramidOutputFormatProvider(final HdfsMrsImageDataProvider provider,
    final TiledOutputFormatContext context)
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
      
      job.setOutputKeyClass(TileIdWritable.class);
      job.setOutputValueClass(RasterWritable.class);

      // make sure the directory is empty

//      final String outputWithZoom = context.getOutput() + "/" + context.getZoomlevel();
      final String outputWithZoom = provider.getResolvedResourceName(false) + "/" + context.getZoomlevel();

      final Path outputPath = new Path(outputWithZoom);
      final FileSystem fs = HadoopFileUtils.getFileSystem(job.getConfiguration(), outputPath);
      if (fs.exists(outputPath))
      {
        fs.delete(outputPath, true);
      }

      if (context.isMultipleOutputFormat())
      {
        setupMultipleOutputs(job, outputPath);
      }
      else
      {
        setupSingleOutput(job, outputWithZoom);
      }
    }
    catch (final IOException e)
    {
      throw new DataProviderException("Error running job setup", e);
    }

  }

  @Override
  public void teardown(final Job job) throws DataProviderException
  {
    try
    {
      Configuration conf = job.getConfiguration();
      String imagePath = provider.getResolvedResourceName(true);
      final String outputWithZoom = imagePath + "/" + context.getZoomlevel();
      if (splitFileTmp != null)
      {

        final SplitFile sf = new SplitFile(conf);
        sf.copySplitFile(splitFileTmp.toString(), outputWithZoom);
      }

      List<String> dirs = DirectoryMultipleOutputs.getNamedOutputsList(conf);
      for (String dir : dirs)
      {
        HadoopFileUtils.cleanDirectory(conf, dir, false);
      }
      HadoopFileUtils.cleanDirectory(job.getConfiguration(), outputWithZoom, false);
    }
    catch (final IOException e)
    {
      throw new DataProviderException("Error in teardown", e);
    }

  }

  @Override
  public MrsImagePyramidMetadataWriter getMetadataWriter()
  {
    return provider.getMetadataWriter();
  }

  private void setupMultipleOutputs(final Job job, final Path outputPath) throws IOException
  {
    @SuppressWarnings("rawtypes")
    final Class<? extends FileOutputFormat> clazz = HdfsMrsPyramidOutputFormat.class;
    HdfsMrsPyramidOutputFormat.setInfo(job);
    job.setOutputFormatClass(MultipleOutputFormat.class);
    MultipleOutputFormat.setRootOutputFormat(job, clazz);

    DirectoryMultipleOutputs.addNamedOutput(job, context.getName(), outputPath, clazz,
      TileIdWritable.class, RasterWritable.class);

    final Configuration conf = job.getConfiguration();

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
        tileBounds.getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY(), zoom, increment), zoom);
    }
    else if (!context.isCalculatePartitions())
    {
      // can't calculate partitions on size, just use increment of 1 (1 row per partition)
      splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds.getMinX(),
        tileBounds.getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY(), zoom, 1), zoom);
    }
    else
    {
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf, outputPath);

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
          tileSizeBytes, fs.getDefaultBlockSize(), maxPartitions), zoom);
      }
      else
      {
        splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds, zoom,
          tileSizeBytes, fs.getDefaultBlockSize()), zoom);
      }
    }
  }

  private void setupSingleOutput(final Job job, final String outputWithZoom) throws IOException
  {
    job.setOutputFormatClass(HdfsMrsPyramidOutputFormat.class);
    HdfsMrsPyramidOutputFormat.setOutputInfo(job, outputWithZoom);

    final Configuration conf = job.getConfiguration();

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
        tileBounds.getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY(), zoom, increment));
    }
    else if (!context.isCalculatePartitions())
    {
      // can't calculate partitions on size, just use increment of 1 (1 row per partition)
      splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds.getMinX(),
        tileBounds.getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY(), zoom, 1));
    }
    else
    {
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf);

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
          tileSizeBytes, fs.getDefaultBlockSize(), maxPartitions));
      }
      else
      {
        splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds, zoom,
          tileSizeBytes, fs.getDefaultBlockSize()));
      }
    }
  }

  @Override
  public boolean validateProtectionLevel(String protectionLevel)
  {
    return true;
  }
}
