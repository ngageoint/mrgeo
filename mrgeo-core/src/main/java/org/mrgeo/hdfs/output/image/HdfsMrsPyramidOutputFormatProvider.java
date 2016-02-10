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
import org.mrgeo.data.image.ImageOutputFormatContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.partitioners.*;
import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;

public class HdfsMrsPyramidOutputFormatProvider extends MrsImageOutputFormatProvider
{
private enum PartitionType {ROW, BLOCKSIZE}

final HdfsMrsImageDataProvider provider;

PartitionType partitioner;


public HdfsMrsPyramidOutputFormatProvider(final HdfsMrsImageDataProvider provider,
                                          final ImageOutputFormatContext context)
{
  super(context);

  this.provider = provider;

  // TODO:  Get this from mrgeo.conf?
  partitioner = PartitionType.BLOCKSIZE;
}

@Override
public OutputFormat<WritableComparable<?>, Writable> getOutputFormat()
{
  return new HdfsMrsPyramidOutputFormat();
}


@Override
public Configuration setupOutput(Configuration conf) throws DataProviderException
{
  try
  {
    // make sure the directory is empty
    final String outputWithZoom = provider.getResolvedResourceName(false) + "/" + context.getZoomlevel();

    final Path outputPath = new Path(outputWithZoom);
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, outputPath);
    if (fs.exists(outputPath))
    {
      fs.delete(outputPath, true);
    }

    HdfsMrsPyramidOutputFormat.setOutputInfo(conf, null, outputWithZoom);
    return new Job(super.setupOutput(conf)).getConfiguration();

  }
  catch (final IOException e)
  {
    throw new DataProviderException("Error running spark job setup", e);
  }
}

@Override
public void teardown(final Configuration conf) throws DataProviderException
{
  try
  {
    String imagePath = provider.getResolvedResourceName(true);
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
public void teardownForSpark(final Configuration conf) throws DataProviderException
{
  // nothing to do
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

@Override
public boolean validateProtectionLevel(String protectionLevel)
{
  return true;
}

@Override
public FileSplitPartitioner getSparkPartitioner()
{
  switch (partitioner)
  {
  case BLOCKSIZE:
    return new BlockSizePartitioner(context);
  case ROW:
  default:
    return new RowPartitioner(context.getBounds(), context.getZoomlevel(), context.getTilesize());
  }
}


}
