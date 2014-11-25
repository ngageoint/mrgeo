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

package org.mrgeo.hdfs.ingest.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class IngestImageSplittingInputFormat extends
FileInputFormat<TileIdWritable, RasterWritable>
{
  private static Logger log = LoggerFactory.getLogger(IngestImageSplittingInputFormat.class);


  @Override
  public RecordReader<TileIdWritable, RasterWritable> createRecordReader(final InputSplit split,
      final TaskAttemptContext context) throws IOException, InterruptedException
      {
    final RecordReader<TileIdWritable, RasterWritable> reader = new IngestImageSplittingRecordReader();
    //FIXME: This seems to be called from AutoFeatureInputFormat.initialize()
    reader.initialize(split, context);

    return reader;
      }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException
  {
    final List<InputSplit> splits = new LinkedList<InputSplit>();
    // mapred.input.dir
    final Path[] inputs = FileInputFormat.getInputPaths(context);

    final Configuration conf = context.getConfiguration();

    int tilesize = -1;
    try
    {
      //metadata = HadoopUtils.getMetadata(conf);
      Map<String, MrsImagePyramidMetadata> meta = HadoopUtils.getMetadata(context.getConfiguration());
      if (!meta.isEmpty())
      {
        MrsImagePyramidMetadata metadata =  meta.values().iterator().next();
        tilesize = metadata.getTilesize();
      }
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (tilesize < 0)
    {
      tilesize = conf.getInt("tilesize", -1);
      if (tilesize < 1)
      {
        throw new MrsImageException(
            "Error, no \"tilesize\" or \"metadata\" parameter in configuration, tilesize needs to be calculated & set before map/reduce");
      }

    }
    
    final int zoomlevel = conf.getInt("zoomlevel", -1);

    // get the tilesize in bytes (default to 3 band, 1 byte per band)
    final long tilebytes = conf.getLong("tilebytes", tilesize * tilesize * 3 * 1);

    if (zoomlevel < 1)
    {
      throw new MrsImageException(
          "Error, no \"zoomlevel\" parameter in configuration, zoomlevel needs to be calculated & set before map/reduce");
    }

    // get the spill buffer percent, then take 95% of it for extra padding...
    double spillpct = conf.getFloat("io.sort.spill.percent", (float)0.8) * 0.95;
    long spillsize = (long) (conf.getFloat("io.sort.mb", 200) * spillpct) * 1024 * 1024;
    log.info("Spill size for splitting is: " + spillsize + "b");

    //log.info("Creating splits for: " + output.toString());
    for (final Path input : inputs)
    {
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf, input);

      log.info("  reading: " + input.toString());
      log.info("    zoomlevel: " + zoomlevel);

      final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(input.toString());

      if (reader != null)
      {
        try
        {
          final LongRectangle bounds = GeotoolsRasterUtils.calculateTiles(reader, tilesize, zoomlevel);
          
          final long minTx = bounds.getMinX();
          final long maxTx = bounds.getMaxX();
          final long minTy = bounds.getMinY();
          final long maxTy = bounds.getMaxY();

          final long width = bounds.getWidth();
          final long height = bounds.getHeight();
          
          final long totaltiles = width * height;
          
          final FileStatus status = fs.getFileStatus(input);

          // for now, we'll just use the 1st block location for the split.
          // we can get more sophisticated later...
          final BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, 0);

          String location = null;
          if (blocks.length > 0)
          {
            final String hosts[] = blocks[0].getHosts();
            if (hosts.length > 0)
            {
              location = hosts[0];
            }
          }

          // long filelen = status.getLen();
          final long totalbytes = totaltiles * tilebytes;

          // if uncompressed tile sizes are greater than the spillsize, break it
          // into pieces
          if (totalbytes > spillsize)
          {
            final long numsplits = (totalbytes / spillsize) + 1;

            final long splitrange = (totaltiles / numsplits);
            long leftovers = totaltiles - (numsplits * splitrange);

            long start = 0;
            long end = 0;

            for (int i = 0; i < numsplits; i++)
            {
              end = start + splitrange;
              if (leftovers > 0)
              {
                end++;
                leftovers--;
              }

              final long sy = (start / width);
              final long sx = (start - (sy * width));

              // since the tile range is inclusive, calculate with end-1
              final long ey = ((end - 1) / width);
              final long ex = ((end - 1) - (ey * width));

              // System.out.println("start: " + start + " end: " + end);
              // System.out.println("  sx: " + sx + " sy: " + sy);
              // System.out.println("  ex: " + ex + " ey: " + ey);
              splits.add(new IngestImageSplit(input.toString(), minTx + sx, minTx + ex, minTy + sy,
                  minTy + ey, (end - start), bounds, zoomlevel, tilesize, location));

              start = end;
            }
          }
          else
          {
            splits.add(new IngestImageSplit(input.toString(), minTx, maxTx, minTy, maxTy,
                (maxTx + 1 - minTx) * (maxTy + 1 - minTy), bounds, zoomlevel, tilesize, location));
          }
        }
        finally
        {
          try
          {
            GeotoolsRasterUtils.closeStreamFromReader(reader);
          }
          catch (Exception e)
          {
            e.printStackTrace();
            throw new IOException(e);
          }
        }
      }

    }

  return splits;
}

@Override
protected boolean isSplitable(final JobContext context, final Path filename)
{
  return false;
}

}
