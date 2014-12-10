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

package org.mrgeo.mapreduce.ingestvector;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.mapreduce.MapGeometryToTiles;
import org.mrgeo.mapreduce.MapReduceUtils;
import org.mrgeo.vector.mrsvector.MrsVectorPyramid;
import org.mrgeo.vector.mrsvector.VectorTileWritable;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.utils.geotools.GeotoolsVectorUtils;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.partitioners.ImageSplitGenerator;
import org.mrgeo.hdfs.partitioners.TileIdPartitioner;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class IngestVectorDriver
{
  private static Logger log = LoggerFactory.getLogger(IngestVectorDriver.class);

  static final private String base = IngestVectorDriver.class.getSimpleName();
  static final public String MAX_ZOOM = base + ".max.zoom";
  static final public String TILESIZE = base + ".tilesize";
  static final public String BOUNDS = base + ".bounds";

  static final int zoomMax = 20;
  static final int pointsPerTile = 100000;
  

  private IngestVectorDriver()
  {
  }

  public static boolean ingest(final String[] inputs, final String output,
    final Configuration config, final Properties providerProperties) throws Exception
    {
    return ingest(inputs, output, config, 0, providerProperties);
    }

  public static boolean ingest(final String[] inputs, final String output,
    final Configuration config, int zoomlevel, Properties providerProperties) throws Exception
    {
    if (zoomlevel <= 0)
    {
      zoomlevel = calculateZoomlevel(inputs, config);
    }
    
    return runJob(inputs, output, config, zoomlevel, providerProperties);
    }

  private static boolean runJob(String[] inputs, String output, Configuration config,
      int zoomlevel, Properties providerProperties) throws IOException
  {
    
    Bounds bounds = GeotoolsVectorUtils.calculateBounds(inputs, config);
    
    final Job job = new Job(config);

    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    final String jobName = "IngestVector_" + now + "_" + UUID.randomUUID().toString();
    job.setJobName(jobName);

    final Configuration conf = job.getConfiguration();

    int tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty(
      "mrsimage.tilesize", "512"));

    conf.setInt(MapGeometryToTiles.ZOOMLEVEL, zoomlevel);
    conf.setInt(MapGeometryToTiles.TILESIZE, tilesize);

    job.setInputFormatClass(IngestVectorGeometryInputFormat.class);

    job.setMapperClass(IngestGeometryMapper.class);
    job.setMapOutputKeyClass(TileIdWritable.class);
    job.setMapOutputValueClass(GeometryWritable.class);

    HadoopUtils.setJar(job, IngestVectorDriver.class);
    for (final String input : inputs)
    {
      // Source file can be on the local file system (instead of hdfs), and
      // we call the following to circumvent a bug in Hadoop 20.2
      // FileInputFormat.addInputPath.
      HadoopVectorUtils.addInputPath(job, new Path(input));
    }
    HadoopFileUtils.delete(output);

    MrsImageOutputFormatProvider ofProvider = MrsImageDataProvider.setupMrsPyramidOutputFormat(
        job, output, bounds, zoomlevel, tilesize, providerProperties);

    job.setReducerClass(IngestGeometryReducer.class);

    job.setOutputKeyClass(TileIdWritable.class);
    job.setOutputValueClass(VectorTileWritable.class);

    try
    {
      job.submit();
      final boolean success = job.waitForCompletion(true);

      if (success)
      {
        ofProvider.teardown(job);
        MrsVectorPyramid.calculateMetadata(output, zoomlevel, tilesize, bounds);
        return true;
      }

    }
    catch (final InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (final ClassNotFoundException e)
    {
      e.printStackTrace();
    }

    return false;
  }


  public static boolean localIngest(final String[] inputs, final String output,
    final Configuration config, final int zoomlevel) throws Exception
    {
    throw new NotImplementedException("localIngest not implemented!");
    }

  public static int readZoomlevel(final String output, final Configuration conf)
  {
    try
    {
      final int[] counts = new int[zoomMax + 1];
      Arrays.fill(counts, 0);

      final Path path = new Path(output);
      final FileSystem fs = path.getFileSystem(conf);

      final FileStatus[] outputFiles = fs.listStatus(path);

      for (final FileStatus fileStatus : outputFiles)
      {
        if (fileStatus.isDir() == false)
        {
          final Path p = fileStatus.getPath();
          final String name = p.getName();
          if (name.startsWith("part-"))
          {
            BufferedReader r = null;
            InputStream fdis = null;
            try
            {
              fdis = HadoopFileUtils.open(conf, p); // fs.open(p);
              r = new BufferedReader(new InputStreamReader(fdis));
              String line = r.readLine();
              while (line != null)
              {
                final String fields[] = line.split("\t");

                final int zoom = Integer.valueOf(fields[0]);
                counts[zoom] += Integer.valueOf(fields[1]);

                line = r.readLine();
              }
            }
            finally
            {
              if (fdis != null)
              {
                fdis.close();
              }
            }
          }
        }
      }

      for (int i = 1; i < counts.length; i++)
      {
        if (counts[i] < pointsPerTile)
        {
          return i;
        }
      }
      return counts.length - 1;
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }

    return -1;
  }

  private static int calculateZoomlevel(final String[] inputs, final Configuration config)
      throws IOException
      {
    log.info("Calculating zoom level");

    final Job job = new Job(config);
    HadoopUtils.setJar(job, IngestVectorDriver.class);

    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    final String jobName = "CalculateVectorZoom_" + now + "_" + UUID.randomUUID().toString();
    job.setJobName(jobName);

    final Configuration conf = job.getConfiguration();

    conf.setInt(MAX_ZOOM, zoomMax);
    conf.setInt(TILESIZE, Integer.parseInt(MrGeoProperties.getInstance().getProperty(
      "mrsimage.tilesize", "512")));

    job.setInputFormatClass(IngestVectorGeometryInputFormat.class);
    job.setMapperClass(CalculateZoomMapper.class);
    job.setMapOutputKeyClass(TileIdZoomWritable.class);
    job.setMapOutputValueClass(LongWritable.class);


    for (final String input : inputs)
    {
      // Source file can be on the local file system (instead of hdfs), and
      // we call the following to circumvent a bug in Hadoop 20.2
      // FileInputFormat.addInputPath.
      HadoopVectorUtils.addInputPath(job, new Path(input));
    }

    job.setReducerClass(CalculateZoomReducer.class);
    String output = new Path(HadoopFileUtils.getTempDir(),HadoopUtils.createRandomString(40)).toString();
    try
    {
      FileOutputFormat.setOutputPath(job, new Path(output));

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(LongWritable.class);

      try
      {
        job.submit();
        final boolean success = job.waitForCompletion(true);

        if (success)
        {
          return readZoomlevel(output, conf);
        }

      }
      catch (final InterruptedException e)
      {
        e.printStackTrace();
      }
      catch (final ClassNotFoundException e)
      {
        e.printStackTrace();
      }
    }
    finally
    {
      HadoopFileUtils.delete(output);
    }

    return 0;
      }
  
  private static Path setupPartitioner(Job job, Bounds bounds, int zoomlevel, int tilesize) throws IOException
  {
    MapReduceUtils.setupTiledJob(job);

    Configuration conf = job.getConfiguration();
    
    // Set up partitioner
    TMSUtils.TileBounds tb = TMSUtils.boundsToTileExact(bounds.getTMSBounds(), zoomlevel, tilesize);
    
    final LongRectangle tileBounds = new LongRectangle(tb.w, tb.s, tb.e, tb.n);
    
//    final int tileSizeBytes = tilesize * tilesize *
//        metadata.getBands() * RasterUtils.getElementSize(metadata.getTileType());

    int increment = conf.getInt(TileIdPartitioner.INCREMENT_KEY, -1);
    Path splitFileTmp;
    if(increment == -1) 
    {
      increment = 1;
    }
    
      // if increment is provided, use it to setup the partitioner
      splitFileTmp = TileIdPartitioner.setup(job, 
        new ImageSplitGenerator(tileBounds.getMinX(), tileBounds.getMinY(),
          tileBounds.getMaxX(), tileBounds.getMaxY(), 
          zoomlevel, increment));

//    } 
//    else 
//    {
//      FileSystem fs = HadoopFileUtils.getFileSystem(conf);
//      
//      // if increment is not provided, set up the partitioner using max partitions 
//      String strMaxPartitions = conf.get(TileIdPartitioner.MAX_PARTITIONS_KEY);
//      if (strMaxPartitions != null)
//      {
//        // We know the max partitions conf setting exists, let's go read it. The
//        // 1000 hard-coded default value is never used.
//        int maxPartitions = conf.getInt(TileIdPartitioner.MAX_PARTITIONS_KEY, 1000);
//        splitFileTmp = TileIdPartitioner.setup(job,
//          new ImageSplitGenerator(tileBounds, zoomlevel,
//            tileSizeBytes, fs.getDefaultBlockSize(), maxPartitions));
//      }
//      else
//      {
//        splitFileTmp = TileIdPartitioner.setup(job,
//          new ImageSplitGenerator(tileBounds, zoomlevel,
//            tileSizeBytes, fs.getDefaultBlockSize()));
//      }
//    }

      return splitFileTmp;
  }
}

