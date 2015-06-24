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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapalgebra.RasterMapOp;
import org.mrgeo.mapalgebra.RenderedImageMapOp;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.OpImage;
import javax.media.jai.RenderedOp;

import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Creates a new image with the supplied image chain using Map Reduce.
 */
public class OpChainDriver
{

  public class OpChainContext
  {
    Set<String> _inputs = null;
    Bounds _bounds = null;
    String _output = null;
    int _zoom = -1;
    Configuration _userConf = null;
    int _tilesize = 0;
    double[] _defaults = null;
    Progress _progress = null;
    JobListener _jobListener = null;
    MrsImageOutputFormatProvider _ofProvider;
    AdHocDataProvider _statsProvider;
    String protectionLevel;
    Properties providerProperties;

    public OpChainContext(final Set<String> inputs, final Bounds bounds, final String output,
        final int zoom, final Configuration userConf,
        final int tilesize, final double[] defaults,
        final MrsImageOutputFormatProvider ofProvider,
        final AdHocDataProvider statsProvider,
        final Progress progress, final JobListener jobListener,
        final String protectionLevel,
        final Properties providerProperties)
    {
      _inputs = inputs;
      _bounds = bounds;
      _output = output;
      _zoom = zoom;
      _tilesize = tilesize;
      _userConf = userConf;
      _defaults = defaults;
      _progress = progress;
      _jobListener = jobListener;
      _ofProvider = ofProvider;
      _statsProvider = statsProvider;
      this.protectionLevel = protectionLevel;
      this.providerProperties = providerProperties;
    }
  }

  public static class OpChainException extends RuntimeException
  {

    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public OpChainException(final Exception e)
    {
      this.origException = e;
    }

    public OpChainException(final String msg)
    {
      final Exception e = new Exception(msg);
      this.origException = e;
    }

    @Override
    public void printStackTrace()
    {
      origException.printStackTrace();
    }
  }

  public static String CALCULATE_STATS = "calculateStats";

  static final Logger log = LoggerFactory.getLogger(OpChainDriver.class);

  protected Job job;

  boolean calculateStats = false;

  private double min, max;

  public static String buildTree(final RenderedImage ri, final int depth)
  {
    String result = "";

    result += nodeName(ri, depth);
    if (ri instanceof RenderedOp)
    {
      final RenderedOp rop = (RenderedOp) ri;
      final RenderedImage r = rop.getRendering();
      result += nodeName(r, depth + 1);
    }

    if (ri.getSources() != null)
    {
      for (final RenderedImage child : ri.getSources())
      {
        result += buildTree(child, depth + 1);
      }
    }

    return result;
  }


  private static String nodeName(final RenderedImage ri, final int depth)
  {
    String result = "";
    if (ri == null)
    {
      return "";
    }

    for (int i = 0; i < depth; i++)
    {
      result += "  ";
    }

    String type = "";
    if (ri instanceof RenderedOp)
    {
      if (type.length() > 0)
      {
        type += ", ";
      }
      type += "RenderedOp";
    }
    if (ri instanceof OpImage)
    {
      if (type.length() > 0)
      {
        type += ", ";
      }
      type += "OpImage";
    }

    if (type.length() == 0)
    {
      type += ri.getClass().getSimpleName();
    }

    // result += String.format("%s (%s) tile size: %d x %d\n", ri.toString(), type,
    // ri.getTileWidth(),
    // ri.getTileHeight());

    result += String.format("%s (%s) tile size: %d x %d\n", ri.getClass().getSimpleName(), type,
        ri.getTileWidth(),
        ri.getTileHeight());

    return result;
  }

  public boolean checkProgress(final OpChainContext context) throws JsonGenerationException,
      JsonMappingException,
      IOException, JobFailedException, JobCancelledException
  {
    final boolean result = MapReduceUtils.checkJobProgress(job, context._progress,
        context._jobListener);
    if (result && job.isComplete() && job.isSuccessful())
    {
      context._ofProvider.teardown(job);
      MrsImagePyramid.calculateMetadataWithProvider(context._output, context._zoom,
          context._ofProvider.getImageProvider(), context._statsProvider,
          context._defaults, context._bounds, context._userConf,
          context.protectionLevel, context.providerProperties);
      context._statsProvider.delete();
    }
    return result;

  }

  public double getMax()
  {
    return max;
  }

  public double getMin()
  {
    return min;
  }

  public void run(final RenderedImageMapOp rimop, final Set<String> inputs, final String output,
      final int zoom,
      final Bounds bounds, final Configuration userConf, final Progress progress,
      final String protectionLevel,
      final Properties providerProperties)
      throws IOException, JobFailedException, JobCancelledException
  {
    final RenderedImage rop = ((RasterMapOp)rimop).getRasterOutput();

    run(rop, inputs, output, zoom, bounds, userConf, progress, protectionLevel,
        providerProperties);
  }

  public void run(final RenderedImageMapOp rimop, final Set<String> inputs, final String output,
      final int zoom,
      final Configuration userConf, final Progress progress,
      final String protectionLevel,
      final Properties providerProperties) throws IOException,
      JobFailedException, JobCancelledException
  {
    run(rimop, inputs, output, zoom, Bounds.world, userConf, progress, protectionLevel,
        providerProperties);
  }

  public void run(final RenderedImage rop, final Set<String> inputs, final String output,
      final int zoom,
      final Bounds bounds, final Configuration userConf, final Progress progress,
      final String protectionLevel,
      final Properties providerProperties)
      throws IOException, JobFailedException, JobCancelledException
  {
    final OpChainContext context = _setUpJob(rop, inputs, output, zoom, bounds, userConf, progress,
        null, protectionLevel, providerProperties);
    // LongRectangle tileBounds = getTileBounds(bounds, zoom, context._tilesize);
    // Path splitFileTmp = TileIdPartitioner.setup(job,
    // new ImageSplitGenerator(tileBounds.getMinX(), tileBounds.getMinY(),
    // tileBounds.getMaxX(), tileBounds.getMaxY(),
    // zoom, 1000));
    MapReduceUtils.runJob(job, progress, null);

    context._ofProvider.teardown(job);
    MrsImagePyramid.calculateMetadataWithProvider(output, zoom,
        context._ofProvider.getImageProvider(), context._statsProvider,
        context._defaults, bounds, context._userConf, context.protectionLevel,
        context.providerProperties);
    context._statsProvider.delete();
  }

  public void run(final RenderedImage rop, final Set<String> inputs, final String output,
      final int zoom,
      final Configuration userConf, final Progress progress,
      final String protectionLevel,
      final Properties providerProperties) throws IOException,
      JobFailedException, JobCancelledException
  {
    run(rop, inputs, output, zoom, Bounds.world, userConf, progress, protectionLevel,
        providerProperties);
  }

  public OpChainContext runAsynchronously(final RenderedImage rop, final Set<String> inputs,
      final String output, final int zoom,
      final Bounds bounds, final Configuration userConf, final Progress progress,
      final JobListener jl, final String protectionLevel,
      final Properties providerProperties) throws IOException, JobFailedException,
      JobCancelledException
  {
    final OpChainContext context = _setUpJob(rop, inputs, output, zoom, bounds, userConf, progress,
        jl, protectionLevel, providerProperties);
    MapReduceUtils.runJobAsynchronously(job, jl);
    return context;
  }

  public void setCalculateStats(final boolean v)
  {
    calculateStats = v;
  }

  private OpChainContext _setUpJob(final RenderedImage rop, final Set<String> inputs,
      final String output, final int zoom,
      final Bounds bounds, final Configuration userConf, final Progress progress,
      final JobListener jl, final String protectionLevel,
      final Properties providerProperties)
      throws IOException
  {

    // if (bounds.equals(Bounds.world))
    // {
    // if (rop.getWidth() <= MAGIC_LOCAL_SIZE && rop.getHeight() <= MAGIC_LOCAL_SIZE)
    // {
    // if (progress != null)
    // {
    // progress.starting();
    // }
    // output.setData(rop, progress);
    //
    // if (calculateStats)
    // {
    // org.mrgeo.rasterops.MrsPyramid.Stats s = OpImageUtils.calculateStats(rop.getData());
    // min = s.min;
    // max = s.max;
    // }
    // if (progress != null)
    // {
    // progress.complete();
    // }
    // return;
    // }
    // }


    double noData = OpImageUtils.getNoData(rop,  Double.NaN);
    // Only single band output is supported at the moment.
    final double[] defaults = new double[] { noData };
    final int tilesize = getTileSize(inputs, providerProperties);

    final LongRectangle tileBounds = getTileBounds(bounds, zoom, tilesize);

    // if (tileBounds == null || defaults == null)
    // {
    // throw new OpChainException("Input pyramids missing tile bounds or default values");
    // }

    // create a new unique job testname
    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    
    final String jobName = "OpChain_" + now + "_" + UUID.randomUUID().toString();
    job = MapReduceUtils.createTiledJob(jobName, userConf);
    
    log.debug(jobName);
    final Configuration conf = job.getConfiguration();

    // String tree = buildTree(rop, 0);

    // create a corresponding tmp directory for the job
    // Path tmp = new Path(HadoopFileUtils.getTempDir(), job.getJobName());

    // Make sure that cropping is performed
//    conf.set(MrsPyramidInputFormatUtils.BOUNDS, bounds.toDelimitedString());

    // don't want to write to a single tile twice.
    // conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    conf.setInt(OpChainMapper.ZOOM_LEVEL, zoom);
    conf.setInt(OpChainMapper.TILE_SIZE, tilesize);

    HadoopUtils.setJar(job, this.getClass());

    // We need to know the default values (nodata) to calculate stats
    final MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setDefaultValues(defaults);
    HadoopUtils.setMetadata(job, metadata);

    job.setMapperClass(OpChainMapper.class);
    // Map only job, don't need a reducer...
    job.setReducerClass(OpChainReducer.class);

    // configure input and output
    if (inputs.size() > 0)
    {
      MrsImageDataProvider.setupMrsPyramidInputFormat(job, inputs, zoom, tilesize, bounds, providerProperties);
    }
    else
    {
      HadoopUtils.setupEmptyTileInputFormat(job, tileBounds, zoom, tilesize, 1,
          DataBuffer.TYPE_DOUBLE, defaults[0]);
    }

    SampleModel sm = rop.getSampleModel();
    MrsImageOutputFormatProvider ofProvider = MrsImageDataProvider.setupMrsPyramidOutputFormat(
        job, output, bounds, zoom, tilesize, sm.getDataType(), sm.getNumBands(),
        protectionLevel, providerProperties);

    // the keys are the string version of the ImageStats enum
    job.setOutputKeyClass(TileIdWritable.class);

    // the values are the values associated with the stat
    job.setOutputValueClass(RasterWritable.class);

    // set the op chain
    OpChainMapper.setOperation(conf, rop);

    // normally, this is taken care of by HadoopUtils.setupMrsPyramidOutputFormat, but since
    // in this case of hdfs_old, the "output" of the job is different from the output image
    // output of the job is the above MapOut.seq
    // MrsPyramidInputFormatUtils.setOutput(job, output);

    // conf.set(CALCULATE_STATS, String.valueOf(calculateStats));
    //
    // if (calculateStats == false)
    // {
    // job.setNumReduceTasks(0);
    // }
    // else
    // {
    // job.setNumReduceTasks(1);
    // }
//    final FileSystem fs = HadoopFileUtils.getFileSystem(conf);
//    final long blockSizeBytes = fs.getDefaultBlockSize();
//    final SampleModel sm = rop.getSampleModel();
//    final int elementSize = RasterUtils.getElementSize(sm.getDataType());
//    final int tileSizeBytes = sm.getWidth() * sm.getHeight() * sm.getNumBands() * elementSize;
//    final Path splitFileTmp = TileIdPartitioner.setup(job,
//        new ImageSplitGenerator(tileBounds,
//            zoom, tileSizeBytes, blockSizeBytes));

    final AdHocDataProvider statsProvider = DataProviderFactory.createAdHocDataProvider(
        providerProperties);
    statsProvider.setupJob(job);
    conf.set(OpChainReducer.STATS_PROVIDER, statsProvider.getResourceName());
    final OpChainContext ocContext = new OpChainContext(inputs, bounds, output, zoom, conf,
        tilesize, defaults, ofProvider, statsProvider, progress, null,
        protectionLevel, providerProperties);

//    conf.setInt("io.sort.mb", 400);
//    conf.set("mapred.child.java.opts", "-Xmx3500m");
    conf.setInt("mapred.merge.recordsBeforeProgress", 100);
    conf.setInt("mapred.combine.recordsBeforeProgress", 100);
    return ocContext;

    // if (calculateStats == true)
    // {
    // readStats(mapOutput);
    // }
  }

  // private void readStats(Path mapOutput) throws IOException
  // {
  // if (mapOutput == null)
  // {
  // log.info("mapOutput is null.");
  // }
  // log.info(mapOutput.toString());
  // FileSystem fs = FileSystemCache.getInstance().openFileSystem();
  // FileStatus[] status = fs.listStatus(mapOutput);
  // for (FileStatus s : status)
  // {
  // if (s.getPath().getName().startsWith("part-") && s.getLen() > 0)
  // {
  // InputStream fdis;
  // if (s.getPath().getName().endsWith(".deflate"))
  // {
  // fdis = new InflaterInputStream(fs.open(s.getPath()));
  // }
  // else
  // {
  // fdis = fs.open(s.getPath());
  // }
  //
  // BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
  // while (reader.ready())
  // {
  // String line = reader.readLine();
  // String[] split = line.split("\t");
  // if (split[0].equals(tileImagingErrorKey))
  // {
  // log.error("Imaging error processing: {}", split[1]);
  // }
  // else
  // {
  // ImageStats stat = ImageStats.valueOf(split[0]);
  // switch (stat)
  // {
  // case MAX:
  // max = Double.valueOf(split[1]);
  // break;
  // case MIN:
  // min = Double.valueOf(split[1]);
  // break;
  // }
  // }
  // }
  // }
  // }
  // FileSystemCache.getInstance().freeFileSystem(fs);
  // }

  private static LongRectangle getTileBounds(final Bounds bounds, final int zoom, final int tilesize)
  {
    final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(new TMSUtils.Bounds(bounds.getMinX(),
        bounds.getMinY(),
        bounds.getMaxX(), bounds.getMaxY()), zoom, tilesize);
    final LongRectangle tileBounds = new LongRectangle(tb.w, tb.s, tb.e, tb.n);
    return tileBounds;
  }

  private static int getTileSize(final Set<String> inputs,
      final Properties providerProperties) throws IOException
  {
    int tilesize = 0;
    for (final String input : inputs)
    {
      final MrsImagePyramid pyramid = MrsImagePyramid.open(input, providerProperties);
      final MrsImagePyramidMetadata metadata = pyramid.getMetadata();

      if (tilesize == 0)
      {
        tilesize = metadata.getTilesize();
      }
    }
    if (tilesize == 0)
    {
      tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE,
          MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT));
    }
    return tilesize;
  }
}
