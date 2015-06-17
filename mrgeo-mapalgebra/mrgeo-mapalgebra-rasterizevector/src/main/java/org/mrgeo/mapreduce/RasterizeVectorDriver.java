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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormat;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 
 * @author jason.surratt
 * 
 */
public class RasterizeVectorDriver
{
  public static final String STATS_PROVIDER = "stats.provider";
  public static final String BOUNDS_PROVIDER = "bounds.provider";

  public static class PaintReduce extends
      Reducer<TileIdWritable, GeometryWritable, TileIdWritable, RasterWritable>
  {
    private RasterizeVectorPainter rvp = new RasterizeVectorPainter();
    private Counter totalTiles = null;
    private Counter features = null;

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException
    {
      final Configuration conf = context.getConfiguration();
      Bounds inputBounds = rvp.getInputBounds();
      if (!conf.getBoolean(RasterizeVectorDriver.TEST_REDUCER, false) && inputBounds != null)
      {
        String adhoc = context.getConfiguration().get(BOUNDS_PROVIDER, null);
        if (adhoc == null)
        {
          throw new IOException("Stats provider not set");
          
        }
        AdHocDataProvider boundsProvider = DataProviderFactory.getAdHocDataProvider(adhoc,
            AccessMode.WRITE, conf);
        MapReduceUtils.writeBounds(boundsProvider, inputBounds);
      }

      ImageStats[] stats = rvp.getStats();
      if (stats != null)
      {
        String adhoc = context.getConfiguration().get(STATS_PROVIDER, null);
        if (adhoc == null)
        {
          throw new IOException("Stats provider not set");
          
        }
        AdHocDataProvider statsProvider = DataProviderFactory.getAdHocDataProvider(adhoc,
            AccessMode.WRITE, conf);
        ImageStats.writeStats(statsProvider, stats);
      }
    }

    @Override
    public void reduce(final TileIdWritable key, final Iterable<GeometryWritable> it, final Context context)
        throws IOException, InterruptedException
    {
      final Iterator<GeometryWritable> values = it.iterator();
      final long tileId = key.get();

      totalTiles.increment(1);

      rvp.beforePaintingTile(tileId);
      while (values.hasNext())
      {
        features.increment(1);
        final Geometry f = values.next().getGeometry();

        if (f != null && f.isValid())
        {
          rvp.paintGeometry(f);
        }
        else
        {
          log.error("Invalid feature: " + f.toString());
        }
        if (!rvp.afterPaintingGeometry(f))
        {
          break;
        }
      }
      final RasterWritable value = rvp.afterPaintingTile();
      context.write(key, value);
    }


    @Override
    public void setup(final Context context)
    {
      totalTiles = context.getCounter("Feature Painter", "Tiles Processed");
      features = context.getCounter("Feature Painter", "Features Processed");
      rvp.setup(context.getConfiguration());
    }
  }

  static final Logger log = LoggerFactory.getLogger(RasterizeVectorDriver.class);

  public static String OUTPUT_FILENAME = "outputFilename";
  public static String GEOMETRY_FILTER = "geometryFilter";
  public static String TEST_REDUCER = "testReducer"; // will be used for testing only
  private FeatureFilter filter = null;
  String valueColumn = null;

  public void run(final Job job, final String output,
      final RasterizeVectorPainter.AggregationType aggregationType,
      final int zoom, final Bounds bounds, final Progress progress,
      final JobListener jobListener, final String protectionLevel,
      final Properties providerProperties)
      throws IOException, JobFailedException, JobCancelledException
  {
    // create a new unique job name
    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    final String jobName = "RasterizeVector_" + now + "_" + UUID.randomUUID().toString();
    job.setJobName(jobName);
    
    MapReduceUtils.setupTiledJob(job);
    
    final Configuration conf = job.getConfiguration();

    if (filter != null)
    {
      conf.set(FeatureToTilesMapper.FEATURE_FILTER, Base64Utils.encodeObject(filter));
    }

    HadoopUtils.setJar(job, this.getClass());
    final int tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty(
        "mrsimage.tilesize", "512"));

    conf.set(FeatureToTilesMapper.ZOOM, String.valueOf(zoom));
    conf.set(FeatureToTilesMapper.TILE_SIZE, String.valueOf(tilesize));
    conf.set(FeatureToTilesMapper.BOUNDS, bounds.toDelimitedString());

    conf.set(RasterizeVectorPainter.ZOOM, String.valueOf(zoom));
    conf.set(RasterizeVectorPainter.TILE_SIZE, String.valueOf(tilesize));
    conf.set(RasterizeVectorPainter.BOUNDS, bounds.toDelimitedString());

    conf.set(RasterizeVectorPainter.AGGREGATION_TYPE, aggregationType.toString());

    if (valueColumn != null)
    {
      conf.set(RasterizeVectorPainter.VALUE_COLUMN, valueColumn);
    }

    job.setMapOutputKeyClass(TileIdWritable.class);
    job.setMapOutputValueClass(GeometryWritable.class);
    job.setMapperClass(FeatureToTilesMapper.class);
    final MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setMaxZoomLevel(zoom);
    metadata.setTilesize(512);
    metadata.setBands(1);
    metadata.setTileType(DataBuffer.TYPE_DOUBLE);
    metadata.setProtectionLevel(protectionLevel);
    HadoopUtils.setMetadata(job, metadata);

    final AdHocDataProvider statsProvider = DataProviderFactory.createAdHocDataProvider(
        providerProperties);
    final AdHocDataProvider boundsProvider = DataProviderFactory.createAdHocDataProvider(
        providerProperties);
    // get the ad hoc provider set up for map/reduce
    statsProvider.setupJob(job);
    boundsProvider.setupJob(job);
    conf.set(RasterizeVectorDriver.STATS_PROVIDER, statsProvider.getResourceName());
    conf.set(RasterizeVectorDriver.BOUNDS_PROVIDER, boundsProvider.getResourceName());

    MrsImageOutputFormatProvider provider = MrsImageDataProvider.setupMrsPyramidOutputFormat(job,
        output, bounds, zoom, tilesize, protectionLevel, providerProperties);
    job.setOutputKeyClass(TileIdWritable.class);
    job.setOutputValueClass(RasterWritable.class);
    job.setReducerClass(PaintReduce.class);

    if (MapReduceUtils.runJob(job, progress, jobListener))
    {
      provider.teardown(job);
      // save the metadata
      final double[] defaultValues = new double[] { Double.NaN };
      
      // FIXME: We need to address if NaN really needs to be marked as a nodata value
      // There are issues with Legion code interpreting this,
      // https://107.23.31.196/redmine/issues/2337
      // In GeoTiffExporter we use -9999 as the default "advertised" nodata value.
      // defaultValues[0]= -9999;
      
      final Bounds outputBounds = MapReduceUtils.aggregateBounds(boundsProvider);
      MrsImagePyramid.calculateMetadataWithProvider(output, zoom, provider.getImageProvider(),
          statsProvider, defaultValues,
          outputBounds, conf, protectionLevel, providerProperties);
      statsProvider.delete();
      boundsProvider.delete();
    }
  }

  public void run(final Configuration conf,
      final String input, final String output, final RasterizeVectorPainter.AggregationType aggregationType,
      final int zoom, final Bounds bounds, final Progress progress,
      final JobListener jobListener, final String protectionLevel,
      final Properties providerProperties)
      throws IOException, JobFailedException, JobCancelledException
  {
    final Job job = new Job(conf);

    VectorDataProvider vdp = DataProviderFactory.getVectorDataProvider(input, AccessMode.READ,
        providerProperties);
    Set<String> inputs = new HashSet<String>();
    inputs.add(input);
    VectorInputFormatContext ifContext = new VectorInputFormatContext(
        inputs, providerProperties);
    VectorInputFormatProvider ifp = vdp.getVectorInputFormatProvider(ifContext);
    ifp.setupJob(job, providerProperties);
//    HadoopUtils.setupLocalRunner(job.getConfiguration());
    job.setInputFormatClass(VectorInputFormat.class);
    run(job, output, aggregationType, zoom, bounds, progress, jobListener, protectionLevel,
        providerProperties);
  }

  public void setFeatureFilter(final FeatureFilter filter)
  {
    this.filter = filter;
  }

  public void setValueColumn(final String weightColumn)
  {
    this.valueColumn = weightColumn;
  }
}
