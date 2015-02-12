package org.mrgeo.mapreduce;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.RasterizeVectorDriver.PaintReduce;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasterizeVectorNewDriver
{
  static final Logger log = LoggerFactory.getLogger(RasterizeVectorDriver.class);

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
      MrsImagePyramid.calculateMetadata(output, zoom, provider.getMetadataWriter(),
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
