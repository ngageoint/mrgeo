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

package org.mrgeo.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.codehaus.jackson.map.ObjectMapper;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProtectionLevelValidator;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestWriterContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileWriter;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatProvider;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class IngestImageDriver
{
  public static class IngestImageException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public IngestImageException(final Exception e)
    {
      this.origException = e;
    }

    public IngestImageException(final String msg)
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

  private static Logger log = LoggerFactory.getLogger(IngestImageDriver.class);

  //private final static GeoTiffWriteParams DEFAULT_WRITE_PARAMS;

  static
  {
    initialize();

    // setting the write parameters (we my want to make these configurable in
    // the future
//    DEFAULT_WRITE_PARAMS = new GeoTiffWriteParams();
//    DEFAULT_WRITE_PARAMS.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
//    DEFAULT_WRITE_PARAMS.setCompressionType("LZW");
//    DEFAULT_WRITE_PARAMS.setCompressionQuality(0.75F);
//    DEFAULT_WRITE_PARAMS.setTilingMode(ImageWriteParam.MODE_DISABLED);
    // DEFAULT_WRITE_PARAMS.setTilingMode(ImageWriteParam.MODE_EXPLICIT);
    // DEFAULT_WRITE_PARAMS.setTiling(512, 512);
  }

  public static boolean ingest(final String[] inputs, final String output,
      final boolean categorical, final Configuration config, final Bounds bounds,
      final int zoomlevel, final int tilesize, final Number nodata, final int bands,
      final Map<String, String> tags, final String protectionLevel,
      final Properties providerProperties) throws Exception
  {
    final ImageIngestDataProvider provider = DataProviderFactory.getImageIngestDataProvider(inputs[0],
        AccessMode.READ);

    return runJob(inputs, output, config, provider.getRawInputFormat(), bounds, nodata,
        categorical, zoomlevel, tilesize, bands, tags, protectionLevel, providerProperties);
  }

  public static boolean localIngest(final String[] inputs, final String output,
      final boolean categorical, final Configuration config, final Bounds bounds,
      final int zoomlevel, final int tilesize, final Number nodata, final int bands,
      final Map<String, String> tags, final String protectionLevel,
      final Properties providerProperties) throws Exception
  {

    final ImageIngestDataProvider provider =
        DataProviderFactory.getImageIngestDataProvider(HadoopUtils.createRandomString(10), AccessMode.OVERWRITE);

    Configuration conf = config;
    if (conf == null)
    {
      conf = HadoopUtils.createConfiguration();
    }

//    final Path unique = HadoopFileUtils.createUniqueTmpPath();
    try
    {
      final ImageIngestWriterContext context = new ImageIngestWriterContext();
      context.setZoomlevel(zoomlevel);
      context.setPartNum(0);
      final MrsTileWriter<Raster> writer = provider.getMrsTileWriter(context);

      // TODO: Can't remember why we needed multiple (sequence) files. Keeping the code commented
      // out here in case I remember.

      // 10 blocks per split
      // final long splitsize = conf.getInt("dfs.block.size", 67108864) * 10;
      // long totalbytes = 0;
      for (final String input : inputs)
      {
        URI uri = new URI(input);
        final AbstractGridCoverage2DReader reader =
            GeotoolsRasterUtils.openImageFromFile(new File(uri.getPath()));

        log.info("  reading: " + input.toString());

        if (reader != null)
        {
          final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");
          final LongRectangle tilebounds = GeotoolsRasterUtils.calculateTiles(reader, tilesize,
              zoomlevel);

          final double[] defaults = new double[bands];
          for (int i = 0; i < bands; i++)
          {
            defaults[i] = nodata.doubleValue();
          }

          log.info("    zoomlevel: " + zoomlevel);

          for (long ty = tilebounds.getMinY(); ty <= tilebounds.getMaxY(); ty++)
          {
            for (long tx = tilebounds.getMinX(); tx <= tilebounds.getMaxX(); tx++)
            {
              final Raster raster = GeotoolsRasterUtils.cutTile(image, tx, ty, zoomlevel, tilesize,
                  defaults, categorical);

              // final long len = raster.getDataBuffer().getSize();
              //
              // if (totalbytes + len > splitsize)
              // {
              // writer.close();
              //
              // context.setPartNum(context.getPartNum() + 1);
              // writer = provider.getMrsTileWriter(context);
              // tmpPaths.add(file.toString());
              //
              // totalbytes = 0;
              // }
              // totalbytes += len;

              writer.append(new TileIdWritable(TMSUtils.tileid(tx, ty, zoomlevel)), raster);

            }
          }

        }
      }
      writer.close();

      return runJob(new String[] {writer.getName()}, output, config, provider.getTiledInputFormat(), bounds, nodata,
          categorical, zoomlevel, tilesize, bands, tags, protectionLevel, providerProperties);
    }
    finally
    {
      provider.delete();
    }
  }

  public static boolean quickIngest(final InputStream input, final String output,
      final boolean categorical, final Configuration config, final boolean overridenodata,
      final String protectionLevel,
      final Number nodata) throws Exception
  {

    Configuration conf = config;
    if (conf == null)
    {
      conf = HadoopUtils.createConfiguration();
    }

    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(input,output, false, categorical);

    if (overridenodata)
    {
      final double[] defaults = metadata.getDefaultValues();
      for (int i = 0; i < defaults.length; i++)
      {
        defaults[i] = nodata.doubleValue();
      }
      metadata.setDefaultValues(defaults);
    }

    //final String outputWithZoom = output + "/" + metadata.getMaxZoomLevel();
    //Path file = new Path(outputWithZoom, "part-00000");

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.OVERWRITE, conf);

    String useProtectionLevel = getAndValidateProtectionLevel(dp, protectionLevel);
    metadata.setProtectionLevel(useProtectionLevel);

    MrsTileWriter<Raster> writer = dp.getMrsTileWriter(metadata.getMaxZoomLevel());

    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImageFromStream(input);

    log.info("  reading: " + input.toString());

    if (reader != null)
    {
      final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

      final LongRectangle bounds = GeotoolsRasterUtils.calculateTiles(reader,
          metadata.getTilesize(),
          metadata.getMaxZoomLevel());

      final int zoomlevel = metadata.getMaxZoomLevel();
      final int tilesize = metadata.getTilesize();
      final double[] defaults = metadata.getDefaultValues();

      log.info("    zoomlevel: " + zoomlevel);

      for (long ty = bounds.getMinY(); ty <= bounds.getMaxY(); ty++)
      {
        for (long tx = bounds.getMinX(); tx <= bounds.getMaxX(); tx++)
        {
          final Raster raster = GeotoolsRasterUtils.cutTile(image, tx, ty, zoomlevel,
              tilesize, defaults, categorical);

          writer.append(new TileIdWritable(TMSUtils.tileid(tx, ty, zoomlevel)), raster);
        }
      }
    }
    writer.close();

    // write the metadata
    dp.getMetadataWriter().write(metadata);

    return true;
  }

  /**
   * If the passed protection level is null or empty, then check to see
   * if MrGeo is configured to require a protection level. If so, return
   * the configured default protection level if it is non-null and non-empty.
   * Otherwise, throw an exception indicating that the required protection
   * level is missing.
   * 
   * If the passed protection level is null or empty, and MrGeo is configured
   * such that protection level is not required, then return a blank string.
   * 
   * @param protectionLevel
   * @return
   * @throws Exception
   */
  private static String getAndValidateProtectionLevel(final ProtectionLevelValidator validator,
      final String protectionLevel) throws Exception
  {
    String actualProtectionLevel = protectionLevel;
    if (actualProtectionLevel == null || actualProtectionLevel.isEmpty())
    {
      // No protection level was passed in, so we need to check to see
      // if it is required. If it is, then return the default protection
      // level if it is defined or throw an exception.
      Properties props = MrGeoProperties.getInstance();
      String protectionLevelRequired = props.getProperty(
          MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED, "false").trim();
      if (protectionLevelRequired.equalsIgnoreCase("true"))
      {
        String protectionLevelDefault = props.getProperty(
            MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT, "");
        if (protectionLevelDefault == null || protectionLevelDefault.isEmpty())
        {
          throw new Exception("Missing required protection level.");
        }
        actualProtectionLevel = protectionLevelDefault;
      }
      else
      {
        actualProtectionLevel = "";
      }
    }
    if (actualProtectionLevel != null && !actualProtectionLevel.isEmpty())
    {
      if (!validator.validateProtectionLevel(protectionLevel))
      {
        throw new Exception("Invalid visibility " + protectionLevel);
      }
    }
    return actualProtectionLevel;
  }

  public static boolean quickIngest(final String input, final String output,
      final boolean categorical, final Configuration config, final boolean overridenodata,
      final Number nodata,
      final Map<String, String> tags, final String protectionLevel,
      final Properties providerProperties) throws Exception
  {

    final MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(output,
        AccessMode.OVERWRITE, providerProperties);
    Configuration conf = config;
    if (conf == null)
    {
      conf = HadoopUtils.createConfiguration();
    }

    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(
        new String[] { input }, output, false, categorical, overridenodata);

    if (tags != null)
    {
      metadata.setTags(tags);
    }

    String useProtectionLevel = getAndValidateProtectionLevel(provider, protectionLevel);
    metadata.setProtectionLevel(useProtectionLevel);

    if (overridenodata)
    {
      final double[] defaults = metadata.getDefaultValues();
      for (int i = 0; i < defaults.length; i++)
      {
        defaults[i] = nodata.doubleValue();
      }
      metadata.setDefaultValues(defaults);
    }

    final MrsTileWriter<Raster> writer = provider.getMrsTileWriter(metadata.getMaxZoomLevel());
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(input);

    log.info("  reading: " + input.toString());

    if (reader != null)
    {
      final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

      final LongRectangle bounds = GeotoolsRasterUtils.calculateTiles(reader, metadata
          .getTilesize(), metadata.getMaxZoomLevel());

      final int zoomlevel = metadata.getMaxZoomLevel();
      final int tilesize = metadata.getTilesize();
      final double[] defaults = metadata.getDefaultValues();

      log.info("    zoomlevel: " + zoomlevel);

      for (long ty = bounds.getMinY(); ty <= bounds.getMaxY(); ty++)
      {
        for (long tx = bounds.getMinX(); tx <= bounds.getMaxX(); tx++)
        {
          final Raster raster = GeotoolsRasterUtils.cutTile(image, tx, ty, zoomlevel, tilesize,
              defaults, categorical);

          writer.append(new TileIdWritable(TMSUtils.tileid(tx, ty, zoomlevel)), raster);
        }
      }
    }
    writer.close();

    provider.getMetadataWriter().write(metadata);

    return true;
  }

  private static void aggregateMetadata(final AdHocDataProvider provider, MrsImageOutputFormatProvider outputProvider,
      final String output, ImageStats[] stats, final Map<String, String> tags,
      final String protectionLevel,
      final Properties providerProperties)
      throws IOException
  {
    MrsImagePyramidMetadata metadata = null;

    try
    {
      final List<MrsImagePyramidMetadata> metas = new ArrayList<MrsImagePyramidMetadata>();
      final ObjectMapper mapper = new ObjectMapper();
      for (int i = 0; i < provider.size(); i++)
      {
        final InputStream stream = provider.get(i);
        metas.add(mapper.readValue(stream, MrsImagePyramidMetadata.class));

        stream.close();
      }

      provider.delete();

      for (final MrsImagePyramidMetadata meta : metas)
      {
        if (metadata == null)
        {
          metadata = meta;
        }
        else
        {
          final Bounds b = metadata.getBounds();
          b.expand(meta.getBounds());

          metadata.setBounds(b);
        }
      }

      if (metadata != null)
      {
        final int zoom = metadata.getMaxZoomLevel();
        final int tilesize = metadata.getTilesize();

        metadata.setPyramid(output);
        metadata.setName(zoom);
        metadata.setProtectionLevel(protectionLevel);

        final TMSUtils.Bounds bounds = metadata.getBounds().getTMSBounds();

        final TMSUtils.Pixel lowerPx = TMSUtils.latLonToPixels(bounds.s, bounds.w, zoom, tilesize);
        final TMSUtils.Pixel upperPx = TMSUtils.latLonToPixels(bounds.n, bounds.e, zoom, tilesize);

        metadata.setPixelBounds(zoom, new LongRectangle(0, 0, upperPx.px - lowerPx.px, upperPx.py -
            lowerPx.py));

        final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(bounds, zoom, tilesize);
        metadata.setTileBounds(zoom, new LongRectangle(tb.w, tb.s, tb.e, tb.n));

        metadata.setStats(stats);
        metadata.setImageStats(zoom, stats);

        if (tags != null)
        {
          metadata.setTags(tags);
        }

        // Use AccessMode READ below since the image itself should already exist
        // by this point.
//        final MrsImageDataProvider imageprovider = DataProviderFactory
//            .getMrsImageDataProvider(output, AccessMode.READ, providerProperties);

        outputProvider.getMetadataWriter().write(metadata);
      }
    }
    catch (final IOException e)
    {
      log.error("Unable to read metadata files.", e);
      throw e;
    }
  }


  @SuppressWarnings("unused")
  private static String getTime(final long milli)
  {
    long time = milli;
    if (time < 0)
    {
      throw new IllegalArgumentException("Duration must be greater than zero!");
    }

    final long days = TimeUnit.MILLISECONDS.toDays(time);
    time -= TimeUnit.DAYS.toMillis(days);

    final long hours = TimeUnit.MILLISECONDS.toHours(time);
    time -= TimeUnit.HOURS.toMillis(hours);

    final long minutes = TimeUnit.MILLISECONDS.toMinutes(time);
    time -= TimeUnit.MINUTES.toMillis(minutes);

    final long seconds = TimeUnit.MILLISECONDS.toSeconds(time);

    final StringBuilder sb = new StringBuilder(64);

    if (days > 0)
    {
      sb.append(String.format("%02d", days));
      sb.append(" d ");
    }
    sb.append(String.format("%02d", hours));
    sb.append(":");
    sb.append(String.format("%02d", minutes));
    sb.append(":");
    sb.append(String.format("%02d", seconds));

    return (sb.toString());
  }

  private static void initialize()
  {
    GeotoolsRasterUtils.addMissingEPSGCodes();
  }

  private static boolean runJob(final String[] inputs, final String output,
      final Configuration config, final TiledInputFormatProvider<RasterWritable> formatProvider,
      final Bounds bounds, final Number nodata, final boolean categorical, final int zoomlevel,
      final int tilesize, final int bands,
      final Map<String, String> tags, final String protectionLevel,
      final Properties providerProperties) throws Exception
  {

    Configuration conf = config;
    if (conf == null)
    {
      conf = HadoopUtils.createConfiguration();
    }

    final Job job = new Job(conf, "IngestImage");
    conf = job.getConfiguration();

    HadoopUtils.setJar(job, IngestImageDriver.class);

    job.setMapperClass(IngestImageMapper.class);
    job.setReducerClass(IngestImageReducer.class);

    for (final String input : inputs)
    {
      // using FileInputFormat for convenience. It creates "mapred.input.dir" in the config
      FileInputFormat.addInputPath(job, new Path(input));
    }

    formatProvider.setupJob(job, providerProperties);

    // getInputFormat takes an image name, but we don't need it here, so we'll just send an empty string
    job.setInputFormatClass(formatProvider.getInputFormat("").getClass());

    final AdHocDataProvider metadataProvider = DataProviderFactory.createAdHocDataProvider(
        providerProperties);
    final AdHocDataProvider statsProvider = DataProviderFactory.createAdHocDataProvider(
        providerProperties);

    // get the ad hoc providers set up for map/reduce
    metadataProvider.setupJob(job);
    statsProvider.setupJob(job);

    conf.set("metadata.provider", metadataProvider.getResourceName());
    conf.set("stats.provider", statsProvider.getResourceName());
    conf.setInt("zoomlevel", zoomlevel);
    conf.setInt("tilesize", tilesize);
    conf.setFloat("nodata", nodata.floatValue());
    conf.setInt("bands", bands);
    
    if (categorical)
    {
      conf.set("classification", Classification.Categorical.name());
    }
    else
    {
      conf.set("classification", Classification.Continuous.name());
    }

    MrsImageOutputFormatProvider provider = MrsImageDataProvider.setupMrsPyramidOutputFormat(job,
        output, bounds, zoomlevel, tilesize, providerProperties);

    // set the protection level of the image
    String useProtectionLevel = getAndValidateProtectionLevel(provider, protectionLevel);
    conf.set("protectionLevel", useProtectionLevel);

    try
    {
      job.submit();

      final boolean success = job.waitForCompletion(true);
      if (success)
      {
        provider.teardown(job);

        ImageStats[] stats = ImageStats.readStats(statsProvider);
        aggregateMetadata(metadataProvider, provider, output, stats, tags, useProtectionLevel, providerProperties);
      }

      return success;
    }
    catch (final ClassNotFoundException e)
    {
      throw new IOException("Error running ingest map/reduce", e);
    }
    catch (final InterruptedException e)
    {
      throw new IOException("Error running ingest map/reduce", e);
    }
    finally
    {
      statsProvider.delete();
      metadataProvider.delete();
    }
  }

}
