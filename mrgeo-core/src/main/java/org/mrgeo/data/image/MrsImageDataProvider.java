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

package org.mrgeo.data.image;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.*;
import org.mrgeo.image.BoundsCropper;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.MapReduceUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * This class is what the MrGeo core code calls to make use of image pyramids
 * for its various operations, including:
 * <ul>
 * <li> map/reducing over input images
 * <li> map/reducing where the output is an image pyramid
 * <li> reading and writing metadata
 * <li> reading and writing image tiles.
 * </ul>
 * <p>
 * A data plugin that wishes to store image data must extent this class and
 * implement its abstract methods.
 * 
 * This class also contains a series of static methods that are conveniences
 * for configuring map/reduces jobs that use image pyramids as input data.
 */
public abstract class MrsImageDataProvider extends TileDataProvider<Raster>
{
  protected Properties providerProperties;

  protected MrsImageDataProvider()
  {
    super();
  }

  public Properties getProviderProperties()
  {
    return providerProperties;
  }

  public MrsImageDataProvider(final String resourceName)
  {
    super(resourceName);
  }


  /**
   * Convenience function for setting up an image pyramid as the input for a
   * map/reduce job along with a bounds in such a way that all tiles within
   * the bounds are sent to the mappers, even if the input image does not
   * contain data in some of those tiles. In that case, a tile containing
   * the specifed fill value for each pixel is sent instead.
   * 
   * @param job
   * @param input
   * @param bounds
   * @param fillValue
   * @param providerProperties
   * @throws IOException
   */
  public static void
  setupMrsImagePyramidAllTilesSingleInputFormat(final Job job, final String input,
      final Bounds bounds, final double fillValue, final Properties providerProperties)
    throws IOException
{

  int zoom = Integer.MAX_VALUE;
  final MrsImagePyramid pyramid = MrsImagePyramid.open(input, providerProperties);
  final MrsImagePyramidMetadata metadata = pyramid.getMetadata();

  if (metadata.getMaxZoomLevel() < zoom)
  {
    zoom = metadata.getMaxZoomLevel();
  }

  setupMrsImagePyramidAllTilesSingleInputFormat(job, input, zoom,
      metadata.getTilesize(), bounds, fillValue, providerProperties);
}

  /**
   * Convenience function for setting up an image pyramid as the input for a
   * map/reduce job along with a bounds in such a way that all tiles within
   * the bounds are sent to the mappers, even if the input image does not
   * contain data in some of those tiles. In that case, a tile containing
   * the specifed fill value for each pixel is sent instead.
   * 
   * @param job
   * @param input
   * @param zoomlevel
   * @param tileSize
   * @param bounds
   * @param fillValue
   * @param providerProperties
   * @throws IOException
   */
public static void setupMrsImagePyramidAllTilesSingleInputFormat(final Job job,
  final String input, final int zoomlevel, final int tileSize, final Bounds bounds,
  final double fillValue, final Properties providerProperties) throws IOException
{
  Set<String> inputs = new HashSet<String>(1);
  inputs.add(input);
  TiledInputFormatContext context = new TiledInputFormatContext(
      zoomlevel, tileSize, inputs, bounds, fillValue, providerProperties);
  MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(input,
      AccessMode.READ, providerProperties);
  if (provider == null)
  {
    throw new IOException("No data provider available for input " + input);
  }
  MrsImageInputFormatProvider ifProvider = provider.getTiledInputFormatProvider(context);
  if (ifProvider == null)
  {
    throw new IOException("No input format provider available for " + input);
  }
  ifProvider.setupJob(job, providerProperties);
  job.setInputFormatClass(MrsImagePyramidInputFormat.class);
}

/**
 * Convenience function for setting up an image pyramid as the input for a
 * map/reduce job. Only the tiles containing actual image data will be input
 * to the mappers.
 * 
 * @param job
 * @param inputs
 * @param zoomlevel
 * @param tileSize
 * @param providerProperties
 * @throws IOException
 */
public static void setupMrsPyramidInputFormat(final Job job, final Set<String> inputs,
    final int zoomlevel, final int tileSize, final Properties providerProperties) throws IOException
{
  setupMrsPyramidInputFormat(job, inputs, zoomlevel, tileSize, null, providerProperties);
}

/**
 * Convenience function for setting up an image pyramid as the input for a
 * map/reduce job. Only the tiles containing actual image data will be input
 * to the mappers.
 * 
 * @param job
 * @param inputs
 * @param zoomlevel
 * @param tileSize
 * @param bounds
 * @param providerProperties
 * @throws IOException
 */
public static void setupMrsPyramidInputFormat(final Job job, final Set<String> inputs,
  final int zoomlevel, final int tileSize, final Bounds bounds,
  final Properties providerProperties) throws IOException
{
  String firstInput = inputs.iterator().next();
  MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(firstInput,
      AccessMode.READ, providerProperties);
  if (provider == null)
  {
    throw new IOException("No data provider available for input " + firstInput);
  }
  MrsImagePyramidMetadata croppedMetadata = provider.getMetadataReader().read();
  if (bounds != null)
  {
    // serialize cropped metadata inside the job
    croppedMetadata = BoundsCropper.getCroppedMetadata(croppedMetadata,
        Collections.singletonList(TMSUtils.Bounds.convertOldToNewBounds(bounds)),
      zoomlevel);
  }
  TiledInputFormatContext context = new TiledInputFormatContext(zoomlevel,
	        tileSize, inputs, croppedMetadata.getBounds(),
	        providerProperties);
  MrsImageInputFormatProvider ifProvider = provider.getTiledInputFormatProvider(context);
  if (ifProvider == null)
  {
    throw new IOException("No input format provider available for " + firstInput);
  }
  ifProvider.setupJob(job, providerProperties);
  job.setInputFormatClass(MrsImagePyramidInputFormat.class);
  // The following must be after the setupJob() call so that we overwrite the metadata
  // stored into the config during setupJob() with the cropped metadata.
  HadoopUtils.setMetadata(job, croppedMetadata);
}

//public static void setupMrsPyramidInputFormat(final Job job, final String input)
//  throws IOException
//{
//  final Set<String> a = new HashSet<String>();
//  a.add(input);
//  setupMrsPyramidInputFormat(job, a);
//}

/**
 * Convenience function for setting up an image pyramid as the input for a
 * map/reduce job. Only the tiles containing actual image data will be input
 * to the mappers.
 * 
 * @param job
 * @param input
 * @param zoomlevel
 * @param tileSize
 * @param providerProperties
 * @throws IOException
 */
public static void setupMrsPyramidInputFormat(final Job job, final String input,
  final int zoomlevel, final int tileSize, final Properties providerProperties) throws IOException
{
  final Set<String> a = new HashSet<String>();
  a.add(input);
  setupMrsPyramidInputFormat(job, a, zoomlevel, tileSize, providerProperties);
}


/**
 * Convenience function for setting up a single image pyramid as the input for a
 * map/reduce job. Only the tiles containing actual image data will be input
 * to the mappers.
 * 
 * @param job
 * @param input
 * @param providerProperties
 * @throws IOException
 */
public static void setupMrsPyramidSingleInputFormat(final Job job, final String input,
    final Properties providerProperties)
  throws IOException
{

  int zoom = Integer.MAX_VALUE;
  final MrsImagePyramid pyramid = MrsImagePyramid.open(input, providerProperties);
  final MrsImagePyramidMetadata metadata = pyramid.getMetadata();

  if (metadata.getMaxZoomLevel() < zoom)
  {
    zoom = metadata.getMaxZoomLevel();
  }
  setupMrsPyramidSingleInputFormat(job, input, zoom, metadata.getTilesize(), null,
      providerProperties);
}

/**
 * Convenience function for setting up a single image pyramid as the input for a
 * map/reduce job. Only the tiles containing actual image data will be input
 * to the mappers.
 * 
 * @param job
 * @param input
 * @param zoomlevel
 * @param tileSize
 * @param bounds
 * @param providerProperties
 * @throws IOException
 */
  public static void setupMrsPyramidSingleInputFormat(final Job job, final String input,
    final int zoomlevel, final int tileSize, final Bounds bounds,
    final Properties providerProperties) throws IOException
  {
    setupCommonMrsPyramidSingleInputFormat(job, input, zoomlevel, tileSize, bounds, providerProperties);
    job.setInputFormatClass(MrsImagePyramidInputFormat.class);
  }

  /**
   * Convenience function for setting up a single image pyramid as the input for a
   * map/reduce job. Only the tiles containing actual image data will be input
   * to the mappers.
   *
   * @param job
   * @param input
   * @param providerProperties
   * @throws IOException
   */
  public static void setupMrsPyramidSingleSimpleInputFormat(final Job job, final String input,
                                                            final Properties providerProperties)
          throws IOException
  {

    int zoom = Integer.MAX_VALUE;
    final MrsImagePyramid pyramid = MrsImagePyramid.open(input, providerProperties);
    final MrsImagePyramidMetadata metadata = pyramid.getMetadata();

    if (metadata.getMaxZoomLevel() < zoom)
    {
      zoom = metadata.getMaxZoomLevel();
    }
    setupMrsPyramidSingleSimpleInputFormat(job, input, zoom, metadata.getTilesize(), null,
            providerProperties);
  }

  /**
   * Convenience function for setting up a single image pyramid as the input for a
   * map/reduce job. Only the tiles containing actual image data will be input
   * to the mappers.
   *
   * @param job
   * @param input
   * @param zoomlevel
   * @param tileSize
   * @param bounds
   * @param providerProperties
   * @throws IOException
   */
  public static void setupMrsPyramidSingleSimpleInputFormat(final Job job, final String input,
                                                            final int zoomlevel, final int tileSize,
                                                            final Bounds bounds,
                                                            final Properties providerProperties) throws IOException
  {
    setupCommonMrsPyramidSingleInputFormat(job, input, zoomlevel, tileSize, bounds, providerProperties);
    job.setInputFormatClass(MrsImagePyramidSimpleInputFormat.class);
  }

  private static void setupCommonMrsPyramidSingleInputFormat(final Job job, final String input,
                                                             final int zoomlevel, final int tileSize,
                                                             final Bounds bounds,
                                                             final Properties providerProperties) throws IOException
  {
    final MrsImagePyramid pyramid = MrsImagePyramid.open(input, providerProperties);
    MrsImagePyramidMetadata croppedMetadata = pyramid.getMetadata();
    if (bounds != null)
    {
      // serialize cropped metadata inside the job
      croppedMetadata = BoundsCropper.getCroppedMetadata(pyramid.getMetadata(),
              Collections.singletonList(TMSUtils.Bounds.convertOldToNewBounds(bounds)),
              zoomlevel);
    }
    Set<String> inputs = new HashSet<String>(1);
    inputs.add(input);
    TiledInputFormatContext context = new TiledInputFormatContext(zoomlevel,
            tileSize, inputs, croppedMetadata.getBounds(),
            providerProperties);
    MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(input,
            AccessMode.READ, providerProperties);
    if (provider == null)
    {
      throw new IOException("No data provider available for input " + input);
    }
    MrsImageInputFormatProvider ifProvider = provider.getTiledInputFormatProvider(context);
    if (ifProvider == null)
    {
      throw new IOException("No input format provider available for " + input);
    }
    ifProvider.setupJob(job, providerProperties);
    // The following must be after the setupJob() call so that we overwrite the metadata
    // stored into the config during setupJob() with the cropped metadata.
    HadoopUtils.setMetadata(job, croppedMetadata);
  }

  /**
   * Convenience function for setting up an image pyramid as the output for
   * a map/reduce job.
   *
   * @param job
   * @param output
   * @param bounds
   * @param zoomlevel
   * @param tilesize
   * @param providerProperties
   * @return
   * @throws IOException
   */
  public static MrsImageOutputFormatProvider setupMrsPyramidOutputFormat(final Job job, final String output,
    final Bounds bounds, final int zoomlevel, final int tilesize,
    final String protectionLevel,
    final Properties providerProperties) throws IOException
  {
    final TiledOutputFormatContext context = new TiledOutputFormatContext(output, bounds,
      zoomlevel, tilesize);
    
    job.getConfiguration().set(MrGeoConstants.MRGEO_PROTECTION_LEVEL, protectionLevel);
    job.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
    final MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(output,
        AccessMode.OVERWRITE, providerProperties);
    
    if (provider == null)
    {
      throw new IOException("No data provider available for output " + output);
    }
    final MrsImageOutputFormatProvider ofProvider = provider.getTiledOutputFormatProvider(context);
    if (ofProvider == null)
    {
      throw new IOException("No output format provider available for " + output);
    }
    MapReduceUtils.setupTiledJob(job);
    ofProvider.setupJob(job);
    return ofProvider;
  }

  /**
   * Convenience function for setting up an image pyramid as the output for
   * a map/reduce job.
   * 
   * @param job
   * @param output
   * @param bounds
   * @param zoomlevel
   * @param tilesize
   * @param tiletype
   * @param bands
   * @param providerProperties
   * @return
   * @throws IOException
   */
  public static MrsImageOutputFormatProvider setupMrsPyramidOutputFormat(final Job job, final String output,
    final Bounds bounds, final int zoomlevel, final int tilesize, final int tiletype,
    final int bands, final String protectionLevel, final Properties providerProperties) throws IOException
  {
    final TiledOutputFormatContext context = new TiledOutputFormatContext(output, bounds,
      zoomlevel, tilesize, tiletype, bands);
    job.getConfiguration().set(MrGeoConstants.MRGEO_PROTECTION_LEVEL, protectionLevel);
    job.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
    final MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(output,
      AccessMode.OVERWRITE, providerProperties);
    if (provider == null)
    {
      throw new IOException("No data provider available for output " + output);
    }
    final MrsImageOutputFormatProvider ofProvider = provider.getTiledOutputFormatProvider(context);
    if (ofProvider == null)
    {
      throw new IOException("No output format provider available for " + output);
    }
    
    MapReduceUtils.setupTiledJob(job);
    ofProvider.setupJob(job);
    return ofProvider;
  }

  public MrsImagePyramidMetadataReader getMetadataReader()
  {
    return getMetadataReader(null);
  }

  /**
   * Return an instance of a class that can read metadata for this resource.
   * 
   * @return
   */
  public abstract MrsImagePyramidMetadataReader getMetadataReader(
    MrsImagePyramidMetadataReaderContext context);

  public MrsImagePyramidMetadataWriter getMetadataWriter()
  {
    return getMetadataWriter(null);
  }

  /**
   * Return an instance of a class that can write metadata for this resource.
   * 
   * @return
   */
  public abstract MrsImagePyramidMetadataWriter getMetadataWriter(
    MrsImagePyramidMetadataWriterContext context);

  public MrsTileReader<Raster> getMrsTileReader(final int zoomlevel) throws IOException
  {
    final MrsImagePyramidReaderContext context = new MrsImagePyramidReaderContext();
    context.setZoomlevel(zoomlevel);
    return getMrsTileReader(context);
  }

  /**
   * Return an instance of a MrsTileReader class to be used for reading tiled data. This method may
   * be invoked by callers regardless of whether they are running within a map/reduce job or not.
   * 
   * @return
   * @throws IOException 
   */
  public abstract MrsTileReader<Raster> getMrsTileReader(MrsImagePyramidReaderContext context) throws IOException;

  public MrsTileWriter<Raster> getMrsTileWriter(final int zoomlevel) throws IOException
  {
    final MrsImagePyramidWriterContext context = new MrsImagePyramidWriterContext(zoomlevel, 0);
    return getMrsTileWriter(context);
  }

  public abstract void delete(final int zoomlevel) throws IOException;

  public abstract MrsTileWriter<Raster> getMrsTileWriter(MrsImagePyramidWriterContext context) throws IOException;

  /**
   * Return an instance of a RecordReader class to be used in map/reduce jobs for reading tiled
   * data.
   * 
   * @return
   */
  public abstract RecordReader<TileIdWritable, RasterWritable> getRecordReader();

  /**
   * Return an instance of a RecordWriter class to be used in map/reduce jobs for writing tiled
   * data.
   * 
   * @return
   */
  public abstract RecordWriter<TileIdWritable, RasterWritable> getRecordWriter();

  /**
   * Return an instance of an InputFormat class to be used in map/reduce jobs for processing tiled
   * data.
   * 
   * @return
   */
  public abstract MrsImageInputFormatProvider getTiledInputFormatProvider(
    final TiledInputFormatContext context);

  /**
   * Return an instance of an OutputFormat class to be used in map/reduce jobs for producing tiled
   * data.
   * 
   * @return
   */
  public abstract MrsImageOutputFormatProvider getTiledOutputFormatProvider(
    final TiledOutputFormatContext context);
}
