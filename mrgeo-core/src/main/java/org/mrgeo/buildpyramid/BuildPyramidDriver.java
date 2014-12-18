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

package org.mrgeo.buildpyramid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.aggregators.AggregatorRegistry;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.MapReduceUtils;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.MrsTileWriter;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class BuildPyramidDriver
{
  private static Logger log = LoggerFactory.getLogger(BuildPyramidDriver.class);

  public static final String base = BuildPyramidDriver.class.getSimpleName();
  public static final String TO_LEVEL = base + ".to.level";
  public static final String FROM_LEVEL = base + ".from.level";
  
  public static final String ADHOC_PROVIDER = base + ".adhoc."; 

  public static final String AGGREGATOR = base + ".aggregator";



  public static boolean build(final String pyramidName, final Aggregator aggregator,
    final Configuration conf,
    final Properties providerProperties ) throws IOException, JobFailedException, JobCancelledException
  {
    return build(pyramidName, aggregator, conf, null, null, providerProperties);
  }

  public static boolean build(final String pyramidName, final Aggregator aggregator,
    final Configuration conf, final Progress progress, final JobListener jobListener,
    final Properties providerProperties)
    throws IOException, JobFailedException, JobCancelledException
  {
    final ProgressHierarchy ph = new ProgressHierarchy(progress);

    final MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(pyramidName,
        AccessMode.READ, providerProperties);
    MrsImagePyramid pyramid; // = MrsImagePyramid.loadPyramid(pyramidName);
    MrsImagePyramidMetadata metadata = provider.getMetadataReader().read();

    // pull the protection level of the image to put into the running configuration
    String pl = metadata.getProtectionLevel();
    if(pl != null){
    	conf.set(MrGeoConstants.MRGEO_PROTECTION_LEVEL, pl);
    } else {
    	conf.set(MrGeoConstants.MRGEO_PROTECTION_LEVEL, "");
    }
    
    final int maxLevel = metadata.getMaxZoomLevel();

    final int tilesize = metadata.getTilesize();
    int jumps = 0;
    int size = 0;
    while (size < tilesize)
    {
      jumps++;
      size = (int) Math.pow(2, jumps);
    }

    // build progress children
    final Progress child[] = new Progress[(maxLevel / jumps) + 1];
    for (int loops = 0; (loops * jumps) + 1 < maxLevel; loops++)
    {
      child[loops] = ph.createChild(1.0f);
    }

    // build the levels
    for (int loops = 0; (loops * jumps) + 1 < maxLevel; loops++)
    {
      // while we were running, there is chance the pyramid was removed from the cache and
      // reopened by another process. Re-opening it here will avoid some potential conflicts.
      pyramid = MrsImagePyramid.open(provider);
      metadata = pyramid.getMetadata();

      final int from = maxLevel - (loops * jumps);
      int to = from - jumps;

      if (to < 1)
      {
        to = 1;
      }

      final MrsTileReader<Raster> reader = provider.getMrsTileReader(from);
      try
      {
        if (reader.calculateTileCount() > 1000)
        {
          if (!buildlevelto(pyramidName, from, to, aggregator, conf, child[loops],
              jobListener, providerProperties))
          {
            return false;
          }
        }
        else
        {
          return buildlevellocal(pyramidName, from, 1, aggregator, child[loops],
              providerProperties);
        }
      }
      finally
      {
        reader.close();
      }
    }

    return true;
  }

  public static boolean buildlevel(final String pyramidName, final int level,
    final Aggregator aggregator, final Configuration conf,
    final Properties providerProperties) throws Exception
  {
    return buildlevel(pyramidName, level, aggregator, conf, null, null,
        providerProperties);
  }

  public static boolean buildlevel(final String pyramidName, final int level,
    final Aggregator aggregator, final Configuration conf, final Progress progress,
    final JobListener jobListener, final Properties providerProperties) throws Exception
  {
    return buildlevelto(pyramidName, level + 1, level, aggregator, conf, progress,
        jobListener, providerProperties);
  }

  private static boolean buildlevellocal(final String pyramidName, final int fromlevel,
    final int tolevel, final Aggregator aggregator, final Progress progress,
    final Properties providerProperties)
        throws IOException
  {
    if (progress != null)
    {
      progress.starting();
    }

    final MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(pyramidName,
        AccessMode.READ, providerProperties);

    MrsImagePyramidMetadata metadata = provider.getMetadataReader().read();
    String pl = metadata.getProtectionLevel();

    deletelevels(fromlevel, tolevel, metadata, provider);


    final Bounds bounds = metadata.getBounds();

    final int tilesize = metadata.getTilesize();

    int inputLevel = fromlevel;
    int outputLevel = inputLevel - 1;

    while (outputLevel >= tolevel)
    {
      log.info("Building pyramid for: " + pyramidName + " from: " + inputLevel + " to: " +
        outputLevel);

      final Map<TileIdWritable, WritableRaster> outputTiles = new TreeMap<TileIdWritable, WritableRaster>();

      final MrsTileReader<Raster> lastImage = provider.getMrsTileReader(inputLevel);
      final KVIterator<TileIdWritable, Raster> iter = lastImage.get();
      while (iter.hasNext())
      {
        final Raster raster = iter.next();
        final long tileid = iter.currentKey().get();

        final TMSUtils.Tile inputTile = TMSUtils.tileid(tileid, inputLevel);

        // create a smaller compatible writable raster
        final WritableRaster toraster = raster.createCompatibleWritableRaster(tilesize / 2,
          tilesize / 2);

        // decimate the input tile
        RasterUtils.decimate(raster, toraster, aggregator, metadata);

        // calculate the tile id of the output tile
        final TMSUtils.Tile outputTile = TMSUtils.calculateTile(inputTile, inputLevel, outputLevel,
          tilesize);
        final TileIdWritable outputkey = new TileIdWritable(TMSUtils.tileid(outputTile.tx,
          outputTile.ty, outputLevel));

        WritableRaster outputRaster;
        if (!outputTiles.containsKey(outputkey))
        {
          outputRaster = raster.createCompatibleWritableRaster(tilesize, tilesize);
          RasterUtils.fillWithNodata(outputRaster, metadata);

          outputTiles.put(outputkey, outputRaster);
        }
        else
        {
          outputRaster = outputTiles.get(outputkey);
        }

        // calculate the starting pixel for the output tile (make sure to use the NW coordinate)
        final TMSUtils.Bounds outputBounds = TMSUtils.tileBounds(outputTile.tx, outputTile.ty,
          outputLevel, tilesize);
        final TMSUtils.Pixel corner = TMSUtils.latLonToPixelsUL(outputBounds.n, outputBounds.w,
          outputLevel, tilesize);

        final TMSUtils.Bounds inputBounds = TMSUtils.tileBounds(inputTile.tx, inputTile.ty,
          inputLevel, tilesize);

        // calculate the starting pixel for the input tile (make sure to use the NW coordinate)
        final TMSUtils.Pixel start = TMSUtils.latLonToPixelsUL(inputBounds.n, inputBounds.w,
          outputLevel, tilesize);

        final int tox = (int) (start.px - corner.px);
        final int toy = (int) (start.py - corner.py);

        log.debug("Calculating tile from  tx: " + inputTile.tx + " ty: " + inputTile.ty + " (" +
          inputLevel + ") to tx: " + outputTile.tx + " ty: " + outputTile.ty + " (" + outputLevel +
          ") x: " + (tox) + " y: " + (toy));

        // paste the decimated raster onto the output
        outputRaster.setDataElements(tox, toy, toraster);
      }

      final ImageStats[] stats = ImageStats.initializeStatsArray(metadata.getBands());

      // final String outputName = outputWithZoom + "/part-00000";

      log.debug("Writing output file: " + provider.getResourceName() + " level: " + outputLevel);

      final MrsTileWriter<Raster> writer = provider.getMrsTileWriter(outputLevel);

      for (final Map.Entry<TileIdWritable, WritableRaster> tile : outputTiles.entrySet())
      {
        log.debug("  writing tile: " + tile.getKey().get());
        writer.append(tile.getKey(), tile.getValue());

        ImageStats.computeAndUpdateStats(stats, tile.getValue(), metadata.getDefaultValues());
      }
      writer.close();

      // save the stats...

      // while we were running, there is chance the pyramid was removed from the cache and
      // reopened by another process. Re-opening it here will avoid some potential conflicts.
//      pyramid = MrsImagePyramid.open(pyramidName);
//      metadata = pyramid.getMetadata();

      final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(new TMSUtils.Bounds(bounds.getMinX(),
        bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY()), outputLevel, tilesize);
      final LongRectangle b = new LongRectangle(tb.w, tb.s, tb.e, tb.n);

      final TMSUtils.Pixel psw = TMSUtils.latLonToPixels(bounds.getMinY(), bounds.getMinX(),
        outputLevel, tilesize);
      final TMSUtils.Pixel pne = TMSUtils.latLonToPixels(bounds.getMaxY(), bounds.getMaxX(),
        outputLevel, tilesize);

      // need to update the metadata
      metadata.setPixelBounds(outputLevel,
        new LongRectangle(0, 0, pne.px - psw.px, pne.py - psw.py));
      metadata.setTileBounds(outputLevel, b);
      metadata.setName(outputLevel);

      // update the pyramid level stats
      metadata.setImageStats(outputLevel, stats);

      // update the resampling method used to build pyramids
      metadata.setResamplingMethod(AggregatorRegistry.aggregatorRegistry.inverse().get(
        aggregator.getClass()));

      inputLevel = outputLevel;
      outputLevel -= 1;

      lastImage.close();

      provider.getMetadataWriter(null).write();
    }

    if (progress != null)
    {
      progress.complete();
    }

    return true;
  }

  private static void deletelevels(int fromlevel, int tolevel, MrsImagePyramidMetadata metadata,
      MrsImageDataProvider provider) throws IOException
  {
    MrsImagePyramidMetadata.ImageMetadata[] imagedata = metadata.getImageMetadata();

    // 1st delete all the levels we're going to create
    for (int i = tolevel; i < fromlevel; i++)
    {
      provider.delete(i);

      // remove the metadata for this level
      imagedata[i] = new MrsImagePyramidMetadata.ImageMetadata();
    }

    provider.getMetadataWriter().write();
  }

  private static boolean buildlevelto(final String pyramidName, final int fromlevel,
    final int tolevel, final Aggregator aggregator, final Configuration config,
    final Progress progress, final JobListener jobListener,
    final Properties providerProperties)
      throws IOException, JobCancelledException, JobFailedException
  {
    final MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(pyramidName,
        AccessMode.READ, providerProperties);
    final MrsImagePyramidMetadata metadata = provider.getMetadataReader().read();

    deletelevels(fromlevel, tolevel, metadata, provider);

    final Job job = MapReduceUtils.createTiledJob("BuildPyramid-" + metadata.getPyramid() + "-" +
        fromlevel + "-" + tolevel, config);

    Configuration conf = job.getConfiguration();
    
    conf.setInt(TO_LEVEL, tolevel);
    conf.setInt(FROM_LEVEL, fromlevel);
    conf.setClass(AGGREGATOR, aggregator.getClass(), Aggregator.class);

    final int tilesize = metadata.getTilesize();

    
    HadoopUtils.setMetadata(job, metadata);

    job.setMapperClass(BuildPyramidMapper.class);
    job.setReducerClass(BuildPyramidReducer.class);

    job.setMapOutputKeyClass(TileIdZoomWritable.class);
    job.setMapOutputValueClass(RasterWritable.class);

    MrsImageDataProvider.setupMrsPyramidInputFormat(job, metadata.getPyramid(), fromlevel,
        metadata.getTilesize(), providerProperties);

    final Bounds bounds = metadata.getBounds();

    // need to set up a empty dummy directory so hadoop can use the multiple output stuff, we're
    // using the adhoc provider because it provides the exact functionality for creating a temporary
    // directory
    AdHocDataProvider dummy = DataProviderFactory.createAdHocDataProvider(providerProperties);

    // mimic FileOutputFormat.setOutputPath(job, path);
    conf.set("mapred.output.dir", dummy.getResourceName());

    final AdHocDataProvider[] statsProviders = new AdHocDataProvider[fromlevel - tolevel];
    final MrsImageOutputFormatProvider[] outputProviders = new MrsImageOutputFormatProvider[fromlevel -
      tolevel];

    MrsImageDataProvider.setupMrsPyramidMultipleOutputJob(job);
    
    for (int level = tolevel; level < fromlevel; level++)
    {
      int lo = level - tolevel;
      
      statsProviders[lo] = DataProviderFactory.createAdHocDataProvider(providerProperties);

      conf.set(ADHOC_PROVIDER + level, statsProviders[lo].getResourceName());
      
      final String l = Integer.toString(level);
      outputProviders[lo] = MrsImageDataProvider.addMrsPyramidMultipleOutputFormat(
        job, metadata.getPyramid(), l, bounds, level, tilesize, metadata.getTileType(), metadata
          .getBands(), metadata.getProtectionLevel(), providerProperties);
    }
    HadoopUtils.setJar(job, BuildPyramidDriver.class);

    final boolean success = MapReduceUtils.runJob(job, progress, jobListener);

    // remove the dummy directory output
    dummy.delete();

    if (success)
    {
      // update the resampling method used to build pyramids
      metadata.setResamplingMethod(AggregatorRegistry.aggregatorRegistry.inverse().get(
          aggregator.getClass()));

      for (int level = tolevel; level < fromlevel; level++)
      {
        int lo = level - tolevel;

        outputProviders[lo].teardown(job);

        final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(new TMSUtils.Bounds(bounds.getMinX(),
          bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY()), level, tilesize);
        final LongRectangle b = new LongRectangle(tb.w, tb.s, tb.e, tb.n);

        final TMSUtils.Pixel psw = TMSUtils.latLonToPixels(bounds.getMinY(), bounds.getMinX(),
          level, tilesize);
        final TMSUtils.Pixel pne = TMSUtils.latLonToPixels(bounds.getMaxY(), bounds.getMaxX(),
          level, tilesize);

        // need to update the metadata
        metadata.setPixelBounds(level, new LongRectangle(0, 0, pne.px - psw.px, pne.py - psw.py));
        metadata.setTileBounds(level, b);
        metadata.setName(level);

        // update the pyramid level stats
        final ImageStats[] levelStats = ImageStats.readStats(statsProviders[lo]);
        statsProviders[lo].delete();
        metadata.setImageStats(level, levelStats);

        provider.getMetadataWriter().write();
      }

    }

    return success;
  }

}
