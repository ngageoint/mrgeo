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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.opimage.CropRasterOpImage;
import org.mrgeo.progress.Progress;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class FillRasterDriver
{
  private static final String STATS_PROVIDER = "stats.provider";

  // This fill steals extensively from the CropRasterMapOp
  public static class FillRasterMapper extends
    Mapper<TileIdWritable, TileCollection<Raster>, TileIdWritable, RasterWritable>
  {
    private double fill = Double.NaN;
    private double nodata = Double.NaN;
    private boolean isnodataNan = true;

    private boolean crop = false;
    private TMSUtils.Bounds cropBounds;
    private TMSUtils.TileBounds cropToTileBounds = null;
    TMSUtils.Pixel bottomLeftCropPixel = null;
    TMSUtils.Pixel topRightCropPixel = null;

    int zoom;

    private ImageStats[] stats = null;

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException
    {
      if (stats != null)
      {
        String adhoc = context.getConfiguration().get(STATS_PROVIDER, null);
        if (adhoc == null)
        {
          throw new IOException("Stats provider not set");
          
        }
        AdHocDataProvider statsProvider = DataProviderFactory.getAdHocDataProvider(adhoc,
            AccessMode.WRITE, context.getConfiguration());
        ImageStats.writeStats(statsProvider, stats);
      }
    }

    @Override
    public void map(final TileIdWritable key, final TileCollection<Raster> value, final Context context)
      throws IOException, InterruptedException
    {
      // There is only one raster input, so we can use a convenience method to
      // get the raster from the collection.
      final WritableRaster raster = RasterUtils.makeRasterWritable(value.get());

      if (crop)
      {
        if (cropToTileBounds == null)
        {
          calculateCrop(raster.getWidth());
        }

        final TMSUtils.Tile tile = TMSUtils.tileid(key.get(), zoom);
        final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, raster
          .getWidth());

        if (!cropBounds.contains(bounds) && cropBounds.intersect(bounds, false))
        {
          crop(raster, tile);
        }
        else
        {
          fill(raster);
        }
      }
      else
      {
        fill(raster);
      }

      context.write(key, RasterWritable.toWritable(raster));

      if (stats != null)
      {
        // compute stats on the tile and update aggregate stats
        final ImageStats[] tileStats = ImageStats.computeStats(raster, new double[] { Double.NaN });
        stats = ImageStats.aggregateStats(Arrays.asList(stats, tileStats));
      }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void setup(final Mapper.Context context)
    {
      final Configuration conf = context.getConfiguration();
      fill = conf.getFloat(FILL_VALUE, Float.NaN);
      if (Float.isNaN((float) fill))
      {
        fill = Double.NaN;
      }
      nodata = conf.getFloat(NODATA, Float.NaN);
      if (Float.isNaN((float) nodata))
      {
        nodata = Double.NaN;
      }

      isnodataNan = Double.isNaN(nodata);

//      String ft = conf.get(FILL_TYPE, "");
      crop = conf.get(FILL_TYPE, "").equals(CropRasterOpImage.EXACT);
      zoom = conf.getInt(ZOOM, 0);

      final Bounds b = Bounds.fromDelimitedString(conf
        .get(BOUNDS, Bounds.world.toDelimitedString()));
      cropBounds = TMSUtils.Bounds.convertOldToNewBounds(b);

      if (!conf.getBoolean("skip.stats", false))
      {
        stats = ImageStats.initializeStatsArray(1);
      }

    }

    private void calculateCrop(final int tilesize)
    {
      // Compute the pixel boundaries within which we want to crop the source image.
      TMSUtils.Pixel bottomRightWorldPixel = TMSUtils.latLonToPixelsUL(cropBounds.s, cropBounds.e,
        zoom, tilesize);

      // Because the world coordinates of a pixel are anchored at the
      // top-left corner of the pixel, if the bottom-right lat or lon are
      // exactly on a pixel boundary, then we need to exclude the last
      // pixel on the right and/or bottom of the crop area because
      // the actual area of the pixel is actually outside of the crop box.
      final TMSUtils.LatLon bottomRightAtPixelBoundary = TMSUtils.pixelToLatLonUL(
        bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize);

      if (Math.abs(bottomRightAtPixelBoundary.lat - cropBounds.n) < EPSILON)
      {
        bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px,
          bottomRightWorldPixel.py - 1);
      }

      if (Math.abs(bottomRightAtPixelBoundary.lon - cropBounds.e) < EPSILON)
      {
        bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px - 1,
          bottomRightWorldPixel.py);
      }

      final TMSUtils.LatLon bottomRightPt = TMSUtils.pixelToLatLonUL(bottomRightWorldPixel.px,
        bottomRightWorldPixel.py, zoom, tilesize);

      // DESIGN NOTE:
      // For efficiency in computeRect, we want to limit the amount of TMSUtils
      // computations are required while looping through the pixels of each
      // tile. As we process a tile, we know the tileX and tileY coordinates of
      // that tile, and of course we know the coordinates of the pixel within
      // that tile as we process them. Rather than converting each pixel to
      // worldwide coordinates, we compute the coordinates of the edge tiles as
      // well as the local pixel coordinates for where the image will crop and
      // use those values in computeRect to determine whether to copy the source
      // pixel or crop it (e.g. assign NoData to it). See the logic in
      // computeRect.

      // Compute the tile bounds for the actual crop area.
      final TMSUtils.Bounds b = new TMSUtils.Bounds(cropBounds.w, bottomRightPt.lat,
        bottomRightPt.lon, cropBounds.n);
      cropToTileBounds = TMSUtils.boundsToTile(b, zoom, tilesize);
      // Find the pixel coordinates within the edge tiles that correspond to
      // where the crop should occur. During processing, if the current tile
      // is one of the edge tiles, then we can use the pixel coordinates below
      // to determine which pixels should be copied from the source or set to
      // NoData, and it doesn't require converting each pixel to world
      // pixel coordinates to do so.
      topRightCropPixel = TMSUtils.latLonToTilePixelUL(cropBounds.n, bottomRightPt.lon,
        cropToTileBounds.e, cropToTileBounds.n, zoom, tilesize);
      bottomLeftCropPixel = TMSUtils.latLonToTilePixelUL(bottomRightPt.lat, cropBounds.w,
        cropToTileBounds.w, cropToTileBounds.s, zoom, tilesize);
    }

    private void crop(final WritableRaster raster, final TMSUtils.Tile tile)
    {
      int minCopyX = raster.getMinX();
      int maxCopyX = raster.getMinX() + raster.getWidth();
      int minCopyY = raster.getMinY();
      int maxCopyY = raster.getMinY() + raster.getHeight();

      boolean doCrop = false;

      // The following logic is dependent on the pixel coordinates
      // starting with 0,0 in the top-left of the tile.
      if (tile.tx == cropToTileBounds.e)
      {
        // Processing the right-most column of tiles.
        maxCopyX = (int) topRightCropPixel.px;
        doCrop = true;
      }

      if (tile.tx == cropToTileBounds.w)
      {
        // Processing the left-most column of tiles.
        minCopyX = (int) bottomLeftCropPixel.px;
        doCrop = true;
      }

      if (tile.ty == cropToTileBounds.n)
      {
        // Processing the top-most column of tiles.
        minCopyY = (int) topRightCropPixel.py;
        doCrop = true;
      }

      if (tile.ty == cropToTileBounds.s)
      {
        // Processing the bottom-most column of tiles.
        maxCopyY = (int) bottomLeftCropPixel.py;
        doCrop = true;
      }

      // Now that the pixel-based crop bounds are set up
      // if needed, we perform the crop if necessary.
      if (doCrop)
      {
        for (int y = raster.getMinY(); y < raster.getMinY() + raster.getHeight(); y++)
        {
          for (int x = raster.getMinX(); x < raster.getMinX() + raster.getWidth(); x++)
          {
            for (int b = 0; b < raster.getNumBands(); b++)
            {
              if (x < minCopyX || x > maxCopyX || y < minCopyY || y > maxCopyY)
              {
                raster.setSample(x, y, 0, nodata);
              }
              else
              {
                final double v = raster.getSampleDouble(x, y, b);

                if ((isnodataNan && Double.isNaN(v)) || v == nodata)
                {
                  raster.setSample(x, y, b, fill);
                }
              }
            }
          }
        }

      }
      else
      {
        fill(raster);
      }

    }

    private void fill(final WritableRaster raster)
    {
      for (int y = raster.getMinY(); y < raster.getMinY() + raster.getHeight(); y++)
      {
        for (int x = raster.getMinX(); x < raster.getMinX() + raster.getWidth(); x++)
        {
          for (int b = 0; b < raster.getNumBands(); b++)
          {
            final double v = raster.getSampleDouble(x, y, b);

            if ((isnodataNan && Double.isNaN(v)) || v == nodata)
            {
              raster.setSample(x, y, b, fill);
            }
          }
        }
      }
    }

  }

  public static String classname = FillRasterDriver.class.getSimpleName();
  public static String FILL_TYPE = classname + ".fillType";
  public static String FILL_VALUE = classname + ".fillValue";
  public static String NODATA = classname + ".nodata";
  public static String BOUNDS = classname + ".bounds";

  public static String ZOOM = classname + ".zoom";

  private static final double EPSILON = 1e-8;

  public FillRasterDriver()
  {

  }

  public static void run(final Job job, final MrsImagePyramid input, final String output,
    final double value, final String fillType, final Bounds bounds, final Progress progress,
    final JobListener jobListener, final String protectionLevel,
    final ProviderProperties providerProperties) throws IOException, JobFailedException, JobCancelledException
  {
    // create a new unique job name
    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    final String jobName = "FillRaster_" + now + "_" + UUID.randomUUID().toString();
    job.setJobName(jobName);

    MapReduceUtils.setupTiledJob(job);

    final Configuration conf = job.getConfiguration();

    HadoopUtils.setJar(job, FillRasterDriver.class);

    final MrsImagePyramidMetadata metadata = input.getMetadata();

    final int zoomlevel = metadata.getMaxZoomLevel();
    final int tilesize = metadata.getTilesize();
    final double nodata = metadata.getDefaultValue(0);

    // set some constants
    conf.set(FILL_TYPE, fillType);
    conf.setFloat(FILL_VALUE, (float) value);
    conf.set(BOUNDS, bounds.toDelimitedString());
    conf.setFloat(NODATA, (float) nodata);
    conf.setInt(ZOOM, zoomlevel);

    MrsImageDataProvider.setupMrsImagePyramidAllTilesSingleInputFormat(
        job, metadata.getPyramid(), zoomlevel, tilesize, bounds, value, providerProperties);

    job.setMapperClass(FillRasterMapper.class);
    
    job.setOutputKeyClass(TileIdWritable.class);
    job.setOutputValueClass(RasterWritable.class);

    HadoopUtils.setMetadata(job, metadata);

    final AdHocDataProvider statsProvider = DataProviderFactory.createAdHocDataProvider(
        providerProperties);
    // get the ad hoc provider set up for map/reduce
    statsProvider.setupJob(job);
    conf.set(STATS_PROVIDER, statsProvider.getResourceName());
    MrsImageOutputFormatProvider ofProvider = MrsImageDataProvider.setupMrsPyramidOutputFormat(
        job, output, bounds, zoomlevel, tilesize,
        metadata.getTileType(), metadata.getBands(), protectionLevel,
        providerProperties);

    if (MapReduceUtils.runJob(job, progress, jobListener))
    {
      ofProvider.teardown(job);
      
      // save the metadata
      MrsImagePyramid.calculateMetadataWithProvider(output, zoomlevel, ofProvider.getImageProvider(),
          statsProvider, metadata.getDefaultValues(), bounds, conf, protectionLevel,
          providerProperties);
    }
    statsProvider.delete();
  }

}
