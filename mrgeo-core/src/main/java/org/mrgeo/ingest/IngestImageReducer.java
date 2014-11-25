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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.image.ImageStats;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;

public class IngestImageReducer extends
    Reducer<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable>
{
  @SuppressWarnings("unused")
  private static Logger log = LoggerFactory.getLogger(IngestImageReducer.class);

  private Counter totalTiles = null;
  private Counter duplicateTiles = null;
  private Counter storedTiles = null;

  private ImageStats[] stats; // stats computed on the reducer tiles for the image

  private int bands;
  private double[] nodata;
  
  @Override
  public void cleanup(final Context context) throws IOException
  {
    String adhoc = context.getConfiguration().get("stats.provider", null);
    if (adhoc == null)
    {
      throw new IOException("Stats provider not set");
      
    }
    AdHocDataProvider provider = DataProviderFactory.getAdHocDataProvider(adhoc,
        AccessMode.WRITE, context.getConfiguration());
    ImageStats.writeStats(provider, stats);
    
    //ImageStats.writeStatsToFile(context, stats);
  }

  @Override
  public void reduce(final TileIdWritable key, final Iterable<RasterWritable> it,
      final Context context) throws IOException, InterruptedException
  {
    WritableRaster tile = null;
    for (final RasterWritable raster : it)
    {
      totalTiles.increment(1);

      if (tile == null)
      {
        storedTiles.increment(1);
        tile = RasterUtils.makeRasterWritable(RasterWritable.toRaster(raster));
      }
      else
      {
        duplicateTiles.increment(1);
        mosaicTile(RasterWritable.toRaster(raster), tile);
      }
    }

    // compute stats on the tile and update reducer stats
    final ImageStats[] tileStats = ImageStats.computeStats(tile, nodata);
    stats = ImageStats.aggregateStats(Arrays.asList(stats, tileStats));

    final RasterWritable value = RasterWritable.toWritable(tile);

    context.write(key, value);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void setup(final Reducer.Context context)
  {
    totalTiles = context.getCounter("Ingest Reducer", "Reducer Tiles Processed");
    duplicateTiles = context.getCounter("Ingest Reducer", "Overlapping Tiles");
    storedTiles = context.getCounter("Ingest Reducer", "Stored Tiles");

    Configuration conf = context.getConfiguration();
    
    double nd = conf.getFloat("nodata", Float.NaN);
    bands = conf.getInt("bands", 1);

    nodata = new double[bands];
    for (int i = 0; i < bands; i++)
    {
      nodata[i] = nd;
    }
    // Initialize ImageStats array
    stats = ImageStats.initializeStatsArray(bands);
  }

  private void copyPixel(final int x, final int y, final int b, final Raster src,
      final WritableRaster dst)
  {
    switch (src.getTransferType())
    {
    case DataBuffer.TYPE_BYTE:
    {
      final byte p = (byte) src.getSample(x, y, b);
      if (p != (byte)nodata[b])
      {
        dst.setSample(x, y, b, p);
      }
      break;
    }
    case DataBuffer.TYPE_FLOAT:
    {
      final float p = src.getSampleFloat(x, y, b);
      if (!Float.isNaN(p) && p != (float)nodata[b])
      {
        dst.setSample(x, y, b, p);
      }

      break;
    }
    case DataBuffer.TYPE_DOUBLE:
    {
      final double p = src.getSampleDouble(x, y, b);
      if (!Double.isNaN(p) && p != nodata[b])
      {
        dst.setSample(x, y, b, p);
      }

      break;
    }
    case DataBuffer.TYPE_INT:
    {
      final int p = src.getSample(x, y, b);
      if (p != (int)nodata[b])
      {
        dst.setSample(x, y, b, p);
      }

      break;
    }
    case DataBuffer.TYPE_SHORT:
    {
      final short p = (short) src.getSample(x, y, b);
      if (p != (short)nodata[b])
      {
        dst.setSample(x, y, b, p);
      }

      break;
    }
    case DataBuffer.TYPE_USHORT:
    {
      final int p = src.getSample(x, y, b);
      if (p != (int)nodata[b])
      {
        dst.setSample(x, y, b, p);
      }

      break;
    }

    }
  }

  private void mosaicTile(final Raster src, final WritableRaster dst)
  {

    for (int y = 0; y < src.getHeight(); y++)
    {
      for (int x = 0; x < src.getWidth(); x++)
      {
        for (int b = 0; b < src.getNumBands(); b++)
        {
          copyPixel(x, y, b, src, dst);
        }
      }
    }
  }
}
