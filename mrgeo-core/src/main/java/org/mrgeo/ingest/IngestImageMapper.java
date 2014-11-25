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
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;


public class IngestImageMapper extends Mapper<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable>
{
  private static Logger log = LoggerFactory.getLogger(IngestImageMapper.class);

  private Counter tileCounter = null;

  private MrsImagePyramidMetadata metadata = null;

  Number nodata = 0;

  enum Direction {
    TOP,
    BOTTOM,
    LEFT,
    RIGHT
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void setup(Mapper.Context context) 
  {
    tileCounter = context.getCounter("Ingest Mapper", "Mapper Tiles Processed");

    Configuration conf = context.getConfiguration();

    metadata = new MrsImagePyramidMetadata();

    // these should have been verified before here. but we'll keep the check, just in case
    int zoomlevel = conf.getInt("zoomlevel", -1);
    if (zoomlevel < 0)
    {
      throw new MrsImageException(
        "Error, no \"zoomlevel\" parameter in configuration, zoomlevel needs to be calculated & set before map/reduce");
    }
    metadata.setMaxZoomLevel(zoomlevel);

    int tilesize = conf.getInt("tilesize", -1);
    if (tilesize < 0)
    {
      throw new MrsImageException(
        "Error, no \"tilesize\" parameter in configuration, tilesize needs to be calculated & set before map/reduce");
    }
    metadata.setTilesize(tilesize);

    String cl = conf.get("classification", null);
    if (cl == null)
    {
      throw new MrsImageException(
        "Error, no \"classification\" parameter in configuration, classification needs to be calculated & set before map/reduce");
    }
    metadata.setClassification(Classification.valueOf(cl));

    String nd = conf.get("nodata", null);
    if (nd == null)
    {
      throw new MrsImageException(
        "Error, no \"nodata\" parameter in configuration, nodata needs to be calculated & set before map/reduce");
    }
    nodata = Double.parseDouble(nd);

    metadata.setBounds(new Bounds());

    //  Don't need to calculate these...
    //      metadata.setImage(zoomlevel);
    //      metadata.setPixelBounds(zoomlevel, pixelBounds);
    //      metadata.setPyramid(pyramid);
    //      metadata.setResamplingMethod(resamplingMethod);
    //      metadata.setTileBounds(zoomlevel, tileBounds);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException
  {
    if (metadata != null)
    {
      // save the (partial) metadata with the adhoc provider.
      String adhoc = context.getConfiguration().get("metadata.provider", null);
      if (adhoc == null)
      {
        throw new IOException("Metadata provider not set");
        
      }
      AdHocDataProvider provider = DataProviderFactory.getAdHocDataProvider(adhoc,
          AccessMode.WRITE, context.getConfiguration());
      
      OutputStream os = provider.add();
      metadata.save(os);
      os.close();
    }
  }

  @Override
  public void map(TileIdWritable key, RasterWritable value, Context context) throws IOException, InterruptedException
  {

    if (log.isDebugEnabled())
    {
      long zerocnt = 0;
      long nonzerocnt = 0;

      byte[] data = value.get();
      byte headerlen = data[3];

      for (int i = ((headerlen + 1) * 4); i < data.length; i++)
      {
        if (data[i] == 0)
        {
          zerocnt++;
        }
        else
        {
          nonzerocnt++;
        }
      }

      TMSUtils.Tile t = TMSUtils.tileid(key.get(), 10);
      log.debug("key: " + key.get() + " (" + t.tx + "," +  t.ty + ")");
      log.debug("data stats:  nonzero bytes: " + nonzerocnt + " zero bytes: " + zerocnt);
    }

    // are we calculating metadata here?
    if (metadata != null)
    {
      Raster raster = RasterWritable.toRaster(value);

      if (metadata.getBands() <= 0)
      {
        metadata.setBands(raster.getNumBands());

        double[] defaults = new double[raster.getNumBands()];
        Arrays.fill(defaults, nodata.doubleValue());
        metadata.setDefaultValues(defaults);

        metadata.setTileType(raster.getTransferType());
      }

      // need to expand the bounds by the tile, but we also need to crop based on the nodata
      // values...
      TMSUtils.Tile tile = TMSUtils.tileid(key.get(), metadata.getMaxZoomLevel());
      TMSUtils.Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, metadata.getMaxZoomLevel(), metadata.getTilesize());

      if (!metadata.getBounds().isValid())
      {
        // in this case, order is important! (B, T, R, L)
        tb = cropToData(Direction.BOTTOM, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
        tb = cropToData(Direction.TOP, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
        tb = cropToData(Direction.RIGHT, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
        tb = cropToData(Direction.LEFT, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());

        metadata.setBounds(new Bounds(tb.w, tb.s, tb.e, tb.n));

      }
      else
      {
        Bounds bounds = metadata.getBounds();


        boolean expand = false;
        // in this case, order is important! (B, T, R, L)
        if (tb.s < bounds.getMinY())
        {
          tb = cropToData(Direction.BOTTOM, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
          expand = true;
        }
        if (tb.n > bounds.getMaxY())
        {
          tb = cropToData(Direction.TOP, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
          expand = true;
        }
        if (tb.e > bounds.getMaxX())
        {
          tb = cropToData(Direction.RIGHT, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
          expand = true;
        }
        if (tb.w < bounds.getMinX())
        {
          tb = cropToData(Direction.LEFT, raster, tb, metadata.getDefaultValuesDouble(), metadata.getMaxZoomLevel(), metadata.getTilesize());
          expand = true;
        }

        if (expand)
        {
          bounds.expand(tb.w, tb.s, tb.e, tb.n);
        }
      }
    }


    tileCounter.increment(1);
    context.write(key, value);
  }

  private static TMSUtils.Bounds cropToData(Direction direction, Raster raster, TMSUtils.Bounds bounds, double[] nodata, int zoom, int tilesize)
  {
    TMSUtils.Pixel ll = TMSUtils.latLonToPixels(bounds.s, bounds.w, zoom, tilesize);
    TMSUtils.Pixel ur = TMSUtils.latLonToPixels(bounds.n, bounds.e, zoom, tilesize);

    int x, y;
    boolean stop = false;

    switch (direction)
    {
    case BOTTOM:
      for (y = raster.getHeight() - 1; y >= 0 && !stop; y--)
      {
        for (x = 0; x < raster.getWidth() && !stop; x++)
        {
          for (int b = 0; b < raster.getNumBands() && !stop; b++)
          {
            double v = raster.getSampleDouble(x, y, b);

            if (Double.isNaN(nodata[b]))
            {
              if (!Double.isNaN(v))
              {
                stop = true;
                break;
              }
            }
            else if (nodata[b] != v)
            {
              stop = true;
              break;
            }
          }
        }
      }

      ll = new TMSUtils.Pixel(ll.px, ur.py - y + 2);

      break;
    case LEFT:
      for (x = 0; x < raster.getWidth() && !stop; x++)
      {
        for (y = 0; y < raster.getHeight() && !stop; y++)
        {
          for (int b = 0; b < raster.getNumBands() && !stop; b++)
          {
            double v = raster.getSampleDouble(x, y, b);

            if (Double.isNaN(nodata[b]))
            {
              if (!Double.isNaN(v))
              {
                stop = true;
                break;
              }
            }
            else if (nodata[b] != v)
            {
              stop = true;
              break;
            }
          }
        }
      }
      ll = new TMSUtils.Pixel(ll.px + x, ll.py);

      break;
    case RIGHT:
      for (x = raster.getWidth() - 1; x >= 0 && !stop; x--)
      {
        for (y = 0; y < raster.getHeight() && !stop; y++)
        {
          for (int b = 0; b < raster.getNumBands() && !stop; b++)
          {
            double v = raster.getSampleDouble(x, y, b);

            if (Double.isNaN(nodata[b]))
            {
              if (!Double.isNaN(v))
              {
                stop = true;
                break;
              }
            }
            else if (nodata[b] != v)
            {
              stop = true;
              break;
            }
          }
        }
      }

      ur = new TMSUtils.Pixel(ll.px + x + 2, ur.py);

      break;
    case TOP:
      for (y = 0; y < raster.getHeight() && !stop; y++)
      {
        for (x = 0; x < raster.getWidth() && !stop; x++)
        {
          for (int b = 0; b < raster.getNumBands() && !stop; b++)
          {
            double v = raster.getSampleDouble(x, y, b);

            if (Double.isNaN(nodata[b]))
            {
              if (!Double.isNaN(v))
              {
                stop = true;
                break;
              }
            }
            else if (nodata[b] != v)
            {
              stop = true;
              break;
            }
          }
        }
      }

      ur = new TMSUtils.Pixel(ur.px, ur.py - y + 1);

      break;
    default:
      break;
    }

    TMSUtils.LatLon llll = TMSUtils.pixelToLatLon(ll.px, ll.py, zoom, tilesize);
    TMSUtils.LatLon urll = TMSUtils.pixelToLatLon(ur.px, ur.py, zoom, tilesize);
    return new TMSUtils.Bounds(llll.lon, llll.lat, urll.lon, urll.lat);
  }
}
