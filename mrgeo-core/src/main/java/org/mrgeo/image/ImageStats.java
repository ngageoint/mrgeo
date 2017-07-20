/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.image;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterWritable.RasterWritableException;
import org.mrgeo.utils.FloatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;

public class ImageStats implements Cloneable, Serializable
{
// tile id that is not valid for TMS and represents stats output from an OpChain mapper
public static final long STATS_TILE_ID = -13;
// count of image statistics measures, currently min, max, sum & count
public static final int STATS_COUNT = 4;
private static final long serialVersionUID = 1L;
@SuppressWarnings("unused")
private static Logger log = LoggerFactory.getLogger(ImageStats.class);
public double min, max, mean, sum;
public long count;

// TODO: Add minx, maxx, miny, maxy
public ImageStats()
{
}

public ImageStats(double min, double max)
{
  this.min = min;
  this.max = max;
}

public ImageStats(double min, double max, double sum, long count)
{
  this.min = min;
  this.max = max;
  this.sum = sum;
  this.count = count;
  mean = sum / count;
}

/**
 * Aggregates statistics output from multiple tasks
 *
 * @param listOfStats the stats arrays to aggregate
 * @return an array of ImageStats objects
 */
static public ImageStats[] aggregateStats(List<ImageStats[]> listOfStats)
{
  ImageStats[] stats;
  if (listOfStats.isEmpty())
  {
    stats = initializeStatsArray(0);
  }
  else
  {
    // aggregate stats from each mapper or reducer
    int bands = listOfStats.get(0).length;
    stats = initializeStatsArray(bands);
    for (ImageStats[] s : listOfStats)
    {
      for (int i = 0; i < s.length; i++)
      {
        stats[i].min = Math.min(stats[i].min, s[i].min);
        stats[i].max = Math.max(stats[i].max, s[i].max);
        stats[i].sum += s[i].sum;
        stats[i].count += s[i].count;
      }
    }
    // update metadata
    for (int i = 0; i < bands; i++)
    {
      stats[i].mean = stats[i].sum / stats[i].count;
    }

  }
  return stats;
}

/**
 * Computes pixel value statistics: min, max, sum, count, & mean for a Raster and returns an array
 * of ImageStats objects, one for each band in the image.
 *
 * @param raster the raster to compute stats for
 * @param nodata the value to ignore
 */
static public void computeAndUpdateStats(ImageStats[] tileStats, MrGeoRaster raster,
    double[] nodata)
{
  double sample;
  for (int y = 0; y < raster.height(); y++)
  {
    for (int x = 0; x < raster.width(); x++)
    {
      for (int b = 0; b < raster.bands(); b++)
      {
        sample = raster.getPixelDouble(x, y, b);

        // throw out NaN samples
        if (FloatUtils.isNotNodata(sample, nodata[b]))
        {
          tileStats[b].min = Math.min(sample, tileStats[b].min);
          tileStats[b].max = Math.max(sample, tileStats[b].max);
          tileStats[b].sum += sample;
          tileStats[b].count++;
          tileStats[b].mean = tileStats[b].sum / tileStats[b].count;
        }
      }
    }
  }

}

/**
 * Computes pixel value statistics: min, max, sum, count, & mean for a Raster and returns an array
 * of ImageStats objects, one for each band in the image.
 *
 * @param raster the raster to compute stats for
 * @param nodata the value to ignore
 * @return an array of ImageStats objects
 */
static public ImageStats[] computeStats(MrGeoRaster raster, double[] nodata)
    throws RasterWritableException
{
  ImageStats[] tileStats = initializeStatsArray(raster.bands());
  computeAndUpdateStats(tileStats, raster, nodata);
  return tileStats;
}

/**
 * Initializes an array of ImageStats objects, one for each band in the image, and initializes the
 * properties min, max, sum, count & mean to appropriate values.
 *
 * @param bands the number of bands in the image
 * @return an array of ImageStats objects
 */
static public ImageStats[] initializeStatsArray(int bands)
{
  ImageStats[] stats = new ImageStats[bands];
  for (int i = 0; i < bands; i++)
  {
    stats[i] = new ImageStats(Double.MAX_VALUE, -Double.MAX_VALUE);
    stats[i].sum = 0;
    stats[i].count = 0;
    stats[i].mean = 0;
  }
  return stats;
}


@SuppressWarnings("squid:S1166") // Exception caught and handled
static public void writeStats(AdHocDataProvider provider, ImageStats[] stats)
    throws IOException
{
  OutputStream stream = null;

  try
  {
    stream = provider.add();

    ObjectMapper mapper = new ObjectMapper();
    try
    {
      mapper.writerWithDefaultPrettyPrinter().writeValue(stream, stats);
    }
    catch (NoSuchMethodError e)
    {
      // if we don't have the pretty printer, just write the json
      mapper.writeValue(stream, stats);
    }
  }
  finally
  {
    if (stream != null)
    {
      stream.close();
    }
  }
}


@Override
@SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "No super.clone() to call")
public Object clone()
{
  ImageStats s = new ImageStats(min, max, sum, count);
  s.mean = sum / count;
  return s;
}

@Override
public boolean equals(Object obj)
{
  if (obj == null)
  {
    return false;
  }
  if (obj == this)
  {
    return true;
  }
  if (obj.getClass() != getClass())
  {
    return false;
  }

  ImageStats rhs = (ImageStats) obj;
  return new EqualsBuilder()
      .
      // if deriving: appendSuper(super.equals(obj)).
          append(min, rhs.min).append(max, rhs.max).append(sum, rhs.sum).append(count, rhs.count)
      .isEquals();
}

@Override
public int hashCode()
{
  return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
      // if deriving: appendSuper(super.hashCode()).
          append(min).append(max).append(sum).append(count).toHashCode();
}

}
