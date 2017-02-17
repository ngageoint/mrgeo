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

/**
 *
 */
package org.mrgeo.aggregators;


import org.apache.commons.lang3.NotImplementedException;
import org.mrgeo.utils.FloatUtils;

/**
 * Use a preference model to assign the destination pixel with a neighboring
 * source pixel.
 * ---------
 * | 0 | 1 |
 * ---------
 * | 3 | 2 |
 * ---------
 * No data values will be excluded.
 */
public class NearestAggregator implements Aggregator
{

@Override
public double aggregate(double[] values, double nodata)
{
  boolean data0 = FloatUtils.isNotNodata(values[0], nodata);
  boolean data1 = FloatUtils.isNotNodata(values[1], nodata);
  boolean data2 = FloatUtils.isNotNodata(values[2], nodata);
  boolean data3 = FloatUtils.isNotNodata(values[3], nodata);

  if (data0)
  {
    return values[0];
  }
  if (data1)
  {
    return values[1];
  }
  if (data3)
  {
    return values[3];
  }
  if (data2)
  {
    return values[2];
  }

  return nodata;
}

@Override
public float aggregate(float[] values, float nodata)
{
  boolean data0 = FloatUtils.isNotNodata(values[0], nodata);
  boolean data1 = FloatUtils.isNotNodata(values[1], nodata);
  boolean data2 = FloatUtils.isNotNodata(values[2], nodata);
  boolean data3 = FloatUtils.isNotNodata(values[3], nodata);

  if (data0)
  {
    return values[0];
  }
  if (data1)
  {
    return values[1];
  }
  if (data3)
  {
    return values[3];
  }
  if (data2)
  {
    return values[2];
  }

  return nodata;
}

@Override
public int aggregate(int[] values, int nodata)
{
  boolean data0 = values[0] != nodata;
  boolean data1 = values[1] != nodata;
  boolean data2 = values[2] != nodata;
  boolean data3 = values[3] != nodata;

  if (data0)
  {
    return values[0];
  }
  if (data1)
  {
    return values[1];
  }
  if (data3)
  {
    return values[3];
  }
  if (data2)
  {
    return values[2];
  }

  return nodata;
}

@Override
public short aggregate(short[] values, short nodata)
{
  boolean data0 = values[0] != nodata;
  boolean data1 = values[1] != nodata;
  boolean data2 = values[2] != nodata;
  boolean data3 = values[3] != nodata;

  if (data0)
  {
    return values[0];
  }
  if (data1)
  {
    return values[1];
  }
  if (data3)
  {
    return values[3];
  }
  if (data2)
  {
    return values[2];
  }

  return nodata;
}

@Override
public byte aggregate(byte[] values, byte nodata)
{
  boolean data0 = values[0] != nodata;
  boolean data1 = values[1] != nodata;
  boolean data2 = values[2] != nodata;
  boolean data3 = values[3] != nodata;

  if (data0)
  {
    return values[0];
  }
  if (data1)
  {
    return values[1];
  }
  if (data3)
  {
    return values[3];
  }
  if (data2)
  {
    return values[2];
  }

  return nodata;
}

@Override
public double aggregate(double[][] values, double weightx, double weighty, double nodata)
{
  throw new NotImplementedException("Not yet implemented");
}

@Override
public float aggregate(float[][] values, double weightx, double weighty, float nodata)
{
  throw new NotImplementedException("Not yet implemented");
}

@Override
public byte aggregate(byte[][] values, double weightx, double weighty, byte nodata)
{
  throw new NotImplementedException("Not yet implemented");
}

@Override
public short aggregate(short[][] values, double weightx, double weighty, short nodata)
{
  throw new NotImplementedException("Not yet implemented");
}

@Override
public int aggregate(final int[][] values, final double weightx, final double weighty, final int nodata)
{
  throw new NotImplementedException("Not yet implemented");
}

}
