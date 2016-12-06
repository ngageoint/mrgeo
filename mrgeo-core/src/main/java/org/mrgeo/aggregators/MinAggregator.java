/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.aggregators;

import org.apache.commons.lang3.NotImplementedException;
import org.mrgeo.utils.FloatUtils;

/**
 * Uses the minimum pixel value for the resampled pixel.
 * No data values are excluded.
 */
public class MinAggregator implements Aggregator
{

@Override
public double aggregate(double[] values, double nodata)
{
  double min = Double.MAX_VALUE;
  for (double value : values)
  {
    if (FloatUtils.isNotNodata(value, nodata))
    {
      min = Math.min(min, value);
    }
  }
  return FloatUtils.isEqual(min, Double.MAX_VALUE) ? nodata : min;
}

@Override
public float aggregate(float[] values, float nodata)
{
  Float min = Float.MAX_VALUE;
  for (float value : values)
  {
    if (FloatUtils.isNotNodata(value, nodata))
    {
      min = Math.min(min, value);
    }
  }
  return FloatUtils.isEqual(min, Float.MAX_VALUE) ? nodata : min;
}

@Override
public int aggregate(int[] values, int nodata)
{
  int min = Integer.MAX_VALUE;
  for (int value : values)
  {
    if (value != nodata)
    {
      min = Math.min(min, value);
    }
  }
  return (min == Integer.MAX_VALUE) ? nodata : min;
}

@Override
public short aggregate(short[] values, short nodata)
{
  short min = Short.MAX_VALUE;
  for (short value : values)
  {
    if (value != nodata)
    {
      min = (short) Math.min(min, value);
    }
  }
  return (min == Short.MAX_VALUE) ? nodata : min;
}

@Override
public byte aggregate(byte[] values, byte nodata)
{
  byte min = Byte.MAX_VALUE;
  for (byte value : values)
  {
    if (value != nodata)
    {
      min = (byte) Math.min(min, value);
    }
  }
  return (min == Byte.MAX_VALUE) ? nodata : min;
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
