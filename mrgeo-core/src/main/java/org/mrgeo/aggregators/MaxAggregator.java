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

package org.mrgeo.aggregators;

import org.apache.commons.lang3.NotImplementedException;
import org.mrgeo.utils.FloatUtils;

/**
 * Uses the maximum pixel value for the resampled pixel.
 * No data values are excluded.
 */
public class MaxAggregator implements Aggregator
{

@Override
public double aggregate(double[] values, double nodata)
{
  double max = -Double.MAX_VALUE;
  for (double value : values)
  {
    if (FloatUtils.isNotNodata(value, nodata))
    {
      max = Math.max(max, value);
    }
  }
  return FloatUtils.isEqual(max, -Double.MAX_VALUE) ? nodata : max;
}

@Override
public float aggregate(float[] values, float nodata)
{
  Float max = -Float.MAX_VALUE;
  for (float value : values)
  {
    if (FloatUtils.isNotNodata(value, nodata))
    {
      max = Math.max(max, value);
    }
  }
  return FloatUtils.isEqual(max, -Float.MAX_VALUE) ? nodata : max;
}

@Override
public int aggregate(int[] values, int nodata)
{
  int max = -Integer.MAX_VALUE;
  for (int value : values)
  {
    if (value != nodata)
    {
      max = Math.max(max, value);
    }
  }
  return (max == -Integer.MAX_VALUE) ? nodata : max;
}

@Override
public short aggregate(short[] values, short nodata)
{
  short max = -Short.MAX_VALUE;
  for (short value : values)
  {
    if (value != nodata)
    {
      max = (short) Math.max(max, value);
    }
  }
  return (max == -Short.MAX_VALUE) ? nodata : max;
}

@Override
public byte aggregate(byte[] values, byte nodata)
{
  byte max = -Byte.MAX_VALUE;
  for (byte value : values)
  {
    if (value != nodata)
    {
      max = (byte) Math.max(max, value);
    }
  }
  return (max == -Byte.MAX_VALUE) ? nodata : max;
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
public int aggregate(int[][] values, double weightx, double weighty, int nodata)
{
  throw new NotImplementedException("Not yet implemented");
}


}
