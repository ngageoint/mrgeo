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
 * Uses the sum of pixel values for the resampled pixel.
 * No data values are excluded.
 */
public class SumAggregator implements Aggregator
{

@Override
public double aggregate(double[] values, double nodata)
{
  double sum = 0;
  int count = 0;
  for (double value : values)
  {
    if (FloatUtils.isNotNodata(value, nodata))
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : sum;
}

@Override
public float aggregate(float[] values, float nodata)
{
  float sum = 0;
  int count = 0;
  for (float value : values)
  {
    if (FloatUtils.isNotNodata(value, nodata))
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : sum;
}

@Override
public byte aggregate(byte[] values, byte nodata)
{
  int sum = 0;
  int count = 0;
  for (byte value : values)
  {
    if (value != nodata)
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : (byte) sum;
}

@Override
public short aggregate(short[] values, short nodata)
{
  int sum = 0;
  int count = 0;
  for (short value : values)
  {
    if (value != nodata)
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : (short) sum;
}

@Override
public int aggregate(int[] values, int nodata)
{
  int sum = 0;
  int count = 0;
  for (int value : values)
  {
    if (value != nodata)
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : sum;
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
