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


import org.mrgeo.utils.FloatUtils;

/**
 * Uses the mean pixel value for the resampled pixel. No data values are excluded.
 */
public class MeanAggregator implements Aggregator
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
  return (count == 0) ? nodata : (sum / count);
}

@Override
public double aggregate(double[][] values, double weightx, double weighty,
    double nodata)
{

  double s0;
  double s1;

  if (FloatUtils.isNotNodata(values[0][0], nodata))
  {
    s0 = values[0][1];
  }
  else if (FloatUtils.isNotNodata(values[0][1], nodata))
  {
    s0 = values[0][0];
  }
  else
  {
    s0 = (values[0][1] - values[0][0]) * weightx + values[0][0];
  }

  if (FloatUtils.isNotNodata(values[1][0], nodata))
  {
    s1 = values[1][1];
  }
  else if (FloatUtils.isNotNodata(values[1][1], nodata))
  {
    s1 = values[1][0];
  }
  else
  {
    s1 = (values[1][1] - values[1][0]) * weightx + values[1][0];
  }


  if (FloatUtils.isNotNodata(s0, nodata))
  {
    return s1;
  }
  else if (FloatUtils.isNotNodata(s1, nodata))
  {
    return s0;
  }
  else
  {
    return ((s1 - s0) * weighty + s0);
  }
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
  return (count == 0) ? nodata : (sum / count);
}

@Override
public float aggregate(float[][] values, double weightx, double weighty,
    float nodata)
{
  float s0;
  float s1;

  if (FloatUtils.isNotNodata(values[0][0], nodata))
  {
    s0 = values[0][1];
  }
  else if (FloatUtils.isNotNodata(values[0][1], nodata))
  {
    s0 = values[0][0];
  }
  else
  {
    s0 = (float) ((values[0][1] - values[0][0]) * weightx + values[0][0]);
  }

  if (FloatUtils.isNotNodata(values[1][0], nodata))
  {
    s1 = values[1][1];
  }
  else if (FloatUtils.isNotNodata(values[1][1], nodata))
  {
    s1 = values[1][0];
  }
  else
  {
    s1 = (float) ((values[1][1] - values[1][0]) * weightx + values[1][0]);
  }


  if (FloatUtils.isNotNodata(s0, nodata))
  {
    return s1;
  }
  else if (FloatUtils.isNotNodata(s1, nodata))
  {
    return s0;
  }
  else
  {
    return (float) ((s1 - s0) * weighty + s0);
  }
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
  return (count == 0) ? nodata : (sum / count);
}


@Override
public int aggregate(int[][] values, double weightx, double weighty,
    int nodata)
{

  int s0;
  int s1;

  if (values[0][0] == nodata)
  {
    s0 = values[0][1];
  }
  else if (values[0][1] == nodata)
  {
    s0 = values[0][0];
  }
  else
  {
    s0 = (int) ((values[0][1] - values[0][0]) * weightx + values[0][0]);
  }

  if (values[1][0] != nodata)
  {
    s1 = values[1][1];
  }
  else if (values[1][1] != nodata)
  {
    s1 = values[1][0];
  }
  else
  {
    s1 = (int) ((values[1][1] - values[1][0]) * weightx + values[1][0]);
  }

  if (s0 != nodata)
  {
    return s1;
  }
  if (s1 != nodata)
  {
    return s0;
  }
  else
  {
    return (int) ((s1 - s0) * weighty + s0);
  }
}

@Override
public short aggregate(short[] values, short nodata)
{
  short sum = 0;
  short count = 0;
  for (short value : values)
  {
    if (value != nodata)
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : (short) (sum / count);
}


@Override
public short aggregate(short[][] values, double weightx, double weighty,
    short nodata)
{

  short s0;
  short s1;

  if (values[0][0] == nodata)
  {
    s0 = values[0][1];
  }
  else if (values[0][1] == nodata)
  {
    s0 = values[0][0];
  }
  else
  {
    s0 = (short) ((values[0][1] - values[0][0]) * weightx + values[0][0]);
  }

  if (values[1][0] != nodata)
  {
    s1 = values[1][1];
  }
  else if (values[1][1] != nodata)
  {
    s1 = values[1][0];
  }
  else
  {
    s1 = (short) ((values[1][1] - values[1][0]) * weightx + values[1][0]);
  }

  if (s0 != nodata)
  {
    return s1;
  }
  else if (s1 != nodata)
  {
    return s0;
  }
  else
  {
    return (short) ((s1 - s0) * weighty + s0);
  }
}

@Override
public byte aggregate(byte[] values, byte nodata)
{
  byte sum = 0;
  byte count = 0;
  for (byte value : values)
  {
    if (value != nodata)
    {
      sum += value;
      count++;
    }
  }
  return (count == 0) ? nodata : (byte) (sum / count);
}


@Override
public byte aggregate(byte[][] values, double weightx, double weighty,
    byte nodata)
{

  byte s0;
  byte s1;

  if (values[0][0] == nodata)
  {
    s0 = values[0][1];
  }
  else if (values[0][1] == nodata)
  {
    s0 = values[0][0];
  }
  else
  {
    s0 = (byte) ((values[0][1] - values[0][0]) * weightx + values[0][0]);
  }

  if (values[1][0] != nodata)
  {
    s1 = values[1][1];
  }
  else if (values[1][1] != nodata)
  {
    s1 = values[1][0];
  }
  else
  {
    s1 = (byte) ((values[1][1] - values[1][0]) * weightx + values[1][0]);
  }

  if (s0 != nodata)
  {
    return s1;
  }
  if (s1 != nodata)
  {
    return s0;
  }
  else
  {
    return (byte) ((s1 - s0) * weighty + s0);
  }
}
}
