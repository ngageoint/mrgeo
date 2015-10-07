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

/**
 * 
 */
package org.mrgeo.aggregators;

import org.apache.commons.lang3.NotImplementedException;

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
    for (int i=0; i<values.length; i++)
    {
      if (Double.compare(values[i], nodata) != 0)
      {
        sum += values[i];
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
    for (int i=0; i<values.length; i++)
    {
      if (Float.compare(values[i], nodata) != 0)
      {
        sum += values[i];
        count++;
      }
    }
    return (count == 0) ? nodata : sum;
  }

  @Override
  public int aggregate(int[] values, int nodata)
  {
    int sum = 0;
    int count = 0;
    for (int i=0; i<values.length; i++)
    {
      if (values[i] != nodata)
      {
        sum += values[i];
        count++;
      }
    }
    return (count == 0) ? nodata : sum;
  }

  @Override
  public double aggregate(double[][]values, double weightx, double weighty, double nodata)
  {
    throw new NotImplementedException("Not yet implemented");
  }
  
  @Override
  public float aggregate(float[][]values, double weightx, double weighty, float nodata)
  {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public int aggregate(final int[][] values, final double weightx, final double weighty, final int nodata)
  {
    throw new NotImplementedException("Not yet implemented");
  }

}
