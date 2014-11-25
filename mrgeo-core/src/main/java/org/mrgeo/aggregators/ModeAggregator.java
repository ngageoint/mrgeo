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

/**
 * 
 */
package org.mrgeo.aggregators;

import jj2000.j2k.NotImplementedError;

import java.util.HashMap;
import java.util.Map;

/**
 * Returns the mode of pixel values.
 * No data values will be excluded.
 */
public class ModeAggregator implements Aggregator
{

  @Override
  public double aggregate(double[] values, double nodata)
  {
    HashMap<Double,Integer> freqs = new HashMap<Double,Integer>();

    for (double val : values) {
      if (Double.compare(val, nodata) != 0)
      {
        Integer freq = freqs.get(val);
        freqs.put(val, (freq == null ? 1 : freq+1));
      }
    }

    double mode = nodata;
    int maxFreq = 0;

    for (Map.Entry<Double,Integer> entry : freqs.entrySet()) {
      int freq = entry.getValue();
      if (freq > maxFreq) {
        maxFreq = freq;
        mode = entry.getKey();
      }
    }

    return mode;
  }

  @Override
  public float aggregate(float[] values, float nodata)
  {
    HashMap<Float,Integer> freqs = new HashMap<Float,Integer>();

    for (float val : values) {
      if (Float.compare(val, nodata) != 0)
      {
        Integer freq = freqs.get(val);
        freqs.put(val, (freq == null ? 1 : freq+1));
      }
    }

    float mode = nodata;
    int maxFreq = 0;

    for (Map.Entry<Float,Integer> entry : freqs.entrySet()) {
      int freq = entry.getValue();
      if (freq > maxFreq) {
        maxFreq = freq;
        mode = entry.getKey();
      }
    }

    return mode;
  }

  @Override
  public int aggregate(int[] values, int nodata)
  {
    HashMap<Integer,Integer> freqs = new HashMap<Integer,Integer>();

    for (int val : values) {
      if (val != nodata)
      {
        Integer freq = freqs.get(val);
        freqs.put(val, (freq == null ? 1 : freq+1));
      }
    }

    int mode = nodata;
    int maxFreq = 0;

    for (Map.Entry<Integer,Integer> entry : freqs.entrySet()) {
      int freq = entry.getValue();
      if (freq > maxFreq) {
        maxFreq = freq;
        mode = entry.getKey();
      }
    }

    return mode;
  }

  @Override
  public double aggregate(double[][]values, double weightx, double weighty, double nodata)
  {
    throw new NotImplementedError("Not yet implemented");
  }
  
  @Override
  public float aggregate(float[][]values, double weightx, double weighty, float nodata)
  {
    throw new NotImplementedError("Not yet implemented");
  }

  @Override
  public int aggregate(final int[][] values, final double weightx, final double weighty, final int nodata)
  {
    throw new NotImplementedError("Not yet implemented");
  }

}
