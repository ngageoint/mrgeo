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

/**
 * 
 */
package org.mrgeo.aggregators;

import org.apache.commons.lang3.NotImplementedException;
import org.mrgeo.utils.FloatUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Uses the minimum average pixel value calculated for 
 * each row and column and diagonal pair.  Pairs with
 * a "no data" value will be excluded.
 */
public class MinAvgPairAggregator implements Aggregator
{

  @Override
  public double aggregate(double[] values, double nodata)
  {
    boolean data0 = FloatUtils.isNotNodata(values[0], nodata);
    boolean data1 = FloatUtils.isNotNodata(values[1], nodata);
    boolean data2 = FloatUtils.isNotNodata(values[2], nodata);
    boolean data3 = FloatUtils.isNotNodata(values[3], nodata);
    
    Collection<Double> averages = new ArrayList<Double>();
    if (data0 && data1)
      averages.add((values[0] + values[1]) / 2);
    if (data0 && data2)
      averages.add((values[0] + values[2]) / 2);
    if (data0 && data3)
      averages.add((values[0] + values[3]) / 2);
    if (data1 && data2)
      averages.add((values[1] + values[2]) / 2);
    if (data1 && data3)
      averages.add((values[1] + values[3]) / 2);
    if (data2 && data3)
      averages.add((values[2] + values[3]) / 2);
    
    return (averages.isEmpty()) ? nodata : Collections.min(averages);
  }

  @Override
  public float aggregate(float[] values, float nodata)
  {
    boolean data0 = FloatUtils.isNotNodata(values[0], nodata);
    boolean data1 = FloatUtils.isNotNodata(values[1], nodata);
    boolean data2 = FloatUtils.isNotNodata(values[2], nodata);
    boolean data3 = FloatUtils.isNotNodata(values[3], nodata);
    
    Collection<Float> averages = new ArrayList<Float>();
    if (data0 && data1)
      averages.add((values[0] + values[1]) / 2);
    if (data0 && data2)
      averages.add((values[0] + values[2]) / 2);
    if (data0 && data3)
      averages.add((values[0] + values[3]) / 2);
    if (data1 && data2)
      averages.add((values[1] + values[2]) / 2);
    if (data1 && data3)
      averages.add((values[1] + values[3]) / 2);
    if (data2 && data3)
      averages.add((values[2] + values[3]) / 2);
    
    return (averages.isEmpty()) ? nodata : Collections.min(averages);
  }

  @Override
  public int aggregate(int[] values, int nodata)
  {
    boolean data0 = values[0] != nodata;
    boolean data1 = values[1] != nodata;
    boolean data2 = values[2] != nodata;
    boolean data3 = values[3] != nodata;
    
    Collection<Integer> averages = new ArrayList<Integer>();
    if (data0 && data1)
      averages.add((values[0] + values[1]) / 2);
    if (data0 && data2)
      averages.add((values[0] + values[2]) / 2);
    if (data0 && data3)
      averages.add((values[0] + values[3]) / 2);
    if (data1 && data2)
      averages.add((values[1] + values[2]) / 2);
    if (data1 && data3)
      averages.add((values[1] + values[3]) / 2);
    if (data2 && data3)
      averages.add((values[2] + values[3]) / 2);
    
    return (averages.isEmpty()) ? nodata : Collections.min(averages);
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
