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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("all") // test code, not included in production
public class SumAggregatorTest
{
  private static double epsilon = 0.0000001;

  @Test
  @Category(UnitTest.class)
  public void testDouble()
  {
    double[] values = {0.21, 0.32, 0.43, 0.54};
    double nodata = Double.NaN;
    double result;
    Aggregator agg = new SumAggregator();

    //Test normal case
    result = agg.aggregate(values, nodata);
    assertEquals(1.50, result, epsilon);

    //Test nodata cases
    values[0] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(1.29, result, epsilon);
 
    values[1] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.97, result, epsilon);
 
    values[2] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.54, result, epsilon);
 
    values[3] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(nodata, result, epsilon);
 
  }

  @Test
  @Category(UnitTest.class)
  public void testFloat()
  {
    float[] values = {0.21f, 0.32f, 0.43f, 0.54f};
    float nodata = -9999.0f;
    float result;
    Aggregator agg = new SumAggregator();

    //Test normal case
    result = agg.aggregate(values, nodata);
    assertEquals(1.50, result, epsilon);

    //Test nodata cases
    values[0] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(1.29, result, epsilon);
 
    values[1] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.97, result, epsilon);
 
    values[2] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.54, result, epsilon);
 
    values[3] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(nodata, result, epsilon);
 
  }

  @Test
  @Category(UnitTest.class)
  public void testInt()
  {
    int[] values = {1, 2, 3, 4};
    int nodata = -9999;
    int result;
    Aggregator agg = new SumAggregator();

    //Test normal case
    result = agg.aggregate(values, nodata);
    assertEquals(10, result);

    //Test nodata cases
    values[0] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(9, result);
 
    values[1] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(7, result);
 
    values[2] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(4, result);
 
    values[3] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(nodata, result);
 
  }

  @Test
  @Category(UnitTest.class)
  public void testIntWithZero()
  {
    int nodata = -9999;
    int result;
    Aggregator agg = new SumAggregator();

    int[] values = {0, -9999, 3, 4};
    result = agg.aggregate(values, nodata);
    assertEquals(7, result);

    int[] values2 = {0, -9999, 0, -9999};
    result = agg.aggregate(values2, nodata);
    assertEquals(0, result);

 
  }

}
