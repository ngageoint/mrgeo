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

package org.mrgeo.aggregators;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import javax.media.jai.RasterFactory;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("static-method")
public class NearestAggregatorTest
{
  private static double epsilon = 0.0000001;

  @Test
  @Category(UnitTest.class)
  public void testDouble()
  {
    double[] values = {0.21, 0.32, 0.43, 0.54};
    double nodata = Double.NaN;
    double result;
    Aggregator agg = new NearestAggregator();

    //Test normal case
    result = agg.aggregate(values, nodata);
    assertEquals(0.21, result, epsilon);

    //Test nodata cases
    values[0] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.32, result, epsilon);
 
    values[1] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.54, result, epsilon);
 
    values[3] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.43, result, epsilon);
 
    values[2] = nodata;
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
    Aggregator agg = new NearestAggregator();

    //Test normal case
    result = agg.aggregate(values, nodata);
    assertEquals(0.21, result, epsilon);

    //Test nodata cases
    values[0] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.32, result, epsilon);
 
    values[1] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.54, result, epsilon);
 
    values[3] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(0.43, result, epsilon);
 
    values[2] = nodata;
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
    Aggregator agg = new NearestAggregator();

    //Test normal case
    result = agg.aggregate(values, nodata);
    assertEquals(1, result);

    //Test nodata cases
    values[0] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(2, result);
 
    values[1] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(4, result);
 
    values[3] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(3, result);
 
    values[2] = nodata;
    result = agg.aggregate(values, nodata);
    assertEquals(nodata, result);
 
  }

  @Test
  @Category(UnitTest.class)
  public void testGetSamples()
  {
    int[] samples = new int[4];
    SampleModel sm = RasterFactory.createPixelInterleavedSampleModel(DataBuffer.TYPE_DOUBLE, 4, 4, 1);
    WritableRaster raster = RasterFactory.createWritableRaster(sm, null);
    WritableRaster parent = raster.createCompatibleWritableRaster();
    parent.setPixels(0, 0, 4, 4, new double[] {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4});
    parent.getSamples(1, 1, 2, 2, 0, samples);
    
    //verify the order of sample pixels is
    //---------
    //| 0 | 1 |
    //---------
    //| 2 | 3 |
    //---------

    assertEquals(1,samples[0]);
    assertEquals(2,samples[1]);
    assertEquals(3,samples[2]);
    assertEquals(4,samples[3]);

  }
}
