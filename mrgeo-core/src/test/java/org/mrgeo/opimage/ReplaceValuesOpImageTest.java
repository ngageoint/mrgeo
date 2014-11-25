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

package org.mrgeo.opimage;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.junit.UnitTest;

import javax.media.jai.RenderedOp;
import java.awt.*;
import java.awt.image.*;

@SuppressWarnings("static-method")
public class ReplaceValuesOpImageTest
{
  private static final double EPSILON = 1e-8;

  // In nodata rasters, set the value of each nth pixel to NODATA
  private static int noDataModValue = 7;
  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static SampleModel sm;
  private static int width;
  private static int height;
  private static WritableRaster numbered;
  private static WritableRaster numberedWithNoData;
  private static WritableRaster numberedWithNanNoData;

  @BeforeClass
  public static void init()
  {
    width = 10;
    height = 10;
    sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    numbered = Raster.createWritableRaster(sm, new Point(0, 0));
    numberedWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    numberedWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        int pixelId = getPixelId(x, y, width);
        numbered.setSample(x, y, 0, (double)pixelId);
        numberedWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : (double)pixelId);
        numberedWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : (double)pixelId);
      }
    }
  }

  @Before
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  private static int getPixelId(int x, int y, int w)
  {
    return x + (y * w);
  }

  private void runTestWithNoData(WritableRaster source, double sourceNoData,
      double newValue, boolean newIsNull,
      double min, double max)
  {
    ColorModel cm1 = RasterUtils.createColorModel(source);
    java.util.Hashtable<String, Object> arg1Properties = new java.util.Hashtable<String, Object>();
    arg1Properties.put(OpImageUtils.NODATA_PROPERTY, sourceNoData);
    BufferedImage s1 = new BufferedImage(cm1, source, false, arg1Properties);
    RenderedOp op = ReplaceValuesDescriptor.create(s1, newValue, newIsNull, min, max, null);
    Raster r = op.getData();
    if (newIsNull)
    {
      Assert.assertEquals(newValue, OpImageUtils.getNoData(op, Double.NaN), EPSILON);
    }
    else
    {
      Assert.assertEquals(sourceNoData, OpImageUtils.getNoData(op, Double.NaN), EPSILON);
    }
    Assert.assertEquals(width, r.getWidth());
    Assert.assertEquals(height, r.getHeight());
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(sourceNoData, v, EPSILON);
        }
        else
        {
          if (pixelId <= min || pixelId >= max)
          {
            Assert.assertEquals(pixelId, v, EPSILON);
          }
          else
          {
            Assert.assertEquals(newValue, v, EPSILON);
          }
        }
      }
    }
  }

  private void runTest(WritableRaster source,
      double newValue, boolean newIsNull,
      double min, double max)
  {
    ColorModel cm1 = RasterUtils.createColorModel(source);
    java.util.Hashtable<String, Object> arg1Properties = new java.util.Hashtable<String, Object>();
    arg1Properties.put(OpImageUtils.NODATA_PROPERTY, Double.NaN);
    BufferedImage s1 = new BufferedImage(cm1, source, false, arg1Properties);
    RenderedOp op = ReplaceValuesDescriptor.create(s1, newValue, newIsNull, min, max, null);
    Raster r = op.getData();
    if (newIsNull)
    {
      Assert.assertEquals(newValue, OpImageUtils.getNoData(op, Double.NaN), EPSILON);
    }
    else
    {
      Assert.assertTrue(Double.isNaN(OpImageUtils.getNoData(op, Double.NaN)));
    }
    Assert.assertEquals(width, r.getWidth());
    Assert.assertEquals(height, r.getHeight());
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if (pixelId <= min || pixelId >= max)
        {
          Assert.assertEquals(pixelId, v, EPSILON);
        }
        else
        {
          Assert.assertEquals("Value was not replaced at pixel " + x + ", " + y, newValue, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testReplace()
  {
    double newValue = -20.0;
    double min = 10.0;
    double max = 40.0;
    runTest(numbered, newValue, false, min, max);
  }

  @Test
  @Category(UnitTest.class)
  public void testReplaceNoop()
  {
    // Verify that if the replacement values do not occur inside the
    // raster, no pixels are replaced.
    double newValue = -20.0;
    double min = -500.0;
    double max = -400.0;
    runTest(numbered, newValue, false, min, max);
  }

  @Test
  @Category(UnitTest.class)
  public void testReplaceWithNoData()
  {
    double newValue = -20.0;
    double min = 10.0;
    double max = 40.0;
    runTestWithNoData(numberedWithNoData, NON_NAN_NODATA_VALUE, newValue, true, min, max);
  }

  @Test
  @Category(UnitTest.class)
  public void testReplaceNoopWithNoData()
  {
    // Verify that if the replacement values do not occur inside the
    // raster, no pixels are replaced.
    double newValue = -20.0;
    double min = -500.0;
    double max = -400.0;
    runTestWithNoData(numberedWithNanNoData, Double.NaN, newValue, true, min, max);
  }
}
