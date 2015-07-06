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
public class ReplaceNullOpImageTest
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
      double newValue, boolean defaultNewIsNull, boolean newIsNull)
  {
    ColorModel cm1 = RasterUtils.createColorModel(source);
    java.util.Hashtable<String, Object> arg1Properties = new java.util.Hashtable<String, Object>();
    arg1Properties.put(OpImageUtils.NODATA_PROPERTY, sourceNoData);
    BufferedImage s1 = new BufferedImage(cm1, source, false, arg1Properties);
    RenderedOp op = null;
    if (defaultNewIsNull)
    {
      op = ReplaceNullDescriptor.create(s1, newValue, null);
    }
    else
    {
      op = ReplaceNullDescriptor.create(s1, newValue, newIsNull, null);
    }
    Raster r = op.getData();
    if (!defaultNewIsNull && newIsNull)
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
          Assert.assertEquals(newValue, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(pixelId, v, EPSILON);
        }
      }
    }
  }

  // If defaultNewIsNull is true, then newIsNull is ignored and it should behave as
  // if newIsNull were false.
  private void runTest(WritableRaster source, double sourceNoData, double newValue,
      boolean defaultNewIsNull, boolean newIsNull)
  {
    ColorModel cm1 = RasterUtils.createColorModel(source);
    java.util.Hashtable<String, Object> arg1Properties = new java.util.Hashtable<String, Object>();
    arg1Properties.put(OpImageUtils.NODATA_PROPERTY, sourceNoData);
    BufferedImage s1 = new BufferedImage(cm1, source, false, arg1Properties);
    RenderedOp op = null;
    if (defaultNewIsNull)
    {
      op = ReplaceNullDescriptor.create(s1, newValue, null);
    }
    else
    {
      op = ReplaceNullDescriptor.create(s1, newValue, newIsNull, null);
    }
    Raster r = op.getData();
    if (!defaultNewIsNull && newIsNull)
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
        Assert.assertEquals(pixelId, v, EPSILON);
      }
    }
  }

  // Replace NoData values in a source raster without NoData. The result
  // should be the same as the original raster. The NoData value for
  // the result should not change.
  @Test
  @Category(UnitTest.class)
  public void testReplaceNoopSameNoData1()
  {
    double newValue = -20.0;
    runTest(numbered, Double.NaN, newValue, true, false);
  }

  // Replace NoData values in a source raster without NoData. The result
  // should be the same as the original raster. The NoData value for
  // the result should not change.
  @Test
  @Category(UnitTest.class)
  public void testReplaceNoopSameNoData2()
  {
    double newValue = -20.0;
    runTest(numbered, Double.NaN, newValue, false, false);
  }

  // Replace NoData values in a source raster without NoData and change the
  // NoData value to the replacement value. The resulting raster
  // should be the same as the original raster. However, the NoData value
  // should change to the replacement value.
  @Test
  @Category(UnitTest.class)
  public void testReplaceNoopNewNoData()
  {
    double newValue = -20.0;
    runTest(numbered, Double.NaN, newValue, false, true);
  }

  @Test
  @Category(UnitTest.class)
  public void testReplaceNullsDefaultChangeNoData()
  {
    double newValue = -20.0;
    runTestWithNoData(numberedWithNanNoData, Double.NaN, newValue, true, false);
  }

  @Test
  @Category(UnitTest.class)
  public void testReplaceNulls()
  {
    double newValue = -20.0;
    runTestWithNoData(numberedWithNanNoData, Double.NaN, newValue, false, false);
  }

  @Test
  @Category(UnitTest.class)
  public void testReplaceNullsChangeNoData()
  {
    double newValue = -20.0;
    runTestWithNoData(numberedWithNanNoData, Double.NaN, newValue, false, true);
  }
}
