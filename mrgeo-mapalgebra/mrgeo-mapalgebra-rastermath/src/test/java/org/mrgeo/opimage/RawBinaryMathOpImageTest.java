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
import javax.media.jai.util.ImagingException;
import java.awt.*;
import java.awt.image.*;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("static-method")
public class RawBinaryMathOpImageTest
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
  private static WritableRaster twos;
  private static WritableRaster twosWithNoData;
  private static WritableRaster twosWithNanNoData;

  @BeforeClass
  public static void init()
  {
    width = 10;
    height = 10;
    sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    numbered = Raster.createWritableRaster(sm, new Point(0, 0));
    numberedWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    numberedWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    twos = Raster.createWritableRaster(sm, new Point(0, 0));
    twosWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    twosWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
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
        twos.setSample(x, y, 0, 2.0);
        twosWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : 2.0);
        twosWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : 2.0);
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

  private Raster runOperation(String operation, WritableRaster arg1, double arg1NoData,
      WritableRaster arg2, double arg2NoData)
  {
    ColorModel cm1 = RasterUtils.createColorModel(arg1);
    java.util.Hashtable<String, Object> arg1Properties = new java.util.Hashtable<String, Object>();
    arg1Properties.put(OpImageUtils.NODATA_PROPERTY, arg1NoData);
    BufferedImage s1 = new BufferedImage(cm1, arg1, false, arg1Properties);
    ColorModel cm2 = RasterUtils.createColorModel(arg2);
    java.util.Hashtable<String, Object> arg2Properties = new java.util.Hashtable<String, Object>();
    arg2Properties.put(OpImageUtils.NODATA_PROPERTY, arg2NoData);
    BufferedImage s2 = new BufferedImage(cm2, arg2, false, arg2Properties);
    RenderedOp op = RawBinaryMathDescriptor.create(s1, s2, operation);
    Raster r = op.getData();
    Assert.assertEquals(width, r.getWidth());
    Assert.assertEquals(height, r.getHeight());
    return r;
  }

  @Test
  @Category(UnitTest.class)
  public void testBadOperation()
  {
    try
    {
      runOperation("A",
          numbered, Double.NaN,
          numbered, Double.NaN);
      Assert.fail("Expected exception for bad operation");
    }
    catch(ImagingException e)
    {
      Assert.assertNotNull(e.getCause());
      Assert.assertTrue(InvocationTargetException.class.isAssignableFrom(e.getCause().getClass()));
      InvocationTargetException ite = (InvocationTargetException)e.getCause();
      Assert.assertNotNull(ite.getTargetException());
      Assert.assertTrue(ite.getTargetException() instanceof IllegalArgumentException);
      System.out.println("Got exception");
    }
  }

  //
  // Test the multiple operation
  //
  @Test
  @Category(UnitTest.class)
  public void testMultiply()
  {
    Raster r = runOperation("*",
        numbered, Double.NaN,
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(pixelId * pixelId, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiplyArg1NoData()
  {
    Raster r = runOperation("*",
        numberedWithNoData, NON_NAN_NODATA_VALUE,
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(NON_NAN_NODATA_VALUE, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(pixelId * pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiplyArg1NanNoData()
  {
    Raster r = runOperation("*",
        numberedWithNanNoData, Double.NaN,
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId * pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiplyArg1NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("*",
        numberedWithNanNoData, Double.NaN,
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId * pixelId, v, EPSILON);
        }
      }
    }
  }

  //
  // Test multiply operation where the second argument has NODATA values
  //
  @Test
  @Category(UnitTest.class)
  public void testMultiplyArg2NoData()
  {
    Raster r = runOperation("*",
        numbered, Double.NaN,
        numberedWithNoData, NON_NAN_NODATA_VALUE);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId * pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiplyArg2NanNoData()
  {
    Raster r = runOperation("*",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId * pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiplyArg2NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("*",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId * pixelId, v, EPSILON);
        }
      }
    }
  }

  //
  // Test the divide operation
  //
  @Test
  @Category(UnitTest.class)
  public void testDivide()
  {
    Raster r = runOperation("/",
        numbered, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(pixelId / 2.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testDivideArg1NoData()
  {
    Raster r = runOperation("/",
        numberedWithNoData, NON_NAN_NODATA_VALUE,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(NON_NAN_NODATA_VALUE, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(pixelId / 2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testDivideArg1NanNoData()
  {
    Raster r = runOperation("/",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId / 2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testDivideArg1NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("/",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId / 2.0, v, EPSILON);
        }
      }
    }
  }


  //
  // Test divide operation where the second argument has NODATA values
  //
  @Test
  @Category(UnitTest.class)
  public void testDivideArg2NoData()
  {
    Raster r = runOperation("/",
        numbered, Double.NaN,
        numberedWithNoData, NON_NAN_NODATA_VALUE);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId / pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testDivideArg2NanNoData()
  {
    Raster r = runOperation("/",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId / pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testDivideArg2NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("/",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId / pixelId, v, EPSILON);
        }
      }
    }
  }
  //
  // Test the add operation
  //
  @Test
  @Category(UnitTest.class)
  public void testAdd()
  {
    Raster r = runOperation("+",
        numbered, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(pixelId + 2.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddArg1NoData()
  {
    Raster r = runOperation("+",
        numberedWithNoData, NON_NAN_NODATA_VALUE,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(NON_NAN_NODATA_VALUE, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(pixelId + 2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddArg1NanNoData()
  {
    Raster r = runOperation("+",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId + 2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddArg1NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("+",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId + 2.0, v, EPSILON);
        }
      }
    }
  }

  //
  //
  // Test add operation where the second argument has NODATA values
  //
  @Test
  @Category(UnitTest.class)
  public void testAddArg2NoData()
  {
    Raster r = runOperation("+",
        numbered, Double.NaN,
        numberedWithNoData, NON_NAN_NODATA_VALUE);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId + pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddArg2NanNoData()
  {
    Raster r = runOperation("+",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId + pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddArg2NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("+",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId + pixelId, v, EPSILON);
        }
      }
    }
  }

  // Test the subtract operation
  //
  @Test
  @Category(UnitTest.class)
  public void testSubtract()
  {
    Raster r = runOperation("-",
        numbered, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(pixelId - 2.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSubtractArg1NoData()
  {
    Raster r = runOperation("-",
        numberedWithNoData, NON_NAN_NODATA_VALUE,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(NON_NAN_NODATA_VALUE, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(pixelId - 2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSubtractArg1NanNoData()
  {
    Raster r = runOperation("-",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId - 2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSubtractArg1NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("-",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId - 2.0, v, EPSILON);
        }
      }
    }
  }

  //
  // Test subtract operation where the second argument has NODATA values
  //
  @Test
  @Category(UnitTest.class)
  public void testSubtractArg2NoData()
  {
    Raster r = runOperation("-",
        numbered, Double.NaN,
        numberedWithNoData, NON_NAN_NODATA_VALUE);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId - pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSubtractArg2NanNoData()
  {
    Raster r = runOperation("-",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId - pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSubtractArg2NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("-",
        numbered, Double.NaN,
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(pixelId - pixelId, v, EPSILON);
        }
      }
    }
  }

  //
  // Test the pow operation
  //
  @Test
  @Category(UnitTest.class)
  public void testPow()
  {
    Raster r = runOperation("^",
        numbered, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testPowArg1NoData()
  {
    Raster r = runOperation("^",
        numberedWithNoData, NON_NAN_NODATA_VALUE,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(NON_NAN_NODATA_VALUE, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testPowArg1NanNoData()
  {
    Raster r = runOperation("^",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testPowArg1NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("^",
        numberedWithNanNoData, Double.NaN,
        twos, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
        }
      }
    }
  }

  //
  // Test pow operation where the second argument has NODATA values
  //
  @Test
  @Category(UnitTest.class)
  public void testPowArg2NoData()
  {
    Raster r = runOperation("^",
        numbered, Double.NaN,
        twosWithNoData, NON_NAN_NODATA_VALUE);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testPowArg2NanNoData()
  {
    Raster r = runOperation("^",
        numbered, Double.NaN,
        twosWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testPowArg2NanNoDataOutputNanNoData()
  {
    Raster r = runOperation("^",
        numbered, Double.NaN,
        twosWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertTrue(Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals(Math.pow(pixelId, 2.0), v, EPSILON);
        }
      }
    }
  }
}
