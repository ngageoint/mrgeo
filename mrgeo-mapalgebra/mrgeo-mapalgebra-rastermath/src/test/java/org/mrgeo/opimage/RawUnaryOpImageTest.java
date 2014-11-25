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
public class RawUnaryOpImageTest
{
  private static final double EPSILON = 1e-8;

  // In nodata rasters, set the value of each nth pixel to NODATA
  private static int noDataModValue = 7;
  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static SampleModel sm;
  private static int width;
  private static int height;
  private static WritableRaster numbered;
  private static WritableRaster negNumbered;
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
    negNumbered = Raster.createWritableRaster(sm, new Point(0, 0));
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
        negNumbered.setSample(x, y, 0, (double)-pixelId);
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

  private Raster runOperation(String operation, WritableRaster arg, double argNoData)
  {
    ColorModel cm1 = RasterUtils.createColorModel(arg);
    java.util.Hashtable<String, Object> argProperties = new java.util.Hashtable<String, Object>();
    argProperties.put(OpImageUtils.NODATA_PROPERTY, argNoData);
    BufferedImage s1 = new BufferedImage(cm1, arg, false, argProperties);
    RenderedOp op = RawUnaryDescriptor.create(s1, operation);
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
      runOperation("BadOp",
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
  // Test the negative operation
  //
  @Test
  @Category(UnitTest.class)
  public void testNegative()
  {
    Raster r = runOperation("-",
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(-pixelId, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNegativeArgNoData()
  {
    Raster r = runOperation("-",
        numberedWithNoData, NON_NAN_NODATA_VALUE);
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
          Assert.assertEquals(-pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNegativeArgNanNoData()
  {
    Raster r = runOperation("-",
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
          Assert.assertEquals(-pixelId, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNegativeArgNanNoDataOutputNanNoData()
  {
    Raster r = runOperation("-",
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
          Assert.assertEquals(-pixelId, v, EPSILON);
        }
      }
    }
  }

  //
  // Test the abs operation
  //
  @Test
  @Category(UnitTest.class)
  public void testAbs()
  {
    Raster r = runOperation("abs",
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(Math.abs(pixelId), v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAbsWithNegValues()
  {
    Raster r = runOperation("abs",
        negNumbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(Math.abs(pixelId), v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAbsArgNoData()
  {
    Raster r = runOperation("abs",
        numberedWithNoData, NON_NAN_NODATA_VALUE);
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
          Assert.assertEquals(Math.abs(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAbsArgNanNoData()
  {
    Raster r = runOperation("abs",
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
          Assert.assertEquals(Math.abs(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAbsArgNanNoDataOutputNanNoData()
  {
    Raster r = runOperation("abs",
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
          Assert.assertEquals(Math.abs(pixelId), v, EPSILON);
        }
      }
    }
  }

  //
  // Test the sine operation
  //
  @Test
  @Category(UnitTest.class)
  public void testSin()
  {
    Raster r = runOperation("sin",
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(Math.sin(pixelId), v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSinArgNoData()
  {
    Raster r = runOperation("sin",
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
          Assert.assertEquals(Math.sin(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSinArgNanNoData()
  {
    Raster r = runOperation("sin",
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
          Assert.assertEquals(Math.sin(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSinArgNanNoDataOutputNanNoData()
  {
    Raster r = runOperation("sin",
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
          Assert.assertEquals(Math.sin(pixelId), v, EPSILON);
        }
      }
    }
  }

  //
  // Test the cosine operation
  //
  @Test
  @Category(UnitTest.class)
  public void testCos()
  {
    Raster r = runOperation("cos",
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(Math.cos(pixelId), v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testCosArgNoData()
  {
    Raster r = runOperation("cos",
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
          Assert.assertEquals(Math.cos(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testCosArgNanNoData()
  {
    Raster r = runOperation("cos",
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
          Assert.assertEquals(Math.cos(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testCosArgNanNoDataOutputNanNoData()
  {
    Raster r = runOperation("cos",
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
          Assert.assertEquals(Math.cos(pixelId), v, EPSILON);
        }
      }
    }
  }

  //
  // Test the tangent operation
  //
  @Test
  @Category(UnitTest.class)
  public void testTan()
  {
    Raster r = runOperation("tan",
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals(Math.tan(pixelId), v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testTanArgNoData()
  {
    Raster r = runOperation("tan",
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
          Assert.assertEquals(Math.tan(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testTanArgNanNoData()
  {
    Raster r = runOperation("tan",
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
          Assert.assertEquals(Math.tan(pixelId), v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testTanArgNanNoDataOutputNanNoData()
  {
    Raster r = runOperation("tan",
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
          Assert.assertEquals(Math.tan(pixelId), v, EPSILON);
        }
      }
    }
  }

  //
  // Test the isNull operation
  //
  @Test
  @Category(UnitTest.class)
  public void testIsNull()
  {
    Raster r = runOperation("isNull",
        numbered, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        Assert.assertEquals(0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testIsNullArgNoData()
  {
    Raster r = runOperation("isNull",
        numberedWithNoData, NON_NAN_NODATA_VALUE);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(1, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testIsNullArgNanNoData()
  {
    Raster r = runOperation("isNull",
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(1, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testIsNullArgNanNoDataOutputNanNoData()
  {
    Raster r = runOperation("isNull",
        numberedWithNanNoData, Double.NaN);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        if ((pixelId % noDataModValue) == 0)
        {
          Assert.assertEquals(1, v, EPSILON);
        }
        else
        {
          Assert.assertEquals(0, v, EPSILON);
        }
      }
    }
  }
}
