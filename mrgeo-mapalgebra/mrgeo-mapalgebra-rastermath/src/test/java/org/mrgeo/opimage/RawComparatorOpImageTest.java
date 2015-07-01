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
import javax.media.jai.util.ImagingException;
import java.awt.*;
import java.awt.image.*;
import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("static-method")
public class RawComparatorOpImageTest
{
  public class TestRunner
  {
    private final String testName;
    private final String operation;
    private final WritableRaster arg1;
    private final double arg1NoData;
    private final WritableRaster arg2;
    private final double arg2NoData;
    private final double valueExpectedAtNoDataPixel;
    private final double valueExpectedAtNonNoDataPixel;

    public TestRunner(final String testName,
        final String operation,
        final WritableRaster arg1,
        final double arg1NoData,
        final WritableRaster arg2,
        final double arg2NoData,
        final double valueExpectedAtNoDataPixel,
        final double valueExpectedAtNonNoDataPixel)
    {
      this.testName = testName;
      this.operation = operation;
      this.arg1 = arg1;
      this.arg1NoData = arg1NoData;
      this.arg2 = arg2;
      this.arg2NoData = arg2NoData;
      this.valueExpectedAtNoDataPixel = valueExpectedAtNoDataPixel;
      this.valueExpectedAtNonNoDataPixel = valueExpectedAtNonNoDataPixel;
    }

    public void runTest()
    {
      final Raster r = runOperation(operation,
          arg1, arg1NoData,
          arg2, arg2NoData);
      for (int x = 0; x < width; x++)
      {
        for (int y = 0; y < height; y++)
        {
          final double v = r.getSampleDouble(x, y, 0);
          final int pixelId = getPixelId(x, y, width);
          if ((pixelId % noDataModValue) == 0)
          {
            Assert.assertEquals("Checking nodata pixel in test '" + testName + "'",
                valueExpectedAtNoDataPixel, v, EPSILON);
          }
          else
          {
            Assert.assertEquals("Checking regular pixel in test '" + testName + "'",
                valueExpectedAtNonNoDataPixel, v, EPSILON);
          }
        }
      }
    }
  }

  private static final double EPSILON = 1e-8;
  // In nodata rasters, set the value of each nth pixel to NODATA
  static int noDataModValue = 7;
  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static double COMPARATOR_OUTPUT_NO_DATA = 255.0;
  private static SampleModel sm;
  static int width;
  static int height;
  private static WritableRaster numbered;
  private static WritableRaster numberedWithNoData;
  private static WritableRaster numberedWithNanNoData;
  private static WritableRaster zeros;
  private static WritableRaster zerosWithNoData;
  private static WritableRaster zerosWithNanNoData;
  private static WritableRaster ones;
  private static WritableRaster onesWithNoData;

  private static WritableRaster onesWithNanNoData;

  @BeforeClass
  public static void init()
  {
    width = 10;
    height = 10;
    sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    numbered = Raster.createWritableRaster(sm, new Point(0, 0));
    numberedWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    numberedWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    zeros = Raster.createWritableRaster(sm, new Point(0, 0));
    zerosWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    zerosWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    ones = Raster.createWritableRaster(sm, new Point(0, 0));
    onesWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    onesWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    for (int x = 0; x < width; x++)
    {
      for (int y = 0; y < height; y++)
      {
        final int pixelId = getPixelId(x, y, width);
        numbered.setSample(x, y, 0, (double) pixelId);
        numberedWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : (double) pixelId);
        numberedWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : (double) pixelId);
        zeros.setSample(x, y, 0, 0.0);
        zerosWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : 0.0);
        zerosWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : 0.0);
        ones.setSample(x, y, 0, 1.0);
        onesWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : 1.0);
        onesWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : 1.0);
      }
    }
  }

  private static void runTests(final TestRunner[] testRunners)
  {
    for (final TestRunner tr : testRunners)
    {
      tr.runTest();
    }
  }

  static int getPixelId(final int x, final int y, final int w)
  {
    return x + (y * w);
  }

  static Raster runOperation(final String operation, final WritableRaster arg1,
      final double arg1NoData,
      final WritableRaster arg2, final double arg2NoData)
  {
    final ColorModel cm1 = RasterUtils.createColorModel(arg1);
    final java.util.Hashtable<String, Object> arg1Properties = new java.util.Hashtable<String, Object>();
    arg1Properties.put(OpImageUtils.NODATA_PROPERTY, arg1NoData);
    final BufferedImage s1 = new BufferedImage(cm1, arg1, false, arg1Properties);
    final ColorModel cm2 = RasterUtils.createColorModel(arg2);
    final java.util.Hashtable<String, Object> arg2Properties = new java.util.Hashtable<String, Object>();
    arg2Properties.put(OpImageUtils.NODATA_PROPERTY, arg2NoData);
    final BufferedImage s2 = new BufferedImage(cm2, arg2, false, arg2Properties);
    final RenderedOp op = RawComparatorDescriptor.create(s1, s2, operation);
    final Raster r = op.getData();
    Assert.assertEquals(width, r.getWidth());
    Assert.assertEquals(height, r.getHeight());
    return r;
  }

  @Before
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @Test
  @Category(UnitTest.class)
  public void testAndOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("1 and 1", "and", ones, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("0 and 1", "and", zeros, -1.0, ones, -2.0, 0.0, 0.0),
        new TestRunner("1 and 0", "&", ones, -1.0, zeros, -2.0, 0.0, 0.0),
        new TestRunner("0 and 0", "&&", zeros, -1.0, zeros, -2.0, 0.0, 0.0),
        // First Argument is NoData
        new TestRunner("1 NoData and 1", "and", onesWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData and 1", "and", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NoData and 0", "&", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData and 0", "&&", zerosWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First Argument is NanNoData
        new TestRunner("1 NanNoData and 1", "and", onesWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData and 1", "and", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData and 0", "&", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData and 0", "&&", zerosWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA,  0.0),
        // Second Argument is NoData
        new TestRunner("1 and 1 NoData", "and", ones, -2.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 and 1 NoData", "and", zeros, -2.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 and 0 NoData", "&", ones, -2.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 and 0 NoData", "&&", zeros, -2.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Second Argument is NanNoData
        new TestRunner("1 NanNoData and 1", "and", ones, Double.NaN, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData and 1", "and", zeros, Double.NaN, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData and 0", "&", ones, Double.NaN, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData and 0", "&&", zeros, Double.NaN, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Both Arguments are NoData
        new TestRunner("1 and 1 Both NoData", "and", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 and 1 Both NoData", "and", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 and 0 Both NoData", "&", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 and 0 Both NoData", "&&", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Both Arguments are NaN
        new TestRunner("1 and 1 Both NanNoData", "and", onesWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 and 1 Both NanNoData", "and", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 and 0 Both NanNoData", "&", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 and 0 Both NanNoData", "&&", zerosWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First argument is NoData and Second Argument is NanNoData
        new TestRunner("1 NoData and 1", "and", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData and 1", "and", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NoData and 0", "&", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData and 0", "&&", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First argument is NanNoData and Second Argument is NoData
        new TestRunner("1 NanNoData and 1", "and", onesWithNanNoData, Double.NaN, onesWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData and 1", "and", zerosWithNanNoData, Double.NaN, onesWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData and 0", "&", onesWithNanNoData, Double.NaN, zerosWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData and 0", "&&", zerosWithNanNoData, Double.NaN, zerosWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0)
    };
    runTests(testRunners);
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
    catch (final ImagingException e)
    {
      Assert.assertNotNull(e.getCause());
      Assert.assertTrue(InvocationTargetException.class.isAssignableFrom(e.getCause().getClass()));
      final InvocationTargetException ite = (InvocationTargetException) e.getCause();
      Assert.assertNotNull(ite.getTargetException());
      Assert.assertTrue(ite.getTargetException() instanceof IllegalArgumentException);
      System.out.println("Got exception");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testEqualsOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("# == #", "==", numbered, -1.0, numbered, -2.0, 1.0, 1.0),
        new TestRunner("# NoData == # NoData", "eq", numberedWithNoData, NON_NAN_NODATA_VALUE,
            numberedWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("# NanNoData == # NanNoData", "==", numberedWithNanNoData, Double.NaN,
            numberedWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 == 1", "==", zeros, -1.0, ones, -2.0, 0.0, 0.0),
        new TestRunner("# NoData == #", "eq", numberedWithNoData, NON_NAN_NODATA_VALUE, numbered,
            -1.0, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("# NanNoData == #", "==", numberedWithNanNoData, Double.NaN, numbered, -1.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("# NoData == #", "eq", numbered, -1.0, numberedWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("# NanNoData == #", "==", numbered, Double.NaN, numberedWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testGreaterThanEqualsOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("0 >= 1", ">=", zeros, -1.0, ones, -2.0, 0.0, 0.0),
        new TestRunner("0 >= 1 NoData", "ge", zeros, -1.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 >= 1 NanNoData", "ge", zeros, -1.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 >= 1", ">=", ones, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("1 >= 1 NoData", "ge", ones, -1.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 >= 1 NanNoData", "ge", ones, -1.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NoData >= 1", ">=", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData >= 1 NoData", ">=", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData >= 1 NanNoData", ">=", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 NoData >= 1", ">=", onesWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData >= 1 NoData", ">=", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData >= 1 NanNoData", ">=", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NanNoData >= 1", ">=", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData >= 1 NoData", ">=", zerosWithNanNoData, Double.NaN,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData >= 1 NanNoData", ">=", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 NanNoData >= 1", ">=", onesWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData >= 1 NoData", ">=", onesWithNanNoData, Double.NaN,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData >= 1 NanNoData", ">=", onesWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("1 >= 0", ">=", ones, -1.0, zeros, -2.0, 1.0, 1.0),
        new TestRunner("1 >= 0 NoData", "ge", ones, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 >= 0 NanNoData", "ge", ones, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("1 NoData >= 0", ">=", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData >= 0 NoData", ">=", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData >= 0 NanNoData", ">=", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("1 NanNoData >= 0", ">=", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData >= 0 NoData", ">=", onesWithNanNoData, Double.NaN,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData >= 0 NanNoData", ">=", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testGreaterThanOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("0 > 1", ">", zeros, -1.0, ones, -2.0, 0.0, 0.0),
        new TestRunner("0 > 1 NoData", "gt", zeros, -1.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 > 1 NanNoData", "gt", zeros, -1.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("0 > 0", ">", zeros, -1.0, zeros, -2.0, 0.0, 0.0),
        new TestRunner("0 > 0 NoData", "gt", zeros, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 > 0 NanNoData", "gt", zeros, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("0 NoData > 1", ">", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData > 1 NoData", ">", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData > 1 NanNoData", ">", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("0 NanNoData > 1", ">", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData > 1 NoData", ">", zerosWithNanNoData, Double.NaN,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData > 1 NanNoData", ">", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 > 0", ">", ones, -1.0, zeros, -2.0, 1.0, 1.0),
        new TestRunner("1 > 0 NoData", ">", ones, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 > 0 NanNoData", ">", ones, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("1 NoData > 0", ">", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData > 0 NoData", ">", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData > 0 NanNoData", ">", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("1 NanNoData > 0", ">", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData > 0 NoData", ">", onesWithNanNoData, Double.NaN,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData > 0 NanNoData", ">", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testLessThanEqualOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("0 <= 1", "<=", zeros, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("0 <= 1 NoData", "le", zeros, -1.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 <= 1 NanNoData", "le", zeros, -1.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 <= 0", "<=", zeros, -1.0, zeros, -2.0, 1.0, 1.0),
        new TestRunner("0 <= 0 NoData", "le", zeros, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 <= 0 NanNoData", "le", zeros, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NoData <= 1", "<=", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData <= 1 NoData", "<=", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData <= 1 NanNoData", "<=", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NoData <= 0", "<=", zerosWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData <= 0 NoData", "<=", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData <= 0 NanNoData", "<=", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NanNoData <= 1", "<=", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData <= 1 NoData", "<=", zerosWithNanNoData, Double.NaN,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData <= 1 NanNoData", "<=", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NanNoData <= 0", "<=", zerosWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData <= 0 NoData", "<=", zerosWithNanNoData, Double.NaN,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData <= 0 NanNoData", "<=", zerosWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("1 <= 0", "<=", ones, -1.0, zeros, -2.0, 0.0, 0.0),
        new TestRunner("1 <= 0 NoData", "<=", ones, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 <= 0 NanNoData", "<=", ones, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 NoData <= 0", "<=", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NoData <= 0 NoData", "<=", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NoData <= 0 NanNoData", "<=", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 NanNoData <= 0", "<=", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData <= 0 NoData", "<=", onesWithNanNoData, Double.NaN,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData <= 0 NanNoData", "<=", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testLessThanOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("0 < 1", "<", zeros, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("0 < 1 NoData", "lt", zeros, -1.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 < 1 NanNoData", "lt", zeros, -1.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 < 0", "<", zeros, -1.0, zeros, -2.0, 0.0, 0.0),
        new TestRunner("0 < 0 NoData", "lt", zeros, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 < 0 NanNoData", "lt", zeros, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("0 NoData < 1", "<", zerosWithNoData, NON_NAN_NODATA_VALUE, ones,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData < 1 NoData", "<", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData < 1 NanNoData", "<", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NoData < 0", "<", zerosWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData < 0 NoData", "<", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData < 0 NanNoData", "<", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("0 NanNoData < 1", "<", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData < 1 NoData", "<", zerosWithNanNoData, Double.NaN,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData < 1 NanNoData", "<", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),

        new TestRunner("0 NanNoData < 0", "<", zerosWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData < 0 NoData", "<", zerosWithNanNoData, Double.NaN,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData < 0 NanNoData", "<", zerosWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 < 0", "<", ones, -1.0, zeros, -2.0, 0.0, 0.0),
        new TestRunner("1 < 0 NoData", "lt", ones, -1.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 < 0 NanNoData", "lt", ones, -1.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 NoData < 0", "<", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NoData < 0 NoData", "<", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NoData < 0 NanNoData", "<", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),

        new TestRunner("1 NanNoData < 0", "<", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData < 0 NoData", "<", onesWithNanNoData, Double.NaN,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("1 NanNoData < 0 NanNoData", "<", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testNotEqualsOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("# <> #", "<>", numbered, -1.0, numbered, -2.0, 0.0, 0.0),
        new TestRunner("# NoData <> # NoData", "ne", numberedWithNoData, NON_NAN_NODATA_VALUE,
            numberedWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("# NanNoData <> # NanNoData", "^=", numberedWithNanNoData, Double.NaN,
            numberedWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 <> 1", "<>", zeros, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("0 NoData <> 1", "<>", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData <> 1 NoData", "<>", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("# NoData <> #", "<>", numberedWithNoData, NON_NAN_NODATA_VALUE, numbered,
            -1.0, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("# NanNoData <> #", "<>", numberedWithNanNoData, Double.NaN, numbered, -1.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("# NoData <> #", "<>", numbered, -1.0, numberedWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("# NanNoData <> #", "<>", numbered, -1.0, numberedWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testOrOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("1 or 1", "or", ones, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("0 or 1", "or", zeros, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("1 or 0", "|", ones, -1.0, zeros, -2.0, 1.0, 1.0),
        new TestRunner("0 or 0", "||", zeros, -1.0, zeros, -2.0, 0.0, 0.0),
        // First Argument is NoData
        new TestRunner("1 NoData or 1", "or", onesWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData or 1", "or", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData or 0", "or", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData or 0", "or", zerosWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First Argument is NanNoData
        new TestRunner("1 NanNoData or 1", "or", onesWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 1", "or", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData or 0", "or", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 0", "or", zerosWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Second Argument is NoData
        new TestRunner("1 or 1 NoData", "or", ones, -2.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 or 1 NoData", "or", zeros, -2.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 or 0 NoData", "or", ones, -2.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0a or 0 NoDat", "or", zeros, -2.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Second Argument is NanNoData
        new TestRunner("1 NanNoData or 1", "or", ones, -2.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 1", "or", zeros, -2.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData or 0", "or", ones, -2.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 0", "or", zeros, -2.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Both Arguments are NoData
        new TestRunner("1 or 1 Both NoData", "or", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 or 1 Both NoData", "or", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 or 0 Both NoData", "or", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 or 0 Both NoData", "or", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Both Arguments are NaN
        new TestRunner("1 or 1 Both NanNoData", "or", onesWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 or 1 Both NanNoData", "or", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 or 0 Both NanNoData", "or", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 or 0 Both NanNoData", "or", zerosWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First argument is NoData and Second Argument is NanNoData
        new TestRunner("1 NanNoData or 1", "or", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 1", "or", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData or 0", "or", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 0", "or", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First argument is NanNoData and Second Argument is NoData
        new TestRunner("1 NanNoData or 1", "or", onesWithNanNoData, Double.NaN, onesWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 1", "or", zerosWithNanNoData, Double.NaN, onesWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData or 0", "or", onesWithNanNoData, Double.NaN, zerosWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData or 0", "or", zerosWithNanNoData, Double.NaN, zerosWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0)
    };
    runTests(testRunners);
  }

  @Test
  @Category(UnitTest.class)
  public void testXorOperator()
  {
    final TestRunner[] testRunners = new TestRunner[] {
        new TestRunner("1 xor 1", "xor", ones, -1.0, ones, -2.0, 0.0, 0.0),
        new TestRunner("0 xor 1", "xor", zeros, -1.0, ones, -2.0, 1.0, 1.0),
        new TestRunner("1 xor 0", "!", ones, -1.0, zeros, -2.0, 1.0, 1.0),
        new TestRunner("0 xor 0", "!", zeros, -1.0, zeros, -2.0, 0.0, 0.0),
        // First Argument is NoData
        new TestRunner("1 NoData xor 1", "xor", onesWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NoData xor 1", "xor", zerosWithNoData, NON_NAN_NODATA_VALUE, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NoData xor 0", "xor", onesWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NoData xor 0", "xor", zerosWithNoData, NON_NAN_NODATA_VALUE, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First Argument is NanNoData
        new TestRunner("1 NanNoData xor 1", "xor", onesWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData xor 1", "xor", zerosWithNanNoData, Double.NaN, ones, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData xor 0", "xor", onesWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData xor 0", "xor", zerosWithNanNoData, Double.NaN, zeros, -2.0,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Second Argument is NoData
        new TestRunner("1 xor 1 NoData", "xor", ones, -2.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 xor 1 NoData", "xor", zeros, -2.0, onesWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 xor 0 NoData", "xor", ones, -2.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0a xor 0 NoDat", "xor", zeros, -2.0, zerosWithNoData, NON_NAN_NODATA_VALUE,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Second Argument is NanNoData
        new TestRunner("1 NanNoData xor 1", "xor", ones, -2.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData xor 1", "xor", zeros, -2.0, onesWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData xor 0", "xor", ones, -2.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData xor 0", "xor", zeros, -2.0, zerosWithNanNoData, Double.NaN,
            COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Both Arguments are NoData
        new TestRunner("1 xor 1 Both NoData", "xor", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 xor 1 Both NoData", "xor", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 xor 0 Both NoData", "xor", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 xor 0 Both NoData", "xor", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNoData, NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // Both Arguments are NaN
        new TestRunner("1 xor 1 Both NanNoData", "xor", onesWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 xor 1 Both NanNoData", "xor", zerosWithNanNoData, Double.NaN,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 xor 0 Both NanNoData", "xor", onesWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 xor 0 Both NanNoData", "xor", zerosWithNanNoData, Double.NaN,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First argument is NoData and Second Argument is NanNoData
        new TestRunner("1 NanNoData xor 1", "xor", onesWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData xor 1", "xor", zerosWithNoData, NON_NAN_NODATA_VALUE,
            onesWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData xor 0", "xor", onesWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData xor 0", "xor", zerosWithNoData, NON_NAN_NODATA_VALUE,
            zerosWithNanNoData, Double.NaN, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        // First argument is NanNoData and Second Argument is NoData
        new TestRunner("1 NanNoData xor 1", "xor", onesWithNanNoData, Double.NaN, onesWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0),
        new TestRunner("0 NanNoData xor 1", "xor", zerosWithNanNoData, Double.NaN, onesWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("1 NanNoData xor 0", "xor", onesWithNanNoData, Double.NaN, zerosWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 1.0),
        new TestRunner("0 NanNoData xor 0", "xor", zerosWithNanNoData, Double.NaN, zerosWithNoData,
            NON_NAN_NODATA_VALUE, COMPARATOR_OUTPUT_NO_DATA, 0.0)
    };
    runTests(testRunners);
  }
}
