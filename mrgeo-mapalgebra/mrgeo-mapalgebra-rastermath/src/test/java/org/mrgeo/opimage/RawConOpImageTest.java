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
import java.util.Vector;

@SuppressWarnings("static-method")
public class RawConOpImageTest
{
  private static final double EPSILON = 1e-8;

  // In nodata rasters, set the value of each nth pixel to NODATA
  private static int noDataModValue = 7;
  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static SampleModel sm;
  private static int width;
  private static int height;
  private static WritableRaster zeros;
  private static WritableRaster zerosWithNoData;
  private static WritableRaster zerosWithNanNoData;
  private static WritableRaster ones;
  private static WritableRaster onesWithNoData;
  private static WritableRaster onesWithNanNoData;
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
    zeros = Raster.createWritableRaster(sm, new Point(0, 0));
    zerosWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    zerosWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    ones = Raster.createWritableRaster(sm, new Point(0, 0));
    onesWithNoData = Raster.createWritableRaster(sm, new Point(0, 0));
    onesWithNanNoData = Raster.createWritableRaster(sm, new Point(0, 0));
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

  private Raster runOperation(WritableRaster[] inputs, double[] sourceNoDataValues)
  {
    if (inputs.length != sourceNoDataValues.length)
    {
      throw new IllegalArgumentException("Internal test setup error - there must be a nodata value for each source");
    }
    Vector<RenderedImage> sources = new Vector<RenderedImage>();
    for (int i = 0; i < inputs.length; i++)
    {
      WritableRaster wr = inputs[i];
      ColorModel cm = RasterUtils.createColorModel(wr);
      java.util.Hashtable<String, Object> argProperties = new java.util.Hashtable<String, Object>();
      argProperties.put(OpImageUtils.NODATA_PROPERTY, sourceNoDataValues[i]);
      BufferedImage bi = new BufferedImage(cm, wr, false, argProperties);
      sources.add(bi);
    }
    RenderedOp op = RawConDescriptor.create(sources);
    Raster r = op.getData();
    Assert.assertEquals(width, r.getWidth());
    Assert.assertEquals(height, r.getHeight());
    return r;
  }

  //
  // Run tests with a single condition and a zero condition layer.
  // Values in the resulting layer will be derived from the "then"
  // layer.
  //
  @Test
  @Category(UnitTest.class)
  public void testFalseWithoutNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        Assert.assertEquals(2.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNoDataFalseWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zerosWithNoData, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNanDataFalseWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zerosWithNanNoData, ones, twos };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testFalseWithNonNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, ones, twosWithNoData };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testFalseWithNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, ones, twosWithNanNoData };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  //
  // Run tests with a single condition and a "one" condition layer.
  // Values in the resulting layer will be derived from the "then"
  // layer.
  //
  @Test
  @Category(UnitTest.class)
  public void testTrueWithoutNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { ones, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        Assert.assertEquals(1.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNoDataTrueWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { onesWithNoData, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNanDataTrueWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { onesWithNanNoData, ones, twos };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testTrueWithNonNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { ones, onesWithNoData, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testTrueWithNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { ones, onesWithNanNoData, twos };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  //
  // Run tests with two conditions, the first being zeros, and the
  // second a "one" condition layer.
  // Values in the resulting layer will be derived from the "then"
  // layer.
  //
  @Test
  @Category(UnitTest.class)
  public void testMultiTrueWithoutNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, ones, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        Assert.assertEquals(1.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiNoDataTrueWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, onesWithNoData, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiNanDataTrueWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, onesWithNanNoData, ones, twos };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiTrueWithNonNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, ones, onesWithNoData, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiTrueWithNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, ones, onesWithNanNoData, twos };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(1.0, v, EPSILON);
        }
      }
    }
  }

  //
  // Run tests with two conditions, the first being zeros, and the
  // second a "zero" condition layer.
  // Values in the resulting layer will be derived from the "then"
  // layer.
  //
  @Test
  @Category(UnitTest.class)
  public void testMultiFalseWithoutNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, zeros, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        Assert.assertEquals(2.0, v, EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiNoDataFalseWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, zerosWithNoData, ones, twos };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiNanDataFalseWithoutNoData()
  {
    // Use false condition layer containing nodata
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, zerosWithNanNoData, ones, twos };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiFalseWithNonNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, zeros, ones, twosWithNoData };
    double[] noDataValues = new double[] {
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE,
        NON_NAN_NODATA_VALUE
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiFalseWithNanNoData()
  {
    // Use false condition layer
    // The "then" is all ones
    // The "else" is all two's
    WritableRaster[] inputs = new WritableRaster[] { zeros, zeros, zeros, ones, twosWithNanNoData };
    double[] noDataValues = new double[] {
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN,
        Double.NaN
        };
    Raster r = runOperation(inputs, noDataValues);
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
          Assert.assertEquals(2.0, v, EPSILON);
        }
      }
    }
  }
}
