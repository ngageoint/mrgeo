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

package org.mrgeo.data.raster;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import java.awt.image.DataBuffer;

@SuppressWarnings("static-method")
public class MrGeoRasterTest
{
//private static int noDataModValue = 7;
private static double NON_NAN_NODATA_VALUE = -32767.0;

private static int width;
private static int height;

private static MrGeoRaster numberedInt;
private static MrGeoRaster numberedFloat;
private static MrGeoRaster numberedDouble;
//private static MrGeoRaster numberedWithNoData;
//private static MrGeoRaster numberedWithNanNoData;

@BeforeClass
public static void init()
{
  width = 50;
  height = 50;

  numberedInt = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_INT);
  numberedFloat = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_FLOAT);
  numberedDouble = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_DOUBLE);
//  numberedWithNoData = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_DOUBLE);
//  numberedWithNanNoData = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_DOUBLE);

  numberedInt = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_INT);
  numberedFloat = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_FLOAT);
  numberedDouble = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_DOUBLE);

//  numberedWithNoData = TestUtils.createNumberedRasterWithNodata(width, height, NON_NAN_NODATA_VALUE, noDataModValue);
//  numberedWithNanNoData = TestUtils.createNumberedRasterWithNodata(width, height, Double.NaN, noDataModValue);
}


@AfterClass
public static void tearDown() throws Exception
{
  numberedInt = null;
  numberedFloat = null;
  numberedDouble = null;

//  numberedWithNoData = null;
//  numberedWithNanNoData = null;
}

@Before
public void setUp() throws Exception
{
}


@Test
@Category(UnitTest.class)
public void scaleRasterNearest()
{
  int scale;
  MrGeoRaster scaled;

  // scale up
  for (scale = 1; scale < 5; scale++)
  {
    scaled = numberedFloat.scale(width * scale, height * scale, false, new double[]{ NON_NAN_NODATA_VALUE });

    Assert.assertEquals("bad width",  numberedFloat.width() * scale, scaled.width());
    Assert.assertEquals("bad height",  numberedFloat.height() * scale, scaled.height());

    compareScaledFloat(numberedFloat, scaled, scale);

    scaled = numberedDouble.scale(width * scale, height * scale, false, new double[]{ NON_NAN_NODATA_VALUE });

    Assert.assertEquals("bad width",  numberedDouble.width() * scale, scaled.width());
    Assert.assertEquals("bad height",  numberedDouble.height() * scale, scaled.height());

    compareScaledDouble(numberedDouble, scaled, scale);

    scaled = numberedInt.scale(width * scale, height * scale, false, new double[]{ NON_NAN_NODATA_VALUE });

    Assert.assertEquals("bad width",  numberedInt.width() * scale, scaled.width());
    Assert.assertEquals("bad height",  numberedInt.height() * scale, scaled.height());


    compareScaledInt(numberedInt, scaled, scale);
  }

  // scale down
  for (scale = 2; scale < 8; scale += 2)
  {
    scaled = numberedInt.scale(width / scale, height / scale, false, new double[]{ NON_NAN_NODATA_VALUE });

    Assert.assertEquals("bad width",  numberedInt.width() / scale, scaled.width());
    Assert.assertEquals("bad height",  numberedInt.height() / scale, scaled.height());

    compareScaledDownInt(numberedInt, scaled, scale);
  }
}

//@Test
//@Category(UnitTest.class)
//public void scaleRasterInterp()
//{
//  int scale;
//  WritableRaster scaled;
//
//  scale = 2;
//  scaled = RasterUtils.scaleRasterInterp(numberedFloat,
//      numberedFloat.width() * scale, numberedFloat.height() * scale, Double.NaN);
//}

private void compareScaledDownInt(MrGeoRaster orig, MrGeoRaster scaled, int scaleFactor)
{
  for (int x = 0; x < scaled.width(); x++)
  {
    for (int y = 0; y < scaled.height(); y++)

      for (int band = 0; band < scaled.bands(); band++)
      {
        int v1 = scaled.getPixelInt(x, y, band);
        int v2 = orig.getPixelInt(x * scaleFactor, y * scaleFactor, band);

        Assert.assertEquals("Pixel value mismatch: scale: 1/" + scaleFactor + "  px: " + x + " py: " +  y
            + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2);
      }
  }
}

private void compareScaledInt(MrGeoRaster orig, MrGeoRaster scaled, int scaleFactor)
{
  for (int x = 0; x < scaled.width(); x++)
  {
    for (int y = 0; y < scaled.height(); y++)

      for (int band = 0; band < scaled.bands(); band++)
      {
        int v1 = scaled.getPixelInt(x, y, band);
        int v2 = orig.getPixelInt(x / scaleFactor, y / scaleFactor, band);

        Assert.assertEquals("Pixel value mismatch: px: " + x + " py: " +  y
            + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2);
      }
  }
}

private void compareScaledFloat(MrGeoRaster orig, MrGeoRaster scaled, int scaleFactor)
{
  for (int x = 0; x < scaled.width(); x++)
  {
    for (int y = 0; y < scaled.height(); y++)

      for (int band = 0; band < scaled.bands(); band++)
      {
        float v1 = scaled.getPixelFloat(x, y, band);
        float v2 = orig.getPixelFloat(x / scaleFactor, y / scaleFactor, band);

        Assert.assertEquals("Pixel NaN mismatch: px: " + x + " py: " +  y
            + " b: " + band + " v1: " + v1 + " v2: " + v2,  Float.isNaN(v1), Float.isNaN(v2));

        // make delta something reasonable relative to the data

        //NOTE: this formula is not very reliable.  An error of 2e-3f for
        //    pixel v1=1 fails, but passes for v1=2.
        float delta = Math.max(Math.abs(v1 * 1e-3f), 1e-3f);
        Assert.assertEquals("Pixel value mismatch: px: " + x + " py: " +  y
            + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2, delta);

      }
  }
}

private void compareScaledDouble(MrGeoRaster orig, MrGeoRaster scaled, int scaleFactor)
{
  for (int x = 0; x < scaled.width(); x++)
  {
    for (int y = 0; y < scaled.height(); y++)

      for (int band = 0; band < scaled.bands(); band++)
      {
        double v1 = scaled.getPixelDouble(x, y, band);
        double v2 = orig.getPixelDouble(x / scaleFactor, y / scaleFactor, band);

        Assert.assertEquals("Pixel NaN mismatch: px: " + x + " py: " +  y
            + " b: " + band + " v1: " + v1 + " v2: " + v2,  Double.isNaN(v1), Double.isNaN(v2));

        // make delta something reasonable relative to the data

        //NOTE: this formula is not very reliable.  An error of 2e-3f for
        //    pixel v1=1 fails, but passes for v1=2.
        double delta = Math.max(Math.abs(v1 * 1e-3f), 1e-3f);
        Assert.assertEquals("Pixel value mismatch: px: " + x + " py: " +  y
            + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2, delta);

      }
  }
}



}
