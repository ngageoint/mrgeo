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

package org.mrgeo.data.raster;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.awt.*;
import java.awt.image.*;

@SuppressWarnings("static-method")
public class RasterUtilsTest
{ 

  private static int noDataModValue = 7;
  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static int width;
  private static int height;

  private static WritableRaster numberedInt;
  private static WritableRaster numberedFloat;
  private static WritableRaster numberedDouble;
  private static WritableRaster numberedWithNoData;
  private static WritableRaster numberedWithNanNoData;

  @BeforeClass
  public static void init()
  {
    width = 50;
    height = 50;
    SampleModel smi = new BandedSampleModel(DataBuffer.TYPE_INT, width, height, 1);
    SampleModel smf = new BandedSampleModel(DataBuffer.TYPE_FLOAT, width, height, 1);
    SampleModel smd = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);

    numberedInt = Raster.createWritableRaster(smi, new Point(0, 0));
    numberedFloat = Raster.createWritableRaster(smf, new Point(0, 0));
    numberedDouble = Raster.createWritableRaster(smd, new Point(0, 0));
    numberedWithNoData = Raster.createWritableRaster(smf, new Point(0, 0));
    numberedWithNanNoData = Raster.createWritableRaster(smf, new Point(0, 0));


    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        int pixelId = x + (y * width);

        numberedInt.setSample(x, y, 0, pixelId);
        numberedFloat.setSample(x, y, 0, (float)pixelId);
        numberedDouble.setSample(x, y, 0, (double)pixelId);

        numberedWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : (double)pixelId);
        numberedWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : (double)pixelId);
      }
    }
  }


  @AfterClass
  public static void tearDown() throws Exception
  {    
    numberedInt = null;
    numberedFloat = null;
    numberedDouble = null;

    numberedWithNoData = null;
    numberedWithNanNoData = null;
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
    WritableRaster scaled;


    // scale up
    for (scale = 1; scale < 5; scale++)
    {
      scaled = RasterUtils.scaleRasterNearest(numberedFloat, 
          numberedFloat.getWidth() * scale, numberedFloat.getHeight() * scale);

      Assert.assertEquals("bad width",  numberedFloat.getWidth() * scale, scaled.getWidth());
      Assert.assertEquals("bad height",  numberedFloat.getHeight() * scale, scaled.getHeight());

      compareScaledFloat(numberedFloat, scaled, scale);

      scaled = RasterUtils.scaleRasterNearest(numberedDouble, 
          numberedDouble.getWidth() * scale, numberedDouble.getHeight() * scale);

      Assert.assertEquals("bad width",  numberedDouble.getWidth() * scale, scaled.getWidth());
      Assert.assertEquals("bad height",  numberedDouble.getHeight() * scale, scaled.getHeight());

      compareScaledDouble(numberedDouble, scaled, scale);

      scaled = RasterUtils.scaleRasterNearest(numberedInt, 
          numberedInt.getWidth() * scale, numberedInt.getHeight() * scale);

      Assert.assertEquals("bad width",  numberedInt.getWidth() * scale, scaled.getWidth());
      Assert.assertEquals("bad height",  numberedInt.getHeight() * scale, scaled.getHeight());


      compareScaledInt(numberedInt, scaled, scale);
    }

    // scale down
    for (scale = 2; scale < 8; scale += 2)
    {
      scaled = RasterUtils.scaleRasterNearest(numberedInt, 
          numberedInt.getWidth() / scale, numberedInt.getHeight() / scale);

      Assert.assertEquals("bad width",  numberedInt.getWidth() / scale, scaled.getWidth());
      Assert.assertEquals("bad height",  numberedInt.getHeight() / scale, scaled.getHeight());

      compareScaledDownInt(numberedInt, scaled, scale);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void scaleRasterInterp()
  {
    int scale;
    WritableRaster scaled;

    scale = 2;
    scaled = RasterUtils.scaleRasterInterp(numberedFloat, 
        numberedFloat.getWidth() * scale, numberedFloat.getHeight() * scale, Float.NaN);

    //    TestUtils.saveRaster(scaled, "TIFF", "scaled.tif");
    //    // scale up
    //    for (scale = 1; scale < 5; scale++)
    //    {
    //      scaled = RasterUtils.scaleRasterInterp(numberedFloat, 
    //          numberedFloat.getWidth() * scale, numberedFloat.getHeight() * scale);
    //
    //      Assert.assertEquals("bad width",  numberedFloat.getWidth() * scale, scaled.getWidth());
    //      Assert.assertEquals("bad height",  numberedFloat.getHeight() * scale, scaled.getHeight());
    //
    //      compareScaledFloat(numberedFloat, scaled, scale);
    //
    //      scaled = RasterUtils.scaleRasterInterp(numberedDouble, 
    //          numberedDouble.getWidth() * scale, numberedDouble.getHeight() * scale);
    //
    //      Assert.assertEquals("bad width",  numberedDouble.getWidth() * scale, scaled.getWidth());
    //      Assert.assertEquals("bad height",  numberedDouble.getHeight() * scale, scaled.getHeight());
    //
    //      compareScaledDouble(numberedDouble, scaled, scale);
    //
    //      scaled = RasterUtils.scaleRasterInterp(numberedInt, 
    //          numberedInt.getWidth() * scale, numberedInt.getHeight() * scale);
    //
    //      Assert.assertEquals("bad width",  numberedInt.getWidth() * scale, scaled.getWidth());
    //      Assert.assertEquals("bad height",  numberedInt.getHeight() * scale, scaled.getHeight());
    //
    //
    //      compareScaledInt(numberedInt, scaled, scale);
    //    }
    //
    //    // scale down
    //    scale = 2;
    //
    //    scaled = RasterUtils.scaleRasterInterp(numberedInt, 
    //        numberedInt.getWidth() / scale, numberedInt.getHeight() / scale);
    //
    //    Assert.assertEquals("bad width",  numberedInt.getWidth() / scale, scaled.getWidth());
    //    Assert.assertEquals("bad height",  numberedInt.getHeight() / scale, scaled.getHeight());
    //
    //    compareScaledDownInt(numberedInt, scaled, scale);
  }


  private void compareScaledDownInt(Raster orig, Raster scaled, int scaleFactor)
  {
    for (int x = 0; x < scaled.getWidth(); x++)
    {
      for (int y = 0; y < scaled.getHeight(); y++)

        for (int band = 0; band < scaled.getNumBands(); band++)
        {
          int v1 = scaled.getSample(x, y, band);
          int v2 = orig.getSample(x * scaleFactor, y * scaleFactor, band);

          Assert.assertEquals("Pixel value mismatch: scale: 1/" + scaleFactor + "  px: " + x + " py: " +  y 
              + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2);
        }
    }
  }

  private void compareScaledInt(Raster orig, Raster scaled, int scaleFactor)
  {
    for (int x = 0; x < scaled.getWidth(); x++)
    {
      for (int y = 0; y < scaled.getHeight(); y++)

        for (int band = 0; band < scaled.getNumBands(); band++)
        {
          int v1 = scaled.getSample(x, y, band);
          int v2 = orig.getSample(x / scaleFactor, y / scaleFactor, band);

          Assert.assertEquals("Pixel value mismatch: px: " + x + " py: " +  y 
              + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2);
        }
    }
  }

  private void compareScaledFloat(Raster orig, Raster scaled, int scaleFactor)
  {
    for (int x = 0; x < scaled.getWidth(); x++)
    {
      for (int y = 0; y < scaled.getHeight(); y++)

        for (int band = 0; band < scaled.getNumBands(); band++)
        {
          float v1 = scaled.getSampleFloat(x, y, band);
          float v2 = orig.getSampleFloat(x / scaleFactor, y / scaleFactor, band);

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

  private void compareScaledDouble(Raster orig, Raster scaled, int scaleFactor)
  {
    for (int x = 0; x < scaled.getWidth(); x++)
    {
      for (int y = 0; y < scaled.getHeight(); y++)

        for (int band = 0; band < scaled.getNumBands(); band++)
        {
          double v1 = scaled.getSampleDouble(x, y, band);
          double v2 = orig.getSampleDouble(x / scaleFactor, y / scaleFactor, band);

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
