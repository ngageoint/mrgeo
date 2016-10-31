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
import org.gdal.gdal.Dataset;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;

@SuppressWarnings("static-method")
public class MrGeoRasterTest
{
@Rule
public TestName testname = new TestName();

private static boolean GEN_BASELINE_DATA_ONLY = false;

private TestUtils testutils;

private static int width;
private static int height;

private static MrGeoRaster numberedInt;
private static MrGeoRaster numberedFloat;
private static MrGeoRaster numberedDouble;

@BeforeClass
public static void init() throws MrGeoRaster.MrGeoRasterException
{
  width = 13;
  height = 13;


  numberedInt = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_INT);
  numberedFloat = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_FLOAT);
  numberedDouble = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_DOUBLE);

}


@AfterClass
public static void tearDown() throws Exception
{
  numberedInt = null;
  numberedFloat = null;
  numberedDouble = null;
}

@Before
public void setUp() throws Exception
{
  testutils = new TestUtils(MrGeoRasterTest.class);
}

@Test
@Category(UnitTest.class)
public void toFromDatasetByte() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_BYTE, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  TestUtils.compareRasters(src, dst);
}

@Test
@Category(UnitTest.class)
public void toFromDatasetShort() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_SHORT, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  TestUtils.compareRasters(src, dst);
}

@Test
@Category(UnitTest.class)
public void toFromDatasetUShort() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_USHORT, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  TestUtils.compareRasters(src, dst);
}

@Test
@Category(UnitTest.class)
public void toFromDatasetInt() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_INT, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  TestUtils.compareRasters(src, dst);
}

@Test
@Category(UnitTest.class)
public void toFromDatasetFloat() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_FLOAT, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  TestUtils.compareRasters(src, dst);
}

@Test
@Category(UnitTest.class)
public void toFromDatasetDouble() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_DOUBLE, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  TestUtils.compareRasters(src, dst);
}


@Test
@Category(UnitTest.class)
public void scaleRasterNearestInt() throws IOException
{
  int scale;
  MrGeoRaster scaled;

  // scale up
  for (scale = 1; scale < 15; scale++)
  {
    scaled = numberedInt.scale(width * scale, height * scale, false, new double[]{ Double.NaN });
    compareResult(scale, scaled);
  }

  // scale down
  for (scale = 2; scale < 8; scale++)
  {
    scaled = numberedInt.scale(width / scale, height / scale, false, new double[]{ Double.NaN });
    compareResult(1.0 / scale, scaled);
  }
}

@Test
@Category(UnitTest.class)
public void scaleRasterNearestFloat() throws IOException
{
  int scale;
  MrGeoRaster scaled;

  // scale up
  for (scale = 1; scale < 15; scale++)
  {
    scaled = numberedFloat.scale(width * scale, height * scale, false, new double[]{ Double.NaN });
    compareResult(scale, scaled);
  }

  // scale down
  for (scale = 2; scale < 8; scale++)
  {
    scaled = numberedFloat.scale(width / scale, height / scale, false, new double[]{ Double.NaN });
    compareResult(1.0 / scale, scaled);
  }
}

private void compareRaster(MrGeoRaster mrgeoraster, Raster raster)
{
  boolean intish = mrgeoraster.datatype() == DataBuffer.TYPE_BYTE || mrgeoraster.datatype() == DataBuffer.TYPE_INT
      || mrgeoraster.datatype() == DataBuffer.TYPE_SHORT || mrgeoraster.datatype() == DataBuffer.TYPE_USHORT;

  for (int b = 0; b < mrgeoraster.bands(); b++)
  {
    for (int y = 0; y < mrgeoraster.height(); y++)
    {
      for (int x = 0; x < mrgeoraster.width(); x++)
      {
        float v1 = mrgeoraster.getPixelFloat(x, y, b);
        float v2 = raster.getSampleFloat(x, y, b);

        if (Float.isNaN(v1) != Float.isNaN(v2))
        {
          org.junit.Assert.assertEquals("Pixel NaN mismatch: px: " + x + " py: " + y
              + " b: " + b + " v1: " + v1 + " v2: " + v2, v1, v2, 0);
        }

        // make delta something reasonable relative to the data

        //NOTE: this formula is not very reliable.  An error of 2e-3f for pixel v1=1 fails, but passes for v1=2.
        float delta = intish ? 1.0001f : Math.max(Math.abs(v1 * 1e-3f), 1e-3f);
        org.junit.Assert.assertEquals("Pixel value mismatch: px: " + x + " py: " + y
            + " b: " + b + " v1: " + v1 + " v2: " + v2, v1, v2, delta);

      }
    }
  }
}


private void compareResult(double scale, MrGeoRaster scaled) throws IOException
{
  String name = testname.getMethodName() + String.format("-%.3f", scale);
  if (GEN_BASELINE_DATA_ONLY)
  {
    testutils.saveBaselineTif(name, scaled);
  }
  else
  {
    testutils.compareRasters(name, scaled);
  }
}

@Test
@Category(UnitTest.class)
public void scaleRasterNearestDouble() throws IOException
{
  int scale;
  MrGeoRaster scaled;

  // scale up
  for (scale = 1; scale < 15; scale++)
  {
    scaled = numberedDouble.scale(width * scale, height * scale, false, new double[]{ Double.NaN });
    compareResult(scale, scaled);
  }

  // scale down
  for (scale = 2; scale < 8; scale += 1)
  {
    scaled = numberedDouble.scale(width / scale, height / scale, false, new double[]{ Double.NaN });
    compareResult(1.0 / scale, scaled);
  }
}

@Test
@Category(UnitTest.class)
public void toRasterByte() throws IOException
{
  MrGeoRaster numberedByte = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_BYTE);
  Raster raster = numberedByte.toRaster();

  compareRaster(numberedByte, raster);
}


@Test
@Category(UnitTest.class)
public void toRasterShort() throws IOException
{
  MrGeoRaster numberedShort = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_SHORT);
  Raster raster = numberedShort.toRaster();

  compareRaster(numberedShort, raster);

}
@Test
@Category(UnitTest.class)
public void toRasterUShort() throws IOException
{
  MrGeoRaster numberedUShort = TestUtils.createNumberedRaster(width, height, DataBuffer.TYPE_USHORT);
  Raster raster = numberedUShort.toRaster();

  compareRaster(numberedUShort, raster);

}
@Test
@Category(UnitTest.class)
public void toRasterInt() throws IOException
{  Raster raster = numberedInt.toRaster();

  compareRaster(numberedInt, raster);

}
@Test
@Category(UnitTest.class)
public void toRasterFloat() throws IOException
{
  Raster raster = numberedFloat.toRaster();

  compareRaster(numberedFloat, raster);
}
@Test
@Category(UnitTest.class)
public void toRasterDouble() throws IOException
{
  Raster raster = numberedDouble.toRaster();

  compareRaster(numberedDouble, raster);
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



}
