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
public static void init()
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
public void toFromDataset() throws IOException
{
  MrGeoRaster src = TestUtils.createConstRaster(10, 10, DataBuffer.TYPE_INT, 1);

  Dataset ds = src.toDataset();

  MrGeoRaster dst = MrGeoRaster.fromDataset(ds);

  Assert.assertEquals("bad width",  src.width(), dst.width());
  Assert.assertEquals("bad height", src.height(), dst.height());

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
    compareResult(scale, numberedInt, scaled);
  }

  // scale down
  for (scale = 2; scale < 8; scale++)
  {
    scaled = numberedInt.scale(width / scale, height / scale, false, new double[]{ Double.NaN });
    compareResult(1.0 / scale, numberedInt, scaled);
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
    compareResult(scale, numberedFloat, scaled);
  }

  // scale down
  for (scale = 2; scale < 8; scale++)
  {
    scaled = numberedFloat.scale(width / scale, height / scale, false, new double[]{ Double.NaN });
    compareResult(1.0 / scale, numberedFloat, scaled);
  }
}

private void compareResult(double scale, MrGeoRaster orig, MrGeoRaster scaled) throws IOException
{
  Assert.assertEquals("bad width",  (int)(orig.width() * scale), scaled.width());
  Assert.assertEquals("bad height", (int)(orig.height() * scale), scaled.height());

  if (GEN_BASELINE_DATA_ONLY)
  {
    testutils.saveBaselineTif(testname.getMethodName() + String.format("-%.3f", scale), scaled);
  }
  else
  {
    testutils.compareRasters(testname.getMethodName() + String.format("-%.3f", scale), scaled);
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
    compareResult(scale, numberedDouble, scaled);
  }

  // scale down
  for (scale = 2; scale < 8; scale += 1)
  {
    scaled = numberedDouble.scale(width / scale, height / scale, false, new double[]{ Double.NaN });
    compareResult(1.0 / scale, numberedDouble, scaled);
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



}
