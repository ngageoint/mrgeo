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
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

@SuppressWarnings("static-method")
public class CropRasterOpImageTest
{
  private static final double EPSILON = 1e-8;

  // In nodata rasters, set the value of each nth pixel to NODATA
//  private static int noDataModValue = 7;
//  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static int width;
  private static int height;
  private static WritableRaster numbered;
//  private static WritableRaster numberedWithNoData;
//  private static WritableRaster numberedWithNanNoData;
//  private static WritableRaster twos;
//  private static WritableRaster twosWithNoData;
//  private static WritableRaster twosWithNanNoData;

  @BeforeClass
  public static void init()
  {
    width = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    height = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    
    numbered = TestUtils.createNumberedRaster(width, height);
//    numberedWithNoData = 
//        TestUtils.createNumberedRasterWithNodata(width, height, NON_NAN_NODATA_VALUE, noDataModValue);
//    numberedWithNanNoData = 
//        TestUtils.createNumberedRasterWithNodata(width, height, Double.NaN, noDataModValue);
//    
//    twos = TestUtils.createConstRaster(width, height, 2.0);
//    twosWithNoData = 
//        TestUtils.createConstRasterWithNodata(width, height, 2.0, NON_NAN_NODATA_VALUE, noDataModValue);
//    twosWithNanNoData = 
//        TestUtils.createConstRasterWithNodata(width, height, 2.0, Double.NaN, noDataModValue);
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

  private static Raster runOperation(WritableRaster src, double srcNoData,
      double minX, double minY, double maxX, double maxY,
      int zoomLevel, int tileSize, String cropType,
      long tileX, long tileY)
  {
    ColorModel cm = RasterUtils.createColorModel(src);
    java.util.Hashtable<String, Object> srcProperties = new java.util.Hashtable<String, Object>();
    srcProperties.put(OpImageUtils.NODATA_PROPERTY, srcNoData);
    BufferedImage s = new BufferedImage(cm, src, false, srcProperties);
    CropRasterOpImage op = CropRasterOpImage.create(s, minX, minY, maxX - minX, maxY - minY,
        zoomLevel, tileSize, cropType);
    op.setTileInfo(tileX, tileY, zoomLevel, tileSize);
    Raster r = op.getData();
    Assert.assertEquals(width, r.getWidth());
    Assert.assertEquals(height, r.getHeight());
    return r;
  }

  @Test
  @Category(UnitTest.class)
  public void testBadCropType()
  {
    try
    {
      runOperation(numbered, Double.NaN, 0.0, 0.0, 0.1, 0.1, 1, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, "BAD", 0, 0);
      Assert.fail("Expected exception for bad crop type");
    }
    catch(IllegalArgumentException e)
    {
    }
  }

  // *********
  // Run crop tests where the crop region does not intersect the
  // tile being cropped. Need to test when the tile is outside of
  // all four sides of the crop region, and with both types of
  // cropping.
  // *********

  private void verifyAllSameValue(Raster r, double value, int tileSize)
  {
    boolean isValueNan = Double.isNaN(value);
    for (int x=0; x < tileSize; x++)
    {
      for (int y=0; y < tileSize; y++)
      {
        double v = r.getSampleDouble(x, y, 0);
        if (isValueNan)
        {
          Assert.assertTrue("Expected NaN, got " + v + " at pixel " + x + ", " + y,
              Double.isNaN(v));
        }
        else
        {
          Assert.assertEquals("Expected " + value + ", got " + v + " at pixel " + x + ", " + y,
              v, value, EPSILON);
        }
      }
    }
  }

  // FAST mode, tile below crop region
  @Test
  @Category(UnitTest.class)
  public void testTileBelowCropFast()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 2;
    long tileY = 0;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX, tileY + 2, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "FAST", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // EXACT mode, tile below crop region
  @Test
  @Category(UnitTest.class)
  public void testTileBelowCropExact()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 2;
    long tileY = 0;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX, tileY + 2, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "EXACT", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // FAST mode, tile above crop region
  @Test
  @Category(UnitTest.class)
  public void testTileAboveCropFast()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 2;
    long tileY = 2;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX, tileY - 2, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "FAST", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // EXACT mode, tile below crop region
  @Test
  @Category(UnitTest.class)
  public void testTileAboveCropExact()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 2;
    long tileY = 2;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX, tileY - 2, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "EXACT", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // FAST mode, tile left of crop region
  @Test
  @Category(UnitTest.class)
  public void testTileLeftOfCropFast()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 2;
    long tileY = 2;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX + 2, tileY, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "FAST", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // EXACT mode, tile left of crop region
  @Test
  @Category(UnitTest.class)
  public void testTileLeftOfCropExact()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 2;
    long tileY = 2;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX + 2, tileY, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "EXACT", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // FAST mode, tile right of crop region
  @Test
  @Category(UnitTest.class)
  public void testTileRightOfCropFast()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 4;
    long tileY = 2;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX - 2, tileY, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "FAST", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // EXACT mode, tile right of crop region
  @Test
  @Category(UnitTest.class)
  public void testTileRightOfCropExact()
  {
    int tileSize = width;
    int zoomLevel = 3;
    int tileX = 4;
    long tileY = 2;
    TMSUtils.Bounds tileBounds = TMSUtils.tileBounds(tileX - 2, tileY, zoomLevel, tileSize);
    Raster r = runOperation(numbered, Double.NaN,
        tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n, 3, tileSize, "EXACT", tileX, tileY);
    verifyAllSameValue(r, Double.NaN, tileSize);
  }

  // *********
  // Test cases where tile intersects crop region
  // *********

  // Fast crop always gives back the full tile
  @Test
  @Category(UnitTest.class)
  public void testFastCrop()
  {
    int tileSize = width;
    int zoomLevel = 1;
    Raster r = runOperation(numbered, Double.NaN,
        -179.0, -89.0, -1.0, 89.0, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  private void verifyCroppedValues(Raster r, TMSUtils.Bounds cropRegion, int zoomLevel, int tileSize)
  {
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    for (int x=0; x < tileSize; x++)
    {
      for (int y=0; y < tileSize; y++)
      {
        // lat/lon coordinates of pixels are at the top-left corner of the pixel
        double latitude = 90.0 - (y * resolution);
        double longitude = -180.0 + (x * resolution);
        TMSUtils.LatLon pt = new TMSUtils.LatLon(latitude, longitude);
        double v = r.getSampleDouble(x, y, 0);
        TMSUtils.Bounds pixelRegion = new TMSUtils.Bounds(pt.lon, pt.lat - resolution,
            pt.lon + resolution, pt.lat);
        if (cropRegion.intersect(pixelRegion, false))
        {
          int pixelId = getPixelId(x, y, width);
          Assert.assertEquals("Failed at pixel " + x + " (" + pt.lon + "), " + y + " (" + pt.lat + "), value is " + v,
              pixelId, v, EPSILON);
        }
        else
        {
          Assert.assertTrue("Failed at pixel " + x + " (" + pt.lon + "), " + y + " (" + pt.lat + "), value is " + v,
              Double.isNaN(v));
        }
      }
    }
  }

  private void verifyPixelValues(Raster r, int zoomLevel, int tileSize)
  {
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    for (int x=0; x < tileSize; x++)
    {
      for (int y=0; y < tileSize; y++)
      {
        // lat/lon coordinates of pixels are at the top-left corner of the pixel
        double latitude = 90.0 - (y * resolution);
        double longitude = -180.0 + (x * resolution);
        TMSUtils.LatLon pt = new TMSUtils.LatLon(latitude, longitude);
        double v = r.getSampleDouble(x, y, 0);
        int pixelId = getPixelId(x, y, width);
        Assert.assertEquals("Failed at pixel " + x + " (" + pt.lon + "), " + y + " (" + pt.lat + "), value is " + v,
            pixelId, v, EPSILON);
      }
    }
  }

  // Exact crop at the top edge
  @Test
  @Category(UnitTest.class)
  public void testExactCropTop()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -90.0, 0.0, 89.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  // Fast crop at the top edge. In this case, no cropping is done
  // because fast-cropping is only done on the tile level.
  @Test
  @Category(UnitTest.class)
  public void testFastCropTop()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -90.0, 0.0, 89.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // Exact crop at the bottom edge
  @Test
  @Category(UnitTest.class)
  public void testExactCropBottom()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -89.0, 0.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  // Fast crop at the bottom edge
  @Test
  @Category(UnitTest.class)
  public void testFastCropBottom()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -89.0, 0.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // Exact crop at the left edge
  @Test
  @Category(UnitTest.class)
  public void testExactCropLeft()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-175.0, -90.0, 0.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  // Fast crop at the left edge
  @Test
  @Category(UnitTest.class)
  public void testFastCropLeft()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-175.0, -90.0, 0.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // Exact crop at the right edge
  @Test
  @Category(UnitTest.class)
  public void testExactCropRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -90.0, -10.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  // Fast crop at the right edge
  @Test
  @Category(UnitTest.class)
  public void testFastCropRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -90.0, -10.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // *******
  // Cropping at corners
  // *******
  @Test
  @Category(UnitTest.class)
  public void testExactCropTopLeft()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-175.0, -90.0, 0.0, 85.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropTopLeft()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-175.0, -90.0, 0.0, 85.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testExactCropBottomLeft()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-175.0, -89.0, 0.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropBottomLeft()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-175.0, -89.0, 0.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testExactCropTopRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -90.0, -10.0, 85.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropTopRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -90.0, -10.0, 85.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testExactCropBottomRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -89.0, -5.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropBottomRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -89.0, -5.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // ********
  // Opposite sides
  // ********
  @Test
  @Category(UnitTest.class)
  public void testExactCropBottomTop()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -89.0, 0.0, 80.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropBottomTop()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-180.0, -89.0, 0.0, 80.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testExactCropLeftRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-170.0, -90.0, -5.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropLeftRight()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-170.0, -90.0, -5.0, 90.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // ********
  // Opposite sides
  // ********
  @Test
  @Category(UnitTest.class)
  public void testExactCropAllSides()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-170.0, -89.5, -5.0, 87.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 0, 0);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }

  @Test
  @Category(UnitTest.class)
  public void testFastCropAllSides()
  {
    int tileSize = width;
    int zoomLevel = 1;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-170.0, -89.5, -5.0, 87.0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "FAST", 0, 0);
    verifyPixelValues(r, zoomLevel, tileSize);
  }

  // Cropping up to tile border
  @Test
  @Category(UnitTest.class)
  public void testCropAtTileBorders()
  {
    int tileSize = width;
    int zoomLevel = 3;
    TMSUtils.Bounds cropRegion = new TMSUtils.Bounds(-135, -45, -90, 0);
    Raster r = runOperation(numbered, Double.NaN,
        cropRegion.w, cropRegion.s, cropRegion.e, cropRegion.n, zoomLevel, tileSize, "EXACT", 1, 2);
    verifyCroppedValues(r, cropRegion, zoomLevel, tileSize);
  }
}
