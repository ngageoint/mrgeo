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

package org.mrgeo.image;


import org.junit.*;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;

import java.io.File;
import java.io.IOException;

@SuppressWarnings("static-method")
public class MrsImageTest extends LocalRunnerTest
{
  private static final double epsilon = 0.000001;

  private static final int zoom = 10;
  private static String allonesName = "all-ones";
  private static String allOnes = Defs.INPUT + allonesName;

  private MrsImage allOnesImage;
  private ProviderProperties providerProperties = null;

  @BeforeClass
  public static void init() throws Exception
  {
    File file = new File(allOnes);
    allOnes = "file://" + file.getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception
  {
    allOnesImage = MrsImage.open(allOnes, zoom, providerProperties);
  }


  @After
  public void tearDown() throws Exception
  {
    allOnesImage.close();
  }

  @Test
  @Category(UnitTest.class)
  public void open() throws IOException
  {
    MrsImage image = MrsImage.open(allOnes, zoom, providerProperties);
    Assert.assertNotNull("MrsImage should not be null!", image);
    image.close();
  }

  @Test
  @Category(UnitTest.class)
  public void openMissing() throws IOException
  {
    String badImage = "this/is/a/bad/name";
    try
    {
      MrsImage.open(badImage, zoom, providerProperties);
      Assert.fail("Expected IOException");
    }
    catch (IOException e)
    {
      Assert.assertEquals("Unable to find a MrsImage data provider for " + badImage, e.getMessage());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void openNull() throws IOException
 {
    try
    {
      MrsImage.open(null, 0, providerProperties);
    }
    catch (IOException e)
    {
      Assert.assertEquals("Unable to open image. Resource name is empty.", e.getMessage());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void openWrongLevel() throws IOException
  {
    MrsImage image = MrsImage.open(allOnes, 99, providerProperties);

    Assert.assertNull("MrsImage should be null!", image);
  }

  @Test
  @Category(UnitTest.class)
  public void close() throws IOException
  {
    MrsImage image = MrsImage.open(allOnes, zoom, providerProperties);

    Assert.assertNotNull("MrsImage should not be null!", image);

    image.close();
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void convertToLatLng()
  {
    // What we're doing here is converting to lat/log based on the pixel bounds, then using 
    // TMSUtils to convert back to pixels (an alternate method), then making sure we get same px/py
    // out of the calculation.
    // The reason for doing so is because in order to calculate the lat/lons, we'd do the exact
    // same calculation as the convertToLatLng(), so we'd really would'nt be testing the
    // algorithm, just that we can do the same calculation twice...
    MrsPyramidMetadata meta = allOnesImage.getMetadata();
    LongRectangle pixelBounds = meta.getPixelBounds(zoom);
    int tilesize = meta.getTilesize();

    Bounds bounds = allOnesImage.getBounds();

    // keep in mind pixel 0,0 is the upper left
    LatLng ul = allOnesImage.convertToLatLng(pixelBounds.getMinX(), pixelBounds.getMinY());
    LatLng lr = allOnesImage.convertToLatLng(pixelBounds.getMaxX(), pixelBounds.getMaxY());
    LatLng ct = allOnesImage.convertToLatLng(pixelBounds.getCenterX(), pixelBounds.getCenterY());

    TMSUtils.Pixel ultms = TMSUtils.latLonToPixels(ul.getLat(), ul.getLng(), zoom, tilesize);
    TMSUtils.Pixel lrtms = TMSUtils.latLonToPixels(lr.getLat(), lr.getLng(), zoom, tilesize);
    TMSUtils.Pixel cttms = TMSUtils.latLonToPixels(ct.getLat(), ct.getLng(), zoom, tilesize);

    TMSUtils.Pixel cnr = TMSUtils.latLonToPixels(bounds.s, bounds.w, zoom, tilesize);

    long ulx = ultms.px - cnr.px;
    long uly = ultms.py - cnr.py;

    Assert.assertEquals("Bad pixel Latitude", uly, pixelBounds.getMaxY());
    Assert.assertEquals("Bad pixel Longitude", ulx, pixelBounds.getMinX());

    long lrx = lrtms.px - cnr.px;
    long lry = lrtms.py - cnr.py;

    Assert.assertEquals("Bad pixel Latitude", lry, pixelBounds.getMinY());
    Assert.assertEquals("Bad pixel Longitude", lrx, pixelBounds.getMaxX());

    long ctx = cttms.px - cnr.px;
    long cty = cttms.py - cnr.py;

    Assert.assertEquals("Bad pixel Latitude", cty, pixelBounds.getCenterY());
    Assert.assertEquals("Bad pixel Longitude", ctx, pixelBounds.getCenterX());
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testConvertToPixelCenterLatLng()
  {
    // What we're doing here is converting to lat/log based on the pixel bounds, then using 
    // TMSUtils to convert back to pixels (an alternate method), then making sure we get same px/py
    // out of the calculation.

    MrsPyramidMetadata meta = allOnesImage.getMetadata();
    LongRectangle pixelBounds = meta.getPixelBounds(zoom);
    int tilesize = meta.getTilesize();

    Bounds bounds = allOnesImage.getBounds();

    // keep in mind pixel 0,0 is the upper left
    LatLng ul = allOnesImage.convertToPixelCenterLatLng(pixelBounds.getMinX(),
        pixelBounds.getMinY());
    LatLng lr = allOnesImage.convertToPixelCenterLatLng(pixelBounds.getMaxX(),
        pixelBounds.getMaxY());
    LatLng ct = allOnesImage.convertToPixelCenterLatLng(pixelBounds.getCenterX(),
        pixelBounds.getCenterY());

    TMSUtils.Pixel ultms = TMSUtils.latLonToPixels(ul.getLat(), ul.getLng(), zoom, tilesize);
    TMSUtils.Pixel lrtms = TMSUtils.latLonToPixels(lr.getLat(), lr.getLng(), zoom, tilesize);
    TMSUtils.Pixel cttms = TMSUtils.latLonToPixels(ct.getLat(), ct.getLng(), zoom, tilesize);

    TMSUtils.Pixel cnr = TMSUtils.latLonToPixels(bounds.s, bounds.w, zoom, tilesize);

    long ulx = ultms.px - cnr.px;
    long uly = ultms.py - cnr.py;

    Assert.assertEquals("Bad pixel Latitude", uly, pixelBounds.getMaxY());
    Assert.assertEquals("Bad pixel Longitude", ulx, pixelBounds.getMinX());

    long lrx = lrtms.px - cnr.px;
    long lry = lrtms.py - cnr.py;

    Assert.assertEquals("Bad pixel Latitude", lry, pixelBounds.getMinY());
    Assert.assertEquals("Bad pixel Longitude", lrx, pixelBounds.getMaxX());

    long ctx = cttms.px - cnr.px;
    long cty = cttms.py - cnr.py;

    Assert.assertEquals("Bad pixel Latitude", cty, pixelBounds.getCenterY());
    Assert.assertEquals("Bad pixel Longitude", ctx, pixelBounds.getCenterX());

  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testConvertToPixelX()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testConvertToPixelY()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testConvertToWorldX()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testConvertToWorldY()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetAnyTile()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetBounds()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetBoundsIntInt()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetColorModel()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetDefaultValue()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetHeight()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetImageBounds()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetMaxTileX()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetMaxTileY()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetMaxZoomlevel()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  public void testGetMetadata()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetMinTileX()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetMinTileY()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetNumXTiles()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetNumYTiles()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetPixelHeight()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetPixelMinX()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetPixelMinY()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetPixelRect()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetPixelWidth()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetSampleModel()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTile()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTileBounds()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTileHeight()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTileInfo()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTileRange()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTilesize()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTileType()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetTileWidth()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetWidth()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetWorldBounds()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetZoomlevel()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testIsTileEmpty()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testIsValidRegion()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testSetScaleType()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetRenderedImage()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetRenderedImageBounds()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Ignore
  @Category(UnitTest.class)
  public void testGetExtrema()
  {
    Assert.fail("Not yet implemented");
  }

}
