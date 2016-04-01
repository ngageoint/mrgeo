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

package org.mrgeo.utils;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;

@SuppressWarnings("static-method")
public class TMSUtilsTest
{
  private Bounds b;
  private int zoom;
  private int ts;
  private LongRectangle tiles;
  private Bounds tb;
  private LongRectangle px;
  private double[] resolutions;
  
  @Before
  public void setUp() throws Exception
  {
    // Bounds were taken from a sample image, then the tiles and pixels were hand-calculated for
    // the zoom level and tilesize
    b = new Bounds(141.7066, -18.3733, 142.5600, -17.5200);
    zoom = 10;
    ts = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;

    tiles = new LongRectangle(915, 203, 917, 206);
    // remember, tile bounds are usually larger than the actual image bounds.
    tb = new Bounds(141.6796, -18.6328, 142.3828, -17.5781);

    px = new LongRectangle(468519, 104313, 469762, 105556);

    // taken from "Tile-Based Geospatial Information Systems Principles and Practices" by John T. Sample • Elias Ioup
    // for 512x512 tiles
    resolutions = new double[] {
        0.3515625000,
        0.1757812500,
        0.0878906250,
        0.0439453125,
        0.0219726563,
        0.0109863281,
        0.0054931641,
        0.0027465820,
        0.0013732910,
        0.0006866455,
        0.0003433228,
        0.0001716614,
        0.0000858307,
        0.0000429153,
        0.0000214577,
        0.0000107288,
        0.0000053644,
        0.0000026822,
        0.0000013411,
        0.0000006706};

  }

  @After
  public void tearDown() throws Exception
  {
  }

  @Test
  @Category(UnitTest.class)
  public void latLonToPixels()
  {
    for (int z = 1; z < TMSUtils.MAXZOOMLEVEL; z++)
    {
      TMSUtils.Pixel min = TMSUtils.latLonToPixels(-90.0, -180.0, z, ts);
      Assert.assertEquals("Bad ll pixel x", 0, min.px);
      Assert.assertEquals("Bad ll pixel y", 0, min.py);

      TMSUtils.Pixel max = TMSUtils.latLonToPixels(90.0, 180.0, z, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(ts * Math.pow(2, z - 1) * 2.0), max.px);
      Assert.assertEquals("Bad ll pixel y", (long)(ts * Math.pow(2, z - 1)), max.py);

      TMSUtils.Pixel middle = TMSUtils.latLonToPixels(0.0, 0.0, z, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(ts * Math.pow(2, z - 1) * 2.0) / 2, middle.px);
      Assert.assertEquals("Bad ll pixel y", (long)(ts * Math.pow(2, z - 1)) / 2, middle.py);

      TMSUtils.Pixel third = TMSUtils.latLonToPixels(-30.0, -60.0, z, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(ts * Math.pow(2, z - 1) * 2.0) / 3, third.px);
      Assert.assertEquals("Bad ll pixel y", (long)(ts * Math.pow(2, z - 1)) / 3, third.py);
    }

    TMSUtils.Pixel ll = TMSUtils.latLonToPixels(b.s, b.w, zoom, ts);

    Assert.assertEquals("Bad ll pixel x", px.getMinX(), ll.px);
    Assert.assertEquals("Bad ll pixel y", px.getMinY(), ll.py);

    TMSUtils.Pixel ur = TMSUtils.latLonToPixels(b.n, b.e, zoom, ts);

    Assert.assertEquals("Bad ur pixel x", px.getMaxX(), ur.px);
    Assert.assertEquals("Bad ur pixel y", px.getMaxY(), ur.py);
  }

  @Test
  @Category(UnitTest.class)
  public void latLonToTile()
  {
    for (int z = 1; z < TMSUtils.MAXZOOMLEVEL; z++)
    {
      TMSUtils.Tile min = TMSUtils.latLonToTile(-90.0, -180.0, z, ts);

      Assert.assertEquals("Bad ll tile x", 0, (int)min.tx);
      Assert.assertEquals("Bad ll tile y", 0, (int)min.ty);

      TMSUtils.Tile max = TMSUtils.latLonToTile(90.0, 180.0, z, ts);

      Assert.assertEquals("Bad ll tile x", (long)(2 * Math.pow(2, z - 1)), max.tx);
      Assert.assertEquals("Bad ll tile y", (long)(Math.pow(2, z - 1)), max.ty);

      TMSUtils.Tile middle = TMSUtils.latLonToTile(0.0, 0.0, z, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(Math.pow(2, z - 1) * 2.0) / 2, middle.tx);
      Assert.assertEquals("Bad ll pixel y", (long)(Math.pow(2, z - 1)) / 2, middle.ty);

      TMSUtils.Tile third = TMSUtils.latLonToTile(-30.0, -60.0, z, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(Math.pow(2, z - 1) * 2.0) / 3, third.tx);
      Assert.assertEquals("Bad ll pixel y", (long)(Math.pow(2, z - 1)) / 3, third.ty);
    }

    TMSUtils.Tile ll = TMSUtils.latLonToTile(b.s, b.w, zoom, ts);

    Assert.assertEquals("Bad ll tile x", tiles.getMinX(), ll.tx);
    Assert.assertEquals("Bad ll tile y", tiles.getMinY(), ll.ty);

    TMSUtils.Tile ur = TMSUtils.latLonToTile(b.n, b.e, zoom, ts);

    Assert.assertEquals("Bad ur tile x", tiles.getMaxX(), ur.tx);
    Assert.assertEquals("Bad ur tile y", tiles.getMaxY(), ur.ty);
    
    // Check a value that's just to the right of a tile x border
    TMSUtils.Tile check = TMSUtils.latLonToTile(0.0, 0.00001, 1, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
    Assert.assertEquals(1, check.tx);
    Assert.assertEquals(0, check.ty);
    // Check a value that's just above a y tile border
    check = TMSUtils.latLonToTile(0.00001, -180.0, 2, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
    Assert.assertEquals(0, check.tx);
    Assert.assertEquals(1, check.ty);
  }

  @Test
  @Category(UnitTest.class)
  public void testLatLonToTilePixelUL()
  {
    double lat = -90.0;
    double lon = 0.0;
    zoom = 1;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    TMSUtils.Tile tile = TMSUtils.latLonToTile(lat, lon, zoom, tileSize);
    TMSUtils.Pixel ptUl = TMSUtils.latLonToTilePixelUL(lat, lon, tile.tx, tile.ty, zoom, tileSize);
    TMSUtils.Pixel pt = TMSUtils.latLonToTilePixel(lat, lon, tile.tx, tile.ty, zoom, tileSize);
    Assert.assertEquals(0, pt.py);
    Assert.assertEquals(511, ptUl.py);
    lat = 89.9999;
    lon = 0.0;
    tile = TMSUtils.latLonToTile(lat, lon, zoom, tileSize);
    ptUl = TMSUtils.latLonToTilePixelUL(lat, lon, tile.tx, tile.ty, zoom, tileSize);
    pt = TMSUtils.latLonToTilePixel(lat, lon, tile.tx, tile.ty, zoom, tileSize);
    Assert.assertEquals(511, pt.py);
    Assert.assertEquals(0, ptUl.py);
  }

  @Test
  @Category(UnitTest.class)
  public void testLatLonToPixelsUL()
  {
    double lat = -90.0;
    double lon = 0.0;
    zoom = 1;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    TMSUtils.Pixel pt = TMSUtils.latLonToPixels(lat, lon, zoom, tileSize);
    TMSUtils.Pixel ptUl = TMSUtils.latLonToPixelsUL(lat, lon, zoom, tileSize);
    Assert.assertEquals(0, pt.py);
    Assert.assertEquals(511, ptUl.py);
    lat = 89.9999;
    lon = 0.0;
    pt = TMSUtils.latLonToPixels(lat, lon, zoom, tileSize);
    ptUl = TMSUtils.latLonToPixelsUL(lat, lon, zoom, tileSize);
    Assert.assertEquals(511, pt.py);
    Assert.assertEquals(0, ptUl.py);
  }

  @Test
  @Category(UnitTest.class)
  public void pixelsToTile()
  {
    TMSUtils.Tile zero = TMSUtils.pixelsToTile(0, 0, ts);

    Assert.assertEquals("Bad ll tile x", 0, zero.tx);
    Assert.assertEquals("Bad ll tile y", 0, zero.ty);

    for (int z = 1; z < TMSUtils.MAXZOOMLEVEL; z++)
    {
      TMSUtils.Tile max = TMSUtils.pixelsToTile((2 * Math.pow(2, z - 1)) * ts, Math.pow(2, z - 1) * ts, ts);

      Assert.assertEquals("Bad ll tile x", (long)(2 * Math.pow(2, z - 1)), max.tx);
      Assert.assertEquals("Bad ll tile y", (long)(Math.pow(2, z - 1)), max.ty);

      TMSUtils.Tile middle = TMSUtils.pixelsToTile((2 * Math.pow(2, z - 1)) * ts / 2, Math.pow(2, z - 1) * ts / 2, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(Math.pow(2, z - 1) * 2.0) / 2, middle.tx);
      Assert.assertEquals("Bad ll pixel y", (long)(Math.pow(2, z - 1)) / 2, middle.ty);

      TMSUtils.Tile third = TMSUtils.pixelsToTile((2 * Math.pow(2, z - 1)) * ts / 3, Math.pow(2, z - 1) * ts / 3, ts);

      Assert.assertEquals("Bad ll pixel x", (long)(Math.pow(2, z - 1) * 2.0) / 3, third.tx);
      Assert.assertEquals("Bad ll pixel y", (long)(Math.pow(2, z - 1)) / 3, third.ty);
    }

    TMSUtils.Tile ll = TMSUtils.pixelsToTile(px.getMinX(), px.getMinY(), ts);
    TMSUtils.Tile ur = TMSUtils.pixelsToTile(px.getMaxX(), px.getMaxY(), ts);

    Assert.assertEquals("Bad ll tile x", tiles.getMinX(), ll.tx);
    Assert.assertEquals("Bad ll tile y", tiles.getMinY(), ll.ty);

    Assert.assertEquals("Bad ur tile x", tiles.getMaxX(), ur.tx);
    Assert.assertEquals("Bad ur tile y", tiles.getMaxY(), ur.ty);
  }

  @Test
  @Category(UnitTest.class)
  public void pixelsULToTile()
  {
    TMSUtils.Tile zero = TMSUtils.pixelsULToTile(0, 0, 1, ts);

    Assert.assertEquals("Bad ll tile x", 0, zero.tx);
    Assert.assertEquals("Bad ll tile y", 0, zero.ty);

    for (int z = 1; z < TMSUtils.MAXZOOMLEVEL; z++)
    {
      double numYTiles = Math.pow(2, z - 1);
      double numXTiles = 2.0 * numYTiles;
      //double maxYTile = numYTiles - 1.0;
      double maxXTile = numXTiles - 1.0;
      double numYPixels = numYTiles * ts;
      double numXPixels = (2.0 * numYPixels);
      double maxPixelX = numXPixels - 1.0;
      double maxPixelY = numYPixels - 1.0;

      // Make sure the bottom, right corner is returned for the max pixel values
      // since we're using top, left as the anchor.
      TMSUtils.Tile max = TMSUtils.pixelsULToTile(maxPixelX, maxPixelY, z, ts);
      Assert.assertEquals("Bad ll tile x at zoom " + z, (long)maxXTile, max.tx);
      Assert.assertEquals("Bad ll tile y at zoom " + z, 0, max.ty);

      // Check the center pixel
      TMSUtils.Tile middle = TMSUtils.pixelsULToTile(numXPixels / 2, numYPixels / 2, z, ts);
      Assert.assertEquals("Bad ll pixel x at zoom " + z, (long)(numXTiles) / 2, middle.tx);
      Assert.assertEquals("Bad ll pixel y at zoom " + z, Math.max(0, (long)(numYTiles) / 2 - 1), middle.ty);

      // Check the pixel whose coordinates are 1/3 of the pixel height and width. In tile
      // coordinates, the X tile coordinate should be 1/3 of the distance from the left
      // side, and the y coordinate should be 2/3 of the distance from the bottom.
      TMSUtils.Tile third = TMSUtils.pixelsULToTile(numXPixels / 3, numYPixels / 3, z, ts);
      Assert.assertEquals("Bad ll pixel x at zoom " + z, (long)(numXTiles) / 3, third.tx);
      Assert.assertEquals("Bad ll pixel y at zoom " + z, Math.max(0, (long)(2.0 * numYTiles / 3.0)), third.ty);
    }

    TMSUtils.Tile ll = TMSUtils.pixelsToTile(px.getMinX(), px.getMinY(), ts);
    TMSUtils.Tile ur = TMSUtils.pixelsToTile(px.getMaxX(), px.getMaxY(), ts);

    Assert.assertEquals("Bad ll tile x at zoom ", tiles.getMinX(), ll.tx);
    Assert.assertEquals("Bad ll tile y at zoom ", tiles.getMinY(), ll.ty);

    Assert.assertEquals("Bad ur tile x at zoom ", tiles.getMaxX(), ur.tx);
    Assert.assertEquals("Bad ur tile y at zoom ", tiles.getMaxY(), ur.ty);
  }

  @Test
  @Category(UnitTest.class)
  public void resolution()
  {
    for (int i = 0; i < resolutions.length; i++)
    {
      double res = TMSUtils.resolution(i + 1, ts);
      Assert.assertEquals("Bad resolution", resolutions[i], res, 0.000000001);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void tileBounds()
  {

    for (int z = 1; z < TMSUtils.MAXZOOMLEVEL; z++)
    {
      double res = TMSUtils.resolution(z, ts);

      Bounds zero = TMSUtils.tileBounds(0, 0, z, ts);

      Assert.assertEquals("Bad ll lat", -90.0, zero.s, 0.0001);
      Assert.assertEquals("Bad ll lon", -180.0, zero.w, 0.0001);

      Assert.assertEquals("Bad ur lat", -90.0 + (res * ts), zero.n, 0.0001);
      Assert.assertEquals("Bad ur lon", -180.0 + (res * ts), zero.e, 0.0001);



      //      Assert.assertEquals("Bad ll pixel x", (int)(Math.pow(2, z - 1) * 2.0) / 2, (int)middle.tx);
      //      Assert.assertEquals("Bad ll pixel y", (int)(Math.pow(2, z - 1)) / 2, (int)middle.ty);
      //
      //      TMSUtils.Tile third = TMSUtils.latLonToTile(-30.0, -60.0, z, ts);
      //
      //      Assert.assertEquals("Bad ll pixel x", (int)(Math.pow(2, z - 1) * 2.0) / 3, (int)third.tx);
      //      Assert.assertEquals("Bad ll pixel y", (int)(Math.pow(2, z - 1)) / 3, (int)third.ty);

      // remember to subtract 1 from the tile calculation because we need to upper right tile
      Bounds max = TMSUtils.tileBounds((int)(2 * Math.pow(2, z - 1)) - 1, (int)(Math.pow(2, z - 1)) - 1, z, ts);
      Assert.assertEquals("Bad ll lat", 90.0 - (res * ts), max.s, 0.0001);
      Assert.assertEquals("Bad ll lon", 180.0 - (res * ts), max.w, 0.0001);

      Assert.assertEquals("Bad ur lat", 90.0, max.n, 0.0001);
      Assert.assertEquals("Bad ur lon", 180.0, max.e, 0.0001);

      // the middle calculation doesn't work a level 1 (too few tiles)
      if (z > 1)
      {
        Bounds middle = TMSUtils.tileBounds((int)(2 * Math.pow(2, z - 1)) / 2, (int)(Math.pow(2, z - 1)) / 2, z, ts);

        Assert.assertEquals("Bad ll lat", 0.0, middle.s, 0.0001);
        Assert.assertEquals("Bad ll lon", 0.0, middle.w, 0.0001);

        Assert.assertEquals("Bad ur lat", 0.0 + (res * ts), middle.n, 0.0001);
        Assert.assertEquals("Bad ur lon", 0.0 + (res * ts), middle.e, 0.0001);
      }

    }

    Bounds ll = TMSUtils.tileBounds(tiles.getMinX(), tiles.getMinY(), zoom, ts);
    Bounds ur = TMSUtils.tileBounds(tiles.getMaxX(), tiles.getMaxY(), zoom, ts);

    Assert.assertEquals("Bad ll lat", tb.s, ll.s, 0.0001);
    Assert.assertEquals("Bad ll lon", tb.w, ll.w, 0.0001);

    Assert.assertEquals("Bad ur lat", tb.n, ur.s, 0.0001);
    Assert.assertEquals("Bad ur lon", tb.e, ur.w, 0.0001);
  }

  @Test
  @Category(UnitTest.class)
  public void tileid()
  {
    for (int tx = 0; tx < (int)(2 * Math.pow(2, zoom - 1)); tx++)
    {
      for (int ty = 0; ty < (int)(Math.pow(2, zoom - 1)); ty++)
      {
        // convert to long and back to tx/ty...
        long l = TMSUtils.tileid(tx, ty, zoom);

        TMSUtils.Tile id = TMSUtils.tileid(l, zoom);

        Assert.assertEquals("Bad tile x", tx, id.tx);
        Assert.assertEquals("Bad tile y", ty, id.ty);
      }
    }

    long l = TMSUtils.tileid(123, 456, zoom);
    Assert.assertEquals("Bad tile id", 467067, l);

    TMSUtils.Tile id = TMSUtils.tileid(123456, zoom);
    Assert.assertEquals("Bad tile x", 576, id.tx);
    Assert.assertEquals("Bad tile y", 120, id.ty);

  }


  @Test
  public void tileSWNEBounds()
  {
    // tileSWNEBounds just reorders the bounds, so the test is simple...
    double[] b1 = TMSUtils.tileBoundsArray(tiles.getMinX(), tiles.getMinY(), zoom, ts);
    double[] b2 = TMSUtils.tileBoundsArray(tiles.getMaxX(), tiles.getMaxY(), zoom, ts);

    double[] b3 = TMSUtils.tileSWNEBoundsArray(tiles.getMinX(), tiles.getMinY(), zoom, ts);
    double[] b4 = TMSUtils.tileSWNEBoundsArray(tiles.getMaxX(), tiles.getMaxY(), zoom, ts);

    Assert.assertEquals("Bounds mismatch", b1[0], b3[1], 0.0001);
    Assert.assertEquals("Bounds mismatch", b1[1], b3[0], 0.0001);
    Assert.assertEquals("Bounds mismatch", b1[2], b3[3], 0.0001);
    Assert.assertEquals("Bounds mismatch", b1[3], b3[2], 0.0001);

    Assert.assertEquals("Bounds mismatch", b2[0], b4[1], 0.0001);
    Assert.assertEquals("Bounds mismatch", b2[1], b4[0], 0.0001);
    Assert.assertEquals("Bounds mismatch", b2[2], b4[3], 0.0001);
    Assert.assertEquals("Bounds mismatch", b2[3], b4[2], 0.0001);
  }

  @Test
  @Category(UnitTest.class)
  public void zoomForPixelSize()
  {
    for (int i = 0; i < resolutions.length; i++)
    {
      int z = TMSUtils.zoomForPixelSize(resolutions[i], ts);

      Assert.assertEquals("Bad zoomlevel", i + 1, z);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBoundsIntersect() {
    Bounds b1 = new Bounds(141.7066, -18.3733, 142.5600, -17.5200);
    
    {
      // test when two bounds are exactly same
      Bounds b2 = b1.clone();
      Assert.assertEquals(true, b1.intersects(b2));
    }
    
    {
      // test when b2 sits on top of b1 (shares a boundary)
      Bounds b2 = new Bounds(b1.w, b1.n, b1.e, b1.n+1);
      Assert.assertEquals(true, b1.intersects(b2));
    }

    {
      // test when b2 is fully contained within b1 
      double smallDelta = 0.000001;
      Bounds b2 = new Bounds(b1.w-smallDelta, b1.n-smallDelta,
                                                b1.e-smallDelta, b1.n-smallDelta);
      Assert.assertEquals(true, b1.intersects(b2));
    }
    
    {
      // test when b2 has no overlap with b1 
      Bounds b2 = new Bounds(b1.e+1, b1.s, b1.e+2, b1.n);
      Assert.assertEquals(false, b1.intersects(b2));
    }
  
  }

  @Test
  @Category(UnitTest.class)
  public void testPixelToLatLon()
  {
    zoom = 1;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    long height = tileSize * (long)Math.pow(2.0, zoom - 1);
    long width = 2 * height;
    double resolution = TMSUtils.resolution(zoom, tileSize);
    // Check the bottom-left point
    TMSUtils.LatLon result = TMSUtils.pixelToLatLon(0, 0, zoom, tileSize);
    Assert.assertEquals(-180.0, result.lon, 1e-8);
    Assert.assertEquals(-90.0, result.lat, 1e-8);
    // Check the pixel left of center
    result = TMSUtils.pixelToLatLon(width / 2 - 1, 0, zoom, tileSize);
    Assert.assertEquals(0.0 - resolution, result.lon, 1e-8);
    Assert.assertEquals(-90.0, result.lat, 1e-8);
    // Check the pixel right of center
    result = TMSUtils.pixelToLatLon(width / 2, 0, zoom, tileSize);
    Assert.assertEquals(0.0, result.lon, 1e-8);
    Assert.assertEquals(-90.0, result.lat, 1e-8);
    // Return coordinates are top-left coordinate of the pixel
    result = TMSUtils.pixelToLatLon(width - 1, 0, zoom, tileSize);
    Assert.assertEquals(180.0 - resolution, result.lon, 1e-8);
    Assert.assertEquals(-90.0, result.lat, 1e-8);
    // Check the top-right point
    result = TMSUtils.pixelToLatLon(width - 1, height - 1, zoom, tileSize);
    Assert.assertEquals(180.0 - resolution, result.lon, 1e-8);
    Assert.assertEquals(90.0 - resolution, result.lat, 1e-8);
  }

  @Test
  @Category(UnitTest.class)
  public void testPixelToLatLonUL()
  {
    zoom = 1;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    long height = tileSize * (long)Math.pow(2.0, zoom - 1);
    long width = 2 * height;
    double resolution = TMSUtils.resolution(zoom, tileSize);
    // Check the bottom-left point
    TMSUtils.LatLon result = TMSUtils.pixelToLatLonUL(0, 0, zoom, tileSize);
    Assert.assertEquals(-180.0, result.lon, 1e-8);
    Assert.assertEquals(90.0 - resolution, result.lat, 1e-8);
    // Check the pixel left of center
    result = TMSUtils.pixelToLatLonUL(width / 2 - 1, 0, zoom, tileSize);
    Assert.assertEquals(0.0 - resolution, result.lon, 1e-8);
    Assert.assertEquals(90.0 - resolution, result.lat, 1e-8);
    // Check the pixel right of center
    result = TMSUtils.pixelToLatLonUL(width / 2, 0, zoom, tileSize);
    Assert.assertEquals(0.0, result.lon, 1e-8);
    Assert.assertEquals(90.0 - resolution, result.lat, 1e-8);
    // Return coordinates are top-left coordinate of the pixel
    result = TMSUtils.pixelToLatLonUL(width - 1, 0, zoom, tileSize);
    Assert.assertEquals(180.0 - resolution, result.lon, 1e-8);
    Assert.assertEquals(90.0 - resolution, result.lat, 1e-8);
    // Check the top-right point
    result = TMSUtils.pixelToLatLonUL(width - 1, height - 1, zoom, tileSize);
    Assert.assertEquals(180.0 - resolution, result.lon, 1e-8);
    Assert.assertEquals(-90.0, result.lat, 1e-8);
  }
}
