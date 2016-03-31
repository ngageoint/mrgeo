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

package org.mrgeo.hdfs.image;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.image.MrsPyramidReaderContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;

public class HdfsMrsImageReaderTest extends LocalRunnerTest
{

  private HdfsMrsImageReader reader = null;


  // all-ones resultTiles (level 10)
  // "minX" : 915,
  // "minY" : 203,
  // "maxX" : 917,
  // "maxY" : 206

  //  id: 208787 tx: 915 ty: 203
  //  id: 208788 tx: 916 ty: 203
  //  id: 208789 tx: 917 ty: 203
  //  id: 209811 tx: 915 ty: 204
  //  id: 209812 tx: 916 ty: 204
  //  id: 209813 tx: 917 ty: 204
  //  id: 210835 tx: 915 ty: 205
  //  id: 210836 tx: 916 ty: 205
  //  id: 210837 tx: 917 ty: 205
  //  id: 211859 tx: 915 ty: 206
  //  id: 211860 tx: 916 ty: 206
  //  id: 211861 tx: 917 ty: 206

  private static final long[] resultTiles = {
      208787, // tx: 915 ty: 203
      208788, // tx: 916 ty: 203
      208789, // tx: 917 ty: 203
      209811, // tx: 915 ty: 204
      209812, // tx: 916 ty: 204
      209813, // tx: 917 ty: 204
      210835, // tx: 915 ty: 205
      210836, // tx: 916 ty: 205
      210837, // tx: 917 ty: 205
      211859, // tx: 915 ty: 206
      211860, // tx: 916 ty: 206
      211861 // tx: 917 ty: 206
  };

  private static String all_ones = Defs.INPUT + "all-ones";
  private static final int zoomlevel = 10;

  @BeforeClass
  static public void setup() throws IOException
  {
    File f = new File(all_ones);
    all_ones = f.toURI().toString();

    // strip the final "/" from the path
    if (all_ones.lastIndexOf("/") == all_ones.length() - 1)
    {
      all_ones = all_ones.substring(0, all_ones.length() - 1);
    }
  }

  @Before
  public void init() throws IOException
  {
    HdfsMrsImageDataProvider provider = new HdfsMrsImageDataProvider(conf, all_ones, null);
    MrsPyramidReaderContext context = new MrsPyramidReaderContext(zoomlevel);

    reader = new HdfsMrsImageReader(provider, context);
  }


  @Test
  @Category(UnitTest.class)
  public void testGetZoomlevel() throws Exception
  {
    Assert.assertEquals("Bad zoom level!", zoomlevel, reader.getZoomlevel());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllRect() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 203, 917, 206);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllRectMinMax() throws Exception
  {
    LongRectangle tiles = new LongRectangle(Integer.MIN_VALUE, Integer.MIN_VALUE,
        Integer.MAX_VALUE, Integer.MAX_VALUE);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllRectMax() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 203, Integer.MAX_VALUE, Integer.MAX_VALUE);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllRectMin() throws Exception
  {
    LongRectangle tiles = new LongRectangle(Integer.MIN_VALUE, Integer.MIN_VALUE,
        917, 206);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGet1stHalfRect() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 203, 917, 204);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx);
  }

  @Test
  @Category(UnitTest.class)
  public void testGet1stHalfRectMin() throws Exception
  {
    LongRectangle tiles = new LongRectangle(Integer.MIN_VALUE, Integer.MIN_VALUE,
        917, 204);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx);
  }
  @Test
  @Category(UnitTest.class)
  public void testGetLastHalfRect() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 205, 917, 206);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int start = 6;
    int ndx = start;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx - start);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLastHalfRectMax() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 205,
        Integer.MAX_VALUE, Integer.MAX_VALUE);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int start = 6;
    int ndx = start;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx - start);
  }


  @Test
  @Category(UnitTest.class)
  public void testGetRectOutOfBoundsHigh() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 210, 917, 215);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int cnt = 0;
    while(iter.hasNext())
    {
      cnt++;
    }

    Assert.assertEquals("Shouldn't have gotten any tiles!", 0, cnt);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRectOutOfBoundsLow() throws Exception
  {
    LongRectangle tiles = new LongRectangle(915, 100, 917, 110);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int cnt = 0;
    while(iter.hasNext())
    {
      cnt++;
    }

    Assert.assertEquals("Shouldn't have gotten any tiles!", 0, cnt);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRectOutOfBounds() throws Exception
  {
    LongRectangle tiles = new LongRectangle(900, 190, 910, 200);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    int cnt = 0;
    while (iter.hasNext())
    {
      cnt++;
    }

    Assert.assertEquals("Shouldn't have gotten any tiles!", 0, cnt);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllTileid() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(208787),
        new TileIdWritable(211861));

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllTileidMinMax() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(Long.MIN_VALUE),
        new TileIdWritable(Long.MAX_VALUE));


    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllTileidMax() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(208787),
        new TileIdWritable(Long.MAX_VALUE));

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGetAllTileidMin() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(Long.MIN_VALUE),
        new TileIdWritable(211861));

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testGet1stHalfTileid() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(208787),
        new TileIdWritable(209813));

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx);
  }

  @Test
  @Category(UnitTest.class)
  public void testGet1stHalfTileidMin() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(Long.MIN_VALUE),
        new TileIdWritable(209813));

    int ndx = 0;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx);
  }
  @Test
  @Category(UnitTest.class)
  public void testGetLastHalfTileid() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(210835),
        new TileIdWritable(211861));

    int start = 6;
    int ndx = start;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx - start);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLastHalfTileidMax() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(210835),
        new TileIdWritable(Long.MAX_VALUE));

    int start = 6;
    int ndx = start;
    while(iter.hasNext())
    {
      Assert.assertEquals("Unexpected tileid: ", resultTiles[ndx++], iter.currentKey().get());
    }

    Assert.assertEquals("Wrong number of items", 6, ndx - start);
  }


  @Test
  @Category(UnitTest.class)
  public void testTileidGetOutOfBoundsHigh() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(222222),
        new TileIdWritable(223222));

    int cnt = 0;
    while(iter.hasNext())
    {
      cnt++;
    }

    Assert.assertEquals("Shouldn't have gotten any tiles!", 0, cnt);
  }

  @Test
  @Category(UnitTest.class)
  public void testTileidGetOutOfBoundsLow() throws Exception
  {
    KVIterator<TileIdWritable, Raster> iter = reader.get(new TileIdWritable(111111),
        new TileIdWritable(112111));

    int cnt = 0;
    while(iter.hasNext())
    {
      cnt++;
    }

    Assert.assertEquals("Shouldn't have gotten any tiles!", 0, cnt);
  }


  @Test
  @Category(UnitTest.class)
  public void testGetWritableSize() throws Exception
  {
      LongRectangle tiles = new LongRectangle(Integer.MIN_VALUE, Integer.MIN_VALUE,
          Integer.MAX_VALUE, Integer.MAX_VALUE);

      KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

      while(iter.hasNext())
      {
        RasterWritable writable = RasterWritable.toWritable(iter.currentValue());
        Assert.assertEquals("Unexpected size: ", writable.getSize(), reader.getWritableSize(writable));

        // only need to do one...
        return;
      }

      Assert.fail("Didn't retrieve any rasters");
    }

  @Test
  @Category(UnitTest.class)
  public void testToNonWritable() throws Exception
  {
    LongRectangle tiles = new LongRectangle(Integer.MIN_VALUE, Integer.MIN_VALUE,
        Integer.MAX_VALUE, Integer.MAX_VALUE);

    KVIterator<TileIdWritable, Raster> iter = reader.get(tiles);

    while(iter.hasNext())
    {
      RasterWritable writable = RasterWritable.toWritable(iter.currentValue());
      Raster raster = reader.toNonWritable(writable);

      TestUtils.compareRasters(iter.currentValue(), raster);

      // only need to do one
      return;
    }

    Assert.fail("Didn't retrieve any rasters");

  }

  @Test
  @Category(UnitTest.class)
  public void testGetWithBoundsOutsideToLeft() throws Exception
  {
    Bounds bounds = new Bounds(120.0, -18.0, 122.0, -17.0);
    KVIterator<Bounds, Raster> results = reader.get(bounds);
    Assert.assertFalse(results.hasNext());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetWithBoundsOutsideToRight() throws Exception
  {
    Bounds bounds = new Bounds(150.0, -18.0, 152.0, -17.0);
    KVIterator<Bounds, Raster> results = reader.get(bounds);
    Assert.assertFalse(results.hasNext());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetWithBoundsOutsideAbove() throws Exception
  {
    Bounds bounds = new Bounds(141.5, -16.0, 142.5, -15.0);
    KVIterator<Bounds, Raster> results = reader.get(bounds);
    Assert.assertFalse(results.hasNext());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetWithBoundsOutsideBelow() throws Exception
  {
    Bounds bounds = new Bounds(141.5, -20.0, 142.5, -19.0);
    KVIterator<Bounds, Raster> results = reader.get(bounds);
    Assert.assertFalse(results.hasNext());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetWithBoundsSingleTile() throws Exception
  {
    Bounds bounds = new Bounds(141.8, -18.0, 141.9, -17.9);
    KVIterator<Bounds, Raster> results = reader.get(bounds);
    Assert.assertTrue(results.hasNext());
    int count = 0;
    while (results.hasNext())
    {
      count++;
    }
    Assert.assertEquals("One tile should have been returned", 1, count);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetWithBoundsAcrossTiles() throws Exception
  {
    Bounds bounds = new Bounds(141.95, -18.6, 142.1, -18.3);
    KVIterator<Bounds, Raster> results = reader.get(bounds);
    int count = 0;
    while (results.hasNext())
    {
      count++;
    }
    Assert.assertEquals("Two tiles should have been returned", 2, count);
  }
}