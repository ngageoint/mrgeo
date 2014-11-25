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

package org.mrgeo.data;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.data.image.MrsImagePyramidInputFormat;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;

import com.google.common.collect.Sets;

public class MrsPyramidInputFormatTest extends LocalRunnerTest
{
  private static String allones = Defs.INPUT + "all-ones";
  private static Bounds bounds;

  @BeforeClass
  public static void setup() throws IOException
  {
    File f = new File(allones);
    allones = f.getCanonicalFile().toURI().toString();

    MrsImagePyramid p = MrsImagePyramid.open(allones, (Properties)null);
    bounds = p.getBounds();
  }
//  @Before
//  public void setUp() throws Exception
//  {
//    Bounds bounds = Bounds.world;
//
//    fill = new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0);
//    nofill = new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds);
//  }


  // Bounds equal to the bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill1() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    long startId = TMSUtils.tileid(tb.w, tb.s, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.n, nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0));

  }

  // Bounds totally within the bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill2() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    long startId = TMSUtils.tileid(tb.w + 1, tb.s + 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e - 1, tb.n - 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
      compareSplit(splits.get(0), filtered.get(0));

  }

  // Bounds totally encompass the bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill3() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    long startId = TMSUtils.tileid(tb.w - 1, tb.s - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.n + 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap left  bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill4() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're only using 1 row!
    long startId = TMSUtils.tileid(tb.w - 1, tb.s, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e - 1, tb.s,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 916, 203);
  }

  // Bounds overlap right bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill5() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're only using 1 row!
    long startId = TMSUtils.tileid(tb.e - 1, tb.s, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.s,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 916, 203, 917, 203);
  }

  // Bounds overlap bottom bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill6() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w, tb.s - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.s,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 203);
  }

  // Bounds overlap top bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill7() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're only using 1 row!
    long startId = TMSUtils.tileid(tb.w, tb.n, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.n + 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 206, 917, 206);
  }

  // Bounds overlap top-left bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill8() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w - 1, tb.n - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w + 1, tb.n + 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 205, 916, 206);
  }

  // Bounds overlap top-right bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill9() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e - 1, tb.n - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.n + 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 916, 205, 917, 206);
  }

  // Bounds overlap bottom-right bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill10() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e - 1, tb.s - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.s + 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 916, 203, 917, 204);
  }

  // Bounds overlap bottom-left bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill11() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w - 1, tb.s - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w + 1, tb.s + 1,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 916, 204);
  }

  // Bounds totally to left of bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill12() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w - 10, tb.s, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w - 5, tb.s,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 0, filtered.size());
  }

  // Bounds totally to right of bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill13() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e + 5, tb.s, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 10, tb.s,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 0, filtered.size());
  }

  // Bounds totally above bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill14() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e, tb.n + 5, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w, tb.n + 10,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 0, filtered.size());
  }

  // Bounds totally below bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill15() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w, tb.s - 10, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.s = 5,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 0, filtered.size());
  }

  // multiple splits
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill16() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();

    for (long y = tb.s; y <= tb.n; y++)
    {
      long startId = TMSUtils.tileid(tb.w, y, nofill.getZoomLevel());
      long endId = TMSUtils.tileid(tb.e, y, nofill.getZoomLevel());

      splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));
    }

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    for (int i = 0; i < splits.size(); i++)
    {
      compareSplit(splits.get(i), filtered.get(i));
    }
  }

  // reverse splits (tests sorting)
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoFill17() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();

    for (long y = tb.n; y > tb.n; y--)
    {
      long startId = TMSUtils.tileid(tb.w, y, nofill.getZoomLevel());
      long endId = TMSUtils.tileid(tb.e, y, nofill.getZoomLevel());

      splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));
    }

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nofill, splits, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    for (int i = 0; i < splits.size(); i++)
    {
      compareSplit(splits.get(splits.size() - (i + 1)), filtered.get(i));
    }
  }

  // Bounds equal to the bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill1() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    long startId = TMSUtils.tileid(tb.w, tb.s, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.n, fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0));

  }

  // Bounds totally within the bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill2() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    long startId = TMSUtils.tileid(tb.w + 1, tb.s + 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e - 1, tb.n - 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);

  }

  // Bounds totally encompass the bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill3() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    long startId = TMSUtils.tileid(tb.w - 1, tb.s - 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.n + 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap left  bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill4() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're only using 1 row!
    long startId = TMSUtils.tileid(tb.w - 1, tb.s, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e - 1, tb.s,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap right bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill5() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're only using 1 row!
    long startId = TMSUtils.tileid(tb.e - 1, tb.s, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.s,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap bottom bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill6() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w, tb.s - 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.s,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap top bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill7() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're only using 1 row!
    long startId = TMSUtils.tileid(tb.w, tb.n, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.n + 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap top-left bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill8() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w - 1, tb.n - 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w + 1, tb.n + 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap top-right bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill9() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e - 1, tb.n - 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.n + 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap bottom-right bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill10() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e - 1, tb.s - 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 1, tb.s + 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds overlap bottom-left bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill11() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w - 1, tb.s - 1, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w + 1, tb.s + 1,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds totally to left of bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill12() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w - 10, tb.s, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w - 5, tb.s,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds totally to right of bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill13() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e + 5, tb.s, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e + 10, tb.s,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds totally above bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill14() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.e, tb.n + 5, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.w, tb.n + 10,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster>input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // Bounds totally below bounds of the image
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill15() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w, tb.s - 10, fill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.s = 5,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 206);
  }

  // multiple splits
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill16() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();

    for (long y = tb.s; y <= tb.n; y++)
    {
      long startId = TMSUtils.tileid(tb.w, y, fill.getZoomLevel());
      long endId = TMSUtils.tileid(tb.e, y, fill.getZoomLevel());

      splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));
    }

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    for (int i = 0; i < splits.size(); i++)
    {
      compareSplit(splits.get(i), filtered.get(i));
    }
  }

  // crazy gaps in the splits
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill17() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    long startId;
    long endId;

    // start after the 1st tile
    startId = TMSUtils.tileid(tb.w + 1, tb.s, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e, tb.s,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // row starts/ends outside the row
    startId = TMSUtils.tileid(tb.w - 1, tb.s + 1, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e + 1, tb.s + 1,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // gap between rows
    startId = TMSUtils.tileid(tb.w, tb.s + 2, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.w + 1, tb.s + 2,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    startId = TMSUtils.tileid(tb.e - 1, tb.s + 2, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e, tb.s + 2,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // ends before the last tile
    startId = TMSUtils.tileid(tb.w, tb.n, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e - 1, tb.n,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 203);
    compareSplit(splits.get(1), filtered.get(1), 915, 204, 917, 204);
    compareSplit(splits.get(2), filtered.get(2), 915, 205, 916, 205);
    compareSplit(splits.get(3), filtered.get(3), 916, 205, 917, 205);
    compareSplit(splits.get(4), filtered.get(4), 915, 206, 917, 206);

  }

  // crazy gaps in the splits, random order (test sort)
  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsFill18() throws Exception
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, 0, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(fill.getBounds()), fill.getZoomLevel(), fill.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    long startId;
    long endId;

    // gap between rows
    startId = TMSUtils.tileid(tb.w, tb.s + 2, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.w + 1, tb.s + 2,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // row starts/ends outside the row
    startId = TMSUtils.tileid(tb.w - 1, tb.s + 1, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e + 1, tb.s + 1,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // gap between rows
    startId = TMSUtils.tileid(tb.e - 1, tb.s + 2, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e, tb.s + 2,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // ends before the last tile
    startId = TMSUtils.tileid(tb.w, tb.n, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e - 1, tb.n,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // start after the 1st tile
    startId = TMSUtils.tileid(tb.w + 1, tb.s, fill.getZoomLevel());
    endId = TMSUtils.tileid(tb.e, tb.s,  fill.getZoomLevel());
    splits.add(new TiledInputSplit(null, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    compareSplit(splits.get(0), filtered.get(0), 915, 203, 917, 203);
    compareSplit(splits.get(1), filtered.get(1), 915, 204, 917, 204);
    compareSplit(splits.get(2), filtered.get(2), 915, 205, 916, 205);
    compareSplit(splits.get(3), filtered.get(3), 916, 205, 917, 205);
    compareSplit(splits.get(4), filtered.get(4), 915, 206, 917, 206);

  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testFilterInputSplitsNullContext() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(nofill.getBounds()), nofill.getZoomLevel(), nofill.getTileSize());;

    // note, we're using 2 rows
    long startId = TMSUtils.tileid(tb.w, tb.s - 1, nofill.getZoomLevel());
    long endId = TMSUtils.tileid(tb.e, tb.s,  nofill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    splits.add(new TiledInputSplit(null, startId, endId, nofill.getZoomLevel(), nofill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    input.filterInputSplits(null, splits, nofill.getZoomLevel(), nofill.getTileSize());

  }

  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoBounds() throws Exception
  {
    TiledInputFormatContext nobounds =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(bounds), nobounds.getZoomLevel(), nobounds.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();

    for (long y = tb.s; y <= tb.n; y++)
    {
      long startId = TMSUtils.tileid(tb.w, y, nobounds.getZoomLevel());
      long endId = TMSUtils.tileid(tb.e, y, nobounds.getZoomLevel());

      splits.add(new TiledInputSplit(null, startId, endId, nobounds.getZoomLevel(), nobounds.getTileSize()));
    }

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nobounds, splits, nobounds.getZoomLevel(), nobounds.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    for (int i = 0; i < splits.size(); i++)
    {
      compareSplit(splits.get(i), filtered.get(i));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testFilterInputSplitsNoBoundsOutsideImage() throws Exception
  {
    TiledInputFormatContext nobounds =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), null);

    TMSUtils.TileBounds tb = TMSUtils
        .boundsToTile(TMSUtils.Bounds.asTMSBounds(bounds), nobounds.getZoomLevel(), nobounds.getTileSize());;

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();

    for (long y = tb.s - 1; y <= tb.n + 1; y++)
    {
      long startId = TMSUtils.tileid(tb.w - 1, y, nobounds.getZoomLevel());
      long endId = TMSUtils.tileid(tb.e + 1, y, nobounds.getZoomLevel());

      splits.add(new TiledInputSplit(null, startId, endId, nobounds.getZoomLevel(), nobounds.getTileSize()));
    }

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(nobounds, splits, nobounds.getZoomLevel(), nobounds.getTileSize());

    Assert.assertEquals("Wrong number of splits", splits.size(), filtered.size());
    for (int i = 0; i < splits.size(); i++)
    {
      compareSplit(splits.get(i), filtered.get(i));
    }
  }


  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testFilterInputSplitsNullSplits() throws Exception
  {
    TiledInputFormatContext nofill =
        new TiledInputFormatContext(10, 512, Sets.newHashSet(allones), bounds, (Properties)null);

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(null, null, nofill.getZoomLevel(), nofill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
  }



  private void compareSplit(TiledInputSplit expected, TiledInputSplit actual)
  {
    Assert.assertEquals("Wrapped split not equal!", expected.getWrappedSplit(), actual.getWrappedSplit());
    Assert.assertEquals("Start tileid not equal!", expected.getStartTileId(), actual.getStartTileId());
    Assert.assertEquals("End tileid not equal!", expected.getEndTileId(), actual.getEndTileId());
    Assert.assertEquals("Zoom level not equal!", expected.getZoomLevel(), actual.getZoomLevel());
    Assert.assertEquals("Tile size not equal!", expected.getTileSize(), actual.getTileSize());
  }

  private void compareSplit(TiledInputSplit expected, TiledInputSplit actual, long expStTx, long expStTy, long expEnTx,long expEnTy)
  {
    Assert.assertEquals("Wrapped split not equal!", expected.getWrappedSplit(), actual.getWrappedSplit());
    Assert.assertEquals("Start tileid not equal!", TMSUtils.tileid(expStTx, expStTy, expected.getZoomLevel()), actual.getStartTileId());
    Assert.assertEquals("End tileid not equal!", TMSUtils.tileid(expEnTx, expEnTy, expected.getZoomLevel()), actual.getEndTileId());
    Assert.assertEquals("Zoom level not equal!", expected.getZoomLevel(), actual.getZoomLevel());
    Assert.assertEquals("Tile size not equal!", expected.getTileSize(), actual.getTileSize());
  }
}
