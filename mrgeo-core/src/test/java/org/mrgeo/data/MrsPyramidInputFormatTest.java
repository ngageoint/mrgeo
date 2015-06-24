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

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.image.MrsImagePyramidInputFormat;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MrsPyramidInputFormatTest extends LocalRunnerTest
{
  private static String allones = Defs.INPUT + "all-ones";
  private static Bounds bounds;
  private static int zoomLevel = 10;
  private static int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
  private static LongRectangle imageTileBounds;

  @BeforeClass
  public static void setup() throws IOException
  {
    File f = new File(allones);
    allones = f.getCanonicalFile().toURI().toString();

    MrsImagePyramid p = MrsImagePyramid.open(allones, (Properties)null);
    bounds = p.getBounds();
    imageTileBounds = p.getTileBounds(zoomLevel); // (915, 203) (917, 206)
  }
//  @Before
//  public void setUp() throws Exception
//  {
//    Bounds bounds = Bounds.world;
//
//    fill = new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones), bounds, 0);
//    nofill = new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones), bounds);
//  }


  // Split is a single row of tiles that is completely to the left of the crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitDoesNotOverlapCropLeft() throws Exception
  {
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        50,
        20,
        50);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(1, 50, fill.getZoomLevel());
    long endId = TMSUtils.tileid(9, 50,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(null, filtered.get(0), crop.w, crop.s, crop.e, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Split is a single row of tiles that is completely to the right of the crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitDoesNotOverlapCropRight() throws Exception
  {
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        50,
        20,
        50);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(21, 50, fill.getZoomLevel());
    long endId = TMSUtils.tileid(29, 50,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(null, filtered.get(0), crop.w, crop.s, crop.e, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Split is a single row of tiles that is completely above the crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitDoesNotOverlapCropTop() throws Exception
  {
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        50,
        20,
        50);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(1, 51, fill.getZoomLevel());
    long endId = TMSUtils.tileid(29, 51,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(null, filtered.get(0), crop.w, crop.s, crop.e, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Split is a single row of tiles that is completely below the crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitDoesNotOverlapCropBottom() throws Exception
  {
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        50,
        20,
        50);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(1, 49, fill.getZoomLevel());
    long endId = TMSUtils.tileid(29, 49,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    compareSplit(null, filtered.get(0), crop.w, crop.s, crop.e, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Split is a single row of tiles that overlaps left bounds of the crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitOverlapsCropLeftEdge() throws Exception
  {
    int ty = 50;
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        ty,
        20,
        ty);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(5, ty, fill.getZoomLevel());
    long endId = TMSUtils.tileid(15, ty,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 2, filtered.size());
    compareSplit(fake, filtered.get(0), 5, ty, 15, ty,
        fill.getZoomLevel(), fill.getTileSize());
    compareSplit(null, filtered.get(1), 16, crop.s, crop.e, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Split is a single row of tiles that overlap the right bounds of the crop
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitOverlapsCropRightEdge() throws Exception
  {
    int ty = 50;
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        ty,
        20,
        ty);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(15, ty, fill.getZoomLevel());
    long endId = TMSUtils.tileid(25, ty,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 2, filtered.size());
    compareSplit(null, filtered.get(0), crop.w, crop.s, 14, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
    compareSplit(fake, filtered.get(1), 15, ty, 25, ty,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Split is a single row of tiles that overlap the middle of the crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitOverlapsCropMiddle() throws Exception
  {
    int ty = 50;
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        ty,
        20,
        ty);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(11, ty, fill.getZoomLevel());
    long endId = TMSUtils.tileid(19, ty,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 3, filtered.size());
    compareSplit(null, filtered.get(0), crop.w, crop.s, 10, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
    compareSplit(fake, filtered.get(1), 11, ty, 19, ty,
        fill.getZoomLevel(), fill.getTileSize());
    compareSplit(null, filtered.get(2), 20, crop.s, crop.e, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  // Crop bounds are a single row of tiles that overlap the entire crop region
  @Test
  @Category(UnitTest.class)
  public void testSingleRowSplitOverlapsCropEntire() throws Exception
  {
    int ty = 50;
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        ty,
        20,
        ty);
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    long startId = TMSUtils.tileid(8, ty, fill.getZoomLevel());
    long endId = TMSUtils.tileid(22, ty,  fill.getZoomLevel());

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    TiledInputSplit fake = new TiledInputSplit();
    splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));

    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", 1, filtered.size());
    // The resulting split is the same as the input split. Note that the MrsPyramidRecordReader is responsible for
    // skipping records within the split that do not overlap the crop region.
    compareSplit(fake, filtered.get(0), 8, crop.s, 22, crop.s,
        fill.getZoomLevel(), fill.getTileSize());
  }

  private class TestResult
  {
    public long txStart;
    public long tyStart;
    public long txEnd;
    public long tyEnd;
    public TiledInputSplit wrapped;

    public TestResult(long txStart, long tyStart, long txEnd, long tyEnd, TiledInputSplit wrapped)
    {
      this.txStart = txStart;
      this.tyStart = tyStart;
      this.txEnd = txEnd;
      this.tyEnd = tyEnd;
      this.wrapped = wrapped;
    }
  }

  private class TestSplit
  {
    public long txStart;
    public long tyStart;
    public long txEnd;
    public long tyEnd;

    public TestSplit(long txStart, long tyStart, long txEnd, long tyEnd)
    {
      this.txStart = txStart;
      this.tyStart = tyStart;
      this.txEnd = txEnd;
      this.tyEnd = tyEnd;
    }
  }

  private class TestSpec
  {
    public TestSplit[] splits;
    public TestResult[] result;
    
    public TestSpec(TestSplit[] splits, TestResult[] result)
    {
      this.splits = splits;
      this.result = result;
    }
  }

  // Run multiple tests with a single split that spans more than one row
  // within the crop region with various combinations of starting and ending
  // to the left of the crop region, to the right, above, below, at the start,
  // at the end, and in the middle.
  // Note that the actual split start/end tile id's are not cropped even when
  // they are outside the crop region. The record reader will ignore those
  // records at runtime (not in this unit test).
  @Test
  @Category(UnitTest.class)
  public void testFilterSingleInputSplitMultiRowCrop() throws Exception
  {
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        50,
        20,
        55);
    TiledInputSplit fake = new TiledInputSplit();
    TestSpec[] testSpecs = {
        new TestSpec(new TestSplit[] {
            new TestSplit(22, 49, 9, 50)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
                new TestSplit(22, 49, 10, 50)
            },
            new TestResult[] {
            new TestResult(22, 49, 10, 50, fake),
            new TestResult(11, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(9, 49, 10, 50)
        },
        new TestResult[] {
            new TestResult(9, 49, 10, 50, fake),
            new TestResult(11, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(9, 49, 15, 50)
        },
        new TestResult[] {
            new TestResult(9, 49, 15, 50, fake),
            new TestResult(16, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 20, 50)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, fake),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 9, 51)
        },
        new TestResult[] {
            new TestResult(10, 50,  9, 51, fake),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 10, 51)
        },
        new TestResult[] {
            new TestResult(10, 50, 10, 51, fake),
            new TestResult(11, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 15, 51)
        },
        new TestResult[] {
            new TestResult(10, 50, 15, 51, fake),
            new TestResult(16, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 20, 51)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 51, fake),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 50, 15, 51)
        },
        new TestResult[] {
            new TestResult(10, 50, 14, 50, null),
            new TestResult(15, 50, 15, 51, fake),
            new TestResult(16, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 51, 10, 52)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 10, 52, fake),
            new TestResult(11, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 51, 15, 52)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 15, 52, fake),
            new TestResult(16, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 51, 20, 52)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 52, fake),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 51, 15, 52)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 14, 51, null),
            new TestResult(15, 51, 15, 52, fake),
            new TestResult(16, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 51, 25, 52)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 14, 51, null),
            new TestResult(15, 51, 25, 52, fake),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 54, 9, 55)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 14, 54, null),
            new TestResult(15, 54,  9, 55, fake),
            new TestResult(10, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 54, 10, 55)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 14, 54, null),
            new TestResult(15, 54, 10, 55, fake),
            new TestResult(11, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(25, 54, 10, 55)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult(25, 54, 10, 55, fake),
            new TestResult(11, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 54, 15, 55)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 14, 54, null),
            new TestResult(15, 54, 15, 55, fake),
            new TestResult(16, 55, 20, 55, null)
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(15, 54, 20, 55)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 14, 54, null),
            new TestResult(15, 54, 20, 55, fake),
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(9, 55, 10, 55)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult( 9, 55, 10, 55, fake),
            new TestResult(11, 55, 20, 55, null),
            }
        ),
        new TestSpec(new TestSplit[] {
            new TestSplit(9, 55, 10, 56)
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            new TestResult(10, 54, 20, 54, null),
            new TestResult( 9, 55, 10, 56, fake),
            }
        ),
    };
    for (TestSpec spec : testSpecs)
    {
      runTestSpec(spec, crop, fake);
    }
  }

  // Run multiple tests including two splits where sometimes the splits are adjacent,
  // and sometimes they have a gap between them. Include various cases where the
  // splits start/end inside the crop region and outside.
  @Test
  @Category(UnitTest.class)
  public void testFilterTwoInputSplits() throws Exception
  {
    TMSUtils.TileBounds crop = new TMSUtils.TileBounds(10,
        50,
        20,
        53);
    TiledInputSplit fake = new TiledInputSplit();
    TestSpec[] testSpecs = {
        // Both splits before the crop start, all results are blank rows
        new TestSpec(new TestSplit[] {
            new TestSplit(1, 49, 1, 50),
            new TestSplit(5, 50, 9, 50),
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits to the left of the crop, all results are blank rows
        new TestSpec(new TestSplit[] {
            new TestSplit(1, 51, 9, 51),
            new TestSplit(2, 52, 9, 52),
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits to the right of the crop, all results are blank rows
        new TestSpec(new TestSplit[] {
            new TestSplit(21, 51, 29, 51),
            new TestSplit(23, 52, 23, 52),
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits beyond the crop, all results are blank rows
        new TestSpec(new TestSplit[] {
            new TestSplit(21, 53, 29, 53),
            new TestSplit(30, 53, 33, 53),
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // First split before crop, second spans left edge
        new TestSpec(new TestSplit[] {
            new TestSplit(1, 49, 1, 50),
            new TestSplit(8, 50, 12, 50),
        },
        new TestResult[] {
            new TestResult( 8, 50, 12, 50, fake),
            new TestResult(13, 50, 20, 50, null),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // First split before crop, second spans entire crop row
        new TestSpec(new TestSplit[] {
            new TestSplit(1, 49, 9, 50),
            new TestSplit(10, 50, 23, 50),
        },
        new TestResult[] {
            new TestResult(10, 50, 23, 50, fake),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits inside crop and adjacent
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 13, 50),
            new TestSplit(14, 50, 20, 50),
        },
        new TestResult[] {
            new TestResult(10, 50, 13, 50, fake),
            new TestResult(14, 50, 20, 50, fake),
            new TestResult(10, 51, 20, 51, null),
            new TestResult(10, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits inside crop and adjacent. Second one spans rows
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 13, 50),
            new TestSplit(14, 50, 15, 52),
        },
        new TestResult[] {
            new TestResult(10, 50, 13, 50, fake),
            new TestResult(14, 50, 15, 52, fake),
            new TestResult(16, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits inside crop and non-adjacent. Second one spans rows
        new TestSpec(new TestSplit[] {
            new TestSplit(10, 50, 13, 50),
            new TestSplit(17, 50, 15, 52),
        },
        new TestResult[] {
            new TestResult(10, 50, 13, 50, fake),
            new TestResult(14, 50, 16, 50, null),
            new TestResult(17, 50, 15, 52, fake),
            new TestResult(16, 52, 20, 52, null),
            new TestResult(10, 53, 20, 53, null),
            }
        ),
        // Both splits inside crop and non-adjacent. Second one spans beyond crop region
        new TestSpec(new TestSplit[] {
            new TestSplit(12, 51, 10, 52),
            new TestSplit(17, 52, 25, 54),
        },
        new TestResult[] {
            new TestResult(10, 50, 20, 50, null),
            new TestResult(10, 51, 11, 51, null),
            new TestResult(12, 51, 10, 52, fake),
            new TestResult(11, 52, 16, 52, null),
            new TestResult(17, 52, 25, 54, fake),
            }
        ),
    };
    for (TestSpec spec : testSpecs)
    {
      runTestSpec(spec, crop, fake);
    }
  }

  private void runTestSpec(TestSpec spec, TMSUtils.TileBounds crop, TiledInputSplit fake)
  {
    TiledInputFormatContext fill =
        new TiledInputFormatContext(zoomLevel, tileSize, Sets.newHashSet(allones),
            TMSUtils.tileToBounds(crop, zoomLevel, tileSize).convertNewToOldBounds(), 0, (Properties)null);

    List<TiledInputSplit> splits = new ArrayList<TiledInputSplit>();
    for (TestSplit split: spec.splits)
    {
      long startId = TMSUtils.tileid(split.txStart, split.tyStart, fill.getZoomLevel());
      long endId = TMSUtils.tileid(split.txEnd, split.tyEnd,  fill.getZoomLevel());

      splits.add(new TiledInputSplit(fake, startId, endId, fill.getZoomLevel(), fill.getTileSize()));
    }
    // need to use the derived class, since MrsPyramidInputFormat is abstract.
    MrsPyramidInputFormat<Raster> input = new MrsImagePyramidInputFormat();

    List<TiledInputSplit> filtered = input.filterInputSplits(fill, splits, fill.getZoomLevel(), fill.getTileSize());

    Assert.assertEquals("Wrong number of splits", spec.result.length, filtered.size());
    for (int i=0; i < spec.result.length; i++)
    {
      TestResult result = spec.result[i];
      compareSplit(result.wrapped, filtered.get(i), result.txStart, result.tyStart, result.txEnd, result.tyEnd,
          fill.getZoomLevel(), fill.getTileSize());
    }
  }

  private void compareSplit(TiledInputSplit expected, TiledInputSplit actual)
  {
    Assert.assertEquals("Wrapped split not equal!", expected.getWrappedSplit(), actual.getWrappedSplit());
    Assert.assertEquals("Start tileid not equal!", expected.getStartTileId(), actual.getStartTileId());
    Assert.assertEquals("End tileid not equal!", expected.getEndTileId(), actual.getEndTileId());
    Assert.assertEquals("Zoom level not equal!", expected.getZoomLevel(), actual.getZoomLevel());
    Assert.assertEquals("Tile size not equal!", expected.getTileSize(), actual.getTileSize());
  }

  private void compareSplit(String message,
      int txExpectedStart, int tyExpectedStart,
      int txExpectedEnd, int tyExpectedEnd,
      int expectedZoom, int expectedTileSize, TiledInputSplit actual)
  {
    long expectedStart = TMSUtils.tileid(txExpectedStart, tyExpectedStart, expectedZoom);
    long expectedEnd = TMSUtils.tileid(txExpectedEnd, tyExpectedEnd, expectedZoom);
    Assert.assertEquals(message + ": start tileid not equal!", expectedStart, actual.getStartTileId());
    Assert.assertEquals(message + " end tileid not equal!", expectedEnd, actual.getEndTileId());
    Assert.assertEquals(message + " zoom level not equal!", expectedZoom, actual.getZoomLevel());
    Assert.assertEquals(message + " tile size not equal!", expectedTileSize, actual.getTileSize());
  }

  private void compareSplit(InputSplit expectedWrappedSplit, TiledInputSplit actual,
      long expStTx, long expStTy, long expEnTx, long expEnTy, int expectedZoom, int expectedTileSize)
  {
    Assert.assertEquals("Zoom level not equal!", expectedZoom, actual.getZoomLevel());
    Assert.assertEquals("Tile size not equal!", expectedTileSize, actual.getTileSize());
    Assert.assertEquals("Wrapped split not equal!", expectedWrappedSplit, actual.getWrappedSplit());
    Assert.assertEquals("Start tileid not equal, expected " + expStTx + ", " + expStTy + " got " + TMSUtils.tileid(actual.getStartTileId(), expectedZoom).toString(),
        TMSUtils.tileid(expStTx, expStTy, expectedZoom), actual.getStartTileId());
    Assert.assertEquals("End tileid not equal, expected "  + expEnTx + ", " + expEnTy + " got " + TMSUtils.tileid(actual.getEndTileId(), expectedZoom).toString(), TMSUtils.tileid(expEnTx, expEnTy, expectedZoom), actual.getEndTileId());
  }
}
