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

package org.mrgeo.buildpyramid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;


@SuppressWarnings("static-method")
public class BuildPyramidMapperTest
{
  private static Raster[] rasters;
  private static int tileids[] = {12345678, 3086503, 771667, 192937, 48212, 12042, 3013, 754, 189, 46};

  @BeforeClass
  public static void setUp() throws Exception
  {
    int tile = 512;

    rasters = new Raster[10];
    for (int i = 0; i < 10; i++)
    {
      rasters[i] = RasterUtils.createEmptyRaster(tile, tile, 1, DataBuffer.TYPE_DOUBLE, Double.NaN);
      tile /= 2;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    rasters = null;
  }


  @Test
  @Category(UnitTest.class)
  public void map()
  {
    Configuration config = new Configuration();
    config.setInt(BuildPyramidDriver.TO_LEVEL, 1);
    config.setInt(BuildPyramidDriver.FROM_LEVEL, 10);

    // only need to set the metadata params we'll use in the splitter
    MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setTilesize(512);
    metadata.setPyramid("foo");

    try
    {
      HadoopUtils.setMetadata(config, metadata);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      Assert.fail("Catastrophic exception");
    }

    TileIdWritable key = new TileIdWritable(tileids[0]);
    TileCollection<Raster> value;
    try
    {
      value = new TileCollection<Raster>();
      value.setTileid(key.get());
      value.set("foo", key.get(), rasters[0]);

      MapDriver<TileIdWritable, TileCollection<Raster>, TileIdZoomWritable, RasterWritable> driver = 
          new MapDriver<TileIdWritable, TileCollection<Raster>, TileIdZoomWritable, RasterWritable>()
          .withConfiguration(config)
          .withMapper(new BuildPyramidMapper())
          .withInputKey(key)
          .withInputValue(value);

      java.util.List<Pair<TileIdZoomWritable, RasterWritable>> results = driver.run();


      // Test the results
      Assert.assertEquals("Bad number of maps returned", 9, results.size());


      java.util.ListIterator<Pair<TileIdZoomWritable, RasterWritable>> iter = results.listIterator();

      Assert.assertTrue("Map iterator doesn't have a next item", iter.hasNext());

      for (int i = 1; i < 10; i++)
      {
        Pair<TileIdZoomWritable, RasterWritable> item = iter.next();

        Assert.assertEquals("Zoomlevel doesn't match output", 10 - i, item.getFirst().getZoom());
        // id 10 in level 4 is id 5 in level 5
        Assert.assertEquals("Tileid doesn't match output", tileids[i], item.getFirst().get());
        TestUtils.compareRasters(rasters[i], RasterWritable.toRaster(item.getSecond()));
      }
      
      // test the counters
      Assert.assertEquals("Tile count (counter) incorrect.", 1,
        driver.getCounters().findCounter("Build Pyramid Mapper", "Source Tiles Processed").getValue());

    }
    catch (Exception e)
    {
      e.printStackTrace();
      Assert.fail("Catastrophic Exception: " + e.getMessage());
    }

  }

}
