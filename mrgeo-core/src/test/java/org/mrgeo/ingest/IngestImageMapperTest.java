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

package org.mrgeo.ingest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.IOException;

public class IngestImageMapperTest
{
  private Raster raster;
  @Before
  public void setUp()
  {
    BufferedImage image = new BufferedImage(512, 512, ColorSpace.TYPE_RGB);
    raster = image.getData();

  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void setup()
  {
    //Assert.fail("Not yet implemented");
  }

  @Test
  @Category(UnitTest.class)
  public void map() throws Exception
  {
    Job job = new Job();
    Configuration config = job.getConfiguration();

    // only need to set the metadata params we'll use in the splitter
    MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setTilesize(512);
    
    try
    {
      HadoopUtils.setMetadata(config, metadata);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      Assert.fail("Catestrophic exception");
    }

    final AdHocDataProvider metadataProvider = DataProviderFactory.createAdHocDataProvider(
        HadoopUtils.createConfiguration());
    metadataProvider.setupJob(job);
    config.set("metadata.provider", metadataProvider.getResourceName());
    config.setInt("zoomlevel", metadata.getMaxZoomLevel());
    config.setInt("tilesize", metadata.getTilesize());
    config.setFloat("nodata", (float)metadata.getDefaultValueShort(0));
    config.setInt("bands", metadata.getBands());
    config.set("classification", Classification.Continuous.name());

    TileIdWritable key = new TileIdWritable(100);
    RasterWritable value;
    try
    {
      value = RasterWritable.toWritable(raster);

      MapDriver<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable> driver = 
          new MapDriver<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable>()
          .withConfiguration(config)
          .withMapper(new IngestImageMapper())
          .withInputKey(key)
          .withInputValue(value);

      java.util.List<Pair<TileIdWritable, RasterWritable>> results = driver.run();

      
      // Test the results
      Assert.assertEquals("Bad number of maps returned", 1, results.size());
      
      java.util.ListIterator<Pair<TileIdWritable, RasterWritable>> iter = results.listIterator();
      
      Assert.assertTrue("Map iterator doesn't have a next item", iter.hasNext());
      
      Pair<TileIdWritable, RasterWritable> item = iter.next();
      Assert.assertEquals("Input tileid doesn't match output", key.get(), item.getFirst().get());
      TestUtils.compareRasters(RasterWritable.toRaster(value), RasterWritable.toRaster(item.getSecond()));
      
      // test the counters
      Assert.assertEquals("Tile count (counter) incorrect.", 1,
          driver.getCounters().findCounter("Ingest Mapper", "Mapper Tiles Processed").getValue());
      metadataProvider.delete();
    }
    catch (Exception e)
    {
      Assert.fail("Catastrophic Exception" + e);
    }


  }

}
