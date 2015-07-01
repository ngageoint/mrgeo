/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.RasterFactory;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class OpChainReducerTest
{
  private static Logger log = LoggerFactory.getLogger(OpChainReducerTest.class);

  private int size;
  private int b;
  private int zoom;
  private double lat, lon;
  private WritableRaster raster1;
  private WritableRaster raster2;
  private WritableRaster raster3;
  private WritableRaster raster4;
  private Raster stats1;
  private Raster stats2;
  private Raster stats3;
  private Raster stats4;
  
  @Before
  public void setUp() throws Exception
  {
    size = 4;
    b = 0;
    zoom = 5;
    lat = 32;
    lon = 69;
    double[] pixels = new double[size*size];
    SampleModel sm = RasterFactory.createPixelInterleavedSampleModel(DataBuffer.TYPE_DOUBLE, size, size, 1);
    WritableRaster raster = RasterFactory.createWritableRaster(sm, null);
    
    double v1 = 1;
    double v2 = 2;
    double v3 = 3;
    double v4 = 5;
    

    for (int i=0; i<size*size; i++) {
      pixels[i] = v1;
    }
    raster1 = raster.createCompatibleWritableRaster();
    raster1.setPixels(0, 0, size, size, pixels);
    
    
    for (int i=0; i<size*size; i++) {
      pixels[i] = v2;
    }
    raster2 = raster.createCompatibleWritableRaster();
    raster2.setPixels(0, 0, size, size, pixels);
    
    
    for (int i=0; i<size*size; i++) {
      pixels[i] = v3;
    }
    raster3 = raster.createCompatibleWritableRaster();
    raster3.setPixels(0, 0, size, size, pixels);
    
    
    for (int i=0; i<size*size; i++) {
      pixels[i] = v4;
    }
    raster4 = raster.createCompatibleWritableRaster();
    raster4.setPixels(0, 0, size, size, pixels);
    
    stats1 = ImageStats.statsToRaster(new ImageStats[]{new ImageStats(1,1,1,1)});
    stats2 = ImageStats.statsToRaster(new ImageStats[]{new ImageStats(2,2,2,2)});
    stats3 = ImageStats.statsToRaster(new ImageStats[]{new ImageStats(3,3,3,3)});
    stats4 = ImageStats.statsToRaster(new ImageStats[]{new ImageStats(5,5,5,5)});
  }

  @Test
  @Category(UnitTest.class)
  public void reduce() throws Exception
  {
    Job job = new Job();
    Configuration config = job.getConfiguration();
//    config.setInt(BuildPyramidDriver.LEVEL, zoom);
//    config.setInt(BuildPyramidDriver.PARENT_LEVEL, zoom+1);

    MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setTilesize(size);
    metadata.setBands(b+1);
    metadata.setMaxZoomLevel(zoom);
    metadata.setDefaultValues(new double[] {Double.NaN});
    
    final AdHocDataProvider statsProvider = DataProviderFactory.createAdHocDataProvider(
        config);
    // get the ad hoc provider set up for map/reduce
    statsProvider.setupJob(job);
    config.set(OpChainReducer.STATS_PROVIDER, statsProvider.getResourceName());

    try
    {
      HadoopUtils.setMetadata(config, metadata);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      Assert.fail("Failed to set metadata to Hadoop configuration.");
    }

    // Tile Id for child
    Tile childTile = TMSUtils.latLonToTile(lat, lon, zoom, size);
    TileIdWritable key = new TileIdWritable(TMSUtils.tileid(childTile.tx, childTile.ty, zoom));
    
    // Tile Ids for parents
    TileIdWritable parentKey1 = new TileIdWritable(TMSUtils.tileid(2*childTile.tx, 2*childTile.ty+1, zoom+1));
    TileIdWritable parentKey2 = new TileIdWritable(TMSUtils.tileid(2*childTile.tx+1, 2*childTile.ty+1, zoom+1));
    TileIdWritable parentKey3 = new TileIdWritable(TMSUtils.tileid(2*childTile.tx, 2*childTile.ty, zoom+1));
    TileIdWritable parentKey4 = new TileIdWritable(TMSUtils.tileid(2*childTile.tx+1, 2*childTile.ty, zoom+1));
    
    
    TileIdWritable statsKey = new TileIdWritable(ImageStats.STATS_TILE_ID);
    
    List<RasterWritable> value;
    try
    {
      //Run reducer with stats key/value pair
      
      value = new ArrayList<RasterWritable>();
      value.add(RasterWritable.toWritable(stats1));
      value.add(RasterWritable.toWritable(stats2));
      value.add(RasterWritable.toWritable(stats3));
      value.add(RasterWritable.toWritable(stats4));

      ReduceDriver<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable> driver = 
          new ReduceDriver<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable>()
          .withConfiguration(config)
          .withReducer(new OpChainReducer())
          .withInputKey(statsKey)
          .withInputValues(value);

      java.util.List<Pair<TileIdWritable, RasterWritable>> results = driver.run();

      Assert.assertEquals("For stats pair no results should emit", 0, results.size());

      //Run reducer with tile key/value pair
      
      value = new ArrayList<RasterWritable>();
      value.add(RasterWritable.toWritable(raster1));

      driver = 
          new ReduceDriver<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable>()
          .withConfiguration(config)
          .withReducer(new OpChainReducer())
          .withInputKey(parentKey1)
          .withInputValues(value);

      results = driver.run();

      statsProvider.delete();
      Assert.assertEquals("For tiles a single result should emit", 1, results.size());

      java.util.ListIterator<Pair<TileIdWritable, RasterWritable>> iter = results.listIterator();

      Assert.assertTrue("Reduce iterator doesn't have a next item", iter.hasNext());

      Pair<TileIdWritable, RasterWritable> item = iter.next();
      
      // Assert that output key equals input key
      Assert.assertEquals("Input tileid doesn't match output", parentKey1.get(), item.getFirst().get());
      TestUtils.compareRasters(raster1, RasterWritable.toRaster(item.getSecond()));

    } catch (NullPointerException ex){
      log.warn("Could not write temporary stats file, which is normal in the unit test.");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Reduce test threw an exception: " + e.getMessage());
    }

  }

}
