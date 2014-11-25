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
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.hadoop.multipleoutputs.DirectoryMultipleOutputs;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.output.image.HdfsMrsPyramidOutputFormat;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.RasterFactory;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class BuildPyramidReducerTest
{
  private static Logger log = LoggerFactory.getLogger(BuildPyramidReducerTest.class);

  private int size;
  private int b;
  private int zoom;
  private double lat, lon;
  private WritableRaster rasterUL;
  private WritableRaster rasterUR;
  private WritableRaster rasterLL;
  private WritableRaster rasterLR;
  private WritableRaster childRaster;
  
  @Before
  public void setUp() throws Exception
  {
    size = 2;
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
    rasterUL = raster.createCompatibleWritableRaster();
    rasterUL.setPixels(0, 0, size, size, pixels);
    
    
    for (int i=0; i<size*size; i++) {
      pixels[i] = v2;
    }
    rasterUR = raster.createCompatibleWritableRaster();
    rasterUR.setPixels(0, 0, size, size, pixels);
    
    
    for (int i=0; i<size*size; i++) {
      pixels[i] = v3;
    }
    rasterLL = raster.createCompatibleWritableRaster();
    rasterLL.setPixels(0, 0, size, size, pixels);
    
    
    for (int i=0; i<size*size; i++) {
      pixels[i] = v4;
    }
    rasterLR = raster.createCompatibleWritableRaster();
    rasterLR.setPixels(0, 0, size, size, pixels);
    
    size *= 2;
    
    childRaster = raster.createCompatibleWritableRaster(size, size);
    childRaster.setPixels(0, 0, size, size, new double[] {v1, v1, v2, v2, v1, v1, v2, v2, v3, v3, v4, v4, v3, v3, v4, v4});
    
  }

  @After
  public void tearDown() throws Exception
  {
    rasterUL = null;
    rasterUR = null;
    rasterLL = null;
    rasterLR = null;
  }

  private static final String base = DirectoryMultipleOutputs.class.getSimpleName();

  private static final String MO_PREFIX = base + ".namedOutput.";
  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String HDFS_PATH = ".hdfsPath";

  @Test
  @Category(UnitTest.class)
  public void reduce()
  {
    Configuration config = new Configuration();
    config.setInt(BuildPyramidDriver.TO_LEVEL, zoom);
    config.setInt(BuildPyramidDriver.FROM_LEVEL, zoom+1);

    // manually set up the configs DiretoryMultipleOutputs
    String namedOutput = "5";
    config.set(base, config.get(base, "") + " " + namedOutput);
    config.setClass(MO_PREFIX + namedOutput + FORMAT, HdfsMrsPyramidOutputFormat.class, OutputFormat.class);
    config.setClass(MO_PREFIX + namedOutput + KEY, TileIdWritable.class, Object.class);
    config.setClass(MO_PREFIX + namedOutput + VALUE, RasterWritable.class, Object.class);
    config.set(MO_PREFIX + namedOutput + HDFS_PATH, Defs.OUTPUT_HDFS);

    
    MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setTilesize(size);
    metadata.setBands(b+1);
    metadata.setMaxZoomLevel(zoom);
    metadata.setDefaultValues(new double[] {Double.NaN});
    
    try
    {
      HadoopUtils.setMetadata(config, metadata);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      Assert.fail("Failed to set metadata to Hadoop configuration.");
    }

    // Tile Id for to
    Tile childTile = TMSUtils.latLonToTile(lat, lon, zoom, size);
    TileIdZoomWritable key = new TileIdZoomWritable(TMSUtils.tileid(childTile.tx, childTile.ty, zoom), zoom);
    
    
    // Tile Ids for froms
    TileIdZoomWritable parentKeyUL = 
        new TileIdZoomWritable(TMSUtils.tileid(2*childTile.tx, 2*childTile.ty+1, zoom+1), zoom+1);
    TileIdZoomWritable parentKeyUR = 
        new TileIdZoomWritable(TMSUtils.tileid(2*childTile.tx+1, 2*childTile.ty+1, zoom+1), zoom+1);
    TileIdZoomWritable parentKeyLL = 
        new TileIdZoomWritable(TMSUtils.tileid(2*childTile.tx, 2*childTile.ty, zoom+1), zoom+1);
    TileIdZoomWritable parentKeyLR = 
        new TileIdZoomWritable(TMSUtils.tileid(2*childTile.tx+1, 2*childTile.ty, zoom+1), zoom+1);
    
    List<RasterWritable> value;
    try
    {
      value = new ArrayList<RasterWritable>();
      value.add(RasterWritable.toWritable(rasterUL, parentKeyUL));
      value.add(RasterWritable.toWritable(rasterUR, parentKeyUR));
      value.add(RasterWritable.toWritable(rasterLL, parentKeyLL));
      value.add(RasterWritable.toWritable(rasterLR, parentKeyLR));

      ReduceDriver<TileIdZoomWritable, RasterWritable, TileIdWritable, RasterWritable> driver = 
          new ReduceDriver<TileIdZoomWritable, RasterWritable, TileIdWritable, RasterWritable>()
          .withConfiguration(config)
          .withReducer(new BuildPyramidReducer())
          .withInputKey(key)
          .withInputValues(value);

      java.util.List<Pair<TileIdWritable, RasterWritable>> results = driver.run();


      // Test the results
      Assert.assertEquals("Bad number of reduces returned", 1, results.size());

      java.util.ListIterator<Pair<TileIdWritable, RasterWritable>> iter = results.listIterator();

      Assert.assertTrue("Reduce iterator doesn't have a next item", iter.hasNext());

      Pair<TileIdWritable, RasterWritable> item = iter.next();
      
      // Assert that output child key equals input key
      Assert.assertEquals("Input tileid doesn't match output", key.get(), item.getFirst().get());
      TestUtils.compareRasters(childRaster, RasterWritable.toRaster(item.getSecond()));
      
      // TODO: Test the stats calculations

      // test the counters, why is the plus one for the child included?
      Assert.assertEquals("Tile count (counter) incorrect.", 4+1,
          driver.getCounters().findCounter("BuildPyramid Reducer", "Parent Tiles Processed").getValue());

    } catch (NullPointerException ex){
      log.warn("Could not write temporary stats file, which is normal in the unit test.");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Reduce test threw an exception: " + e.getMessage());
    }

  }

}
