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

import com.vividsolutions.jts.io.WKTReader;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.featurefilter.BaseFeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.mrgeo.utils.Base64Utils;

import java.io.IOException;
import java.util.List;

class MapFeatureToTilesTestFilter extends BaseFeatureFilter
{
  private Geometry geom;

  @SuppressWarnings("unused")
  private MapFeatureToTilesTestFilter()
  {
  }

  public MapFeatureToTilesTestFilter(Geometry geom)
  {
    this.geom = geom;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Geometry filterInPlace(Geometry f)
  {
    return geom.createWritableClone();
  }
}


public class MapFeatureToTilesTest extends LocalRunnerTest
{
  private static MapOpTestVectorUtils testUtils;

  private static final String ALL_ONES = "all-ones";
  private static final String FILTER_GEOM = "POLYGON ((141.8 -17.5, 141.9 -17.5, 141.9 -17.4, 141.8 -17.4, 141.8 -17.5))";

  WKTReader reader;
  
  @Before
  public void setup()
  {
    reader = new WKTReader();
  }
  
  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new MapOpTestVectorUtils(MapFeatureToTilesTest.class);

    HadoopFileUtils.delete(testUtils.getInputHdfs());
    
    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), ALL_ONES);
  }
 
  @Test
  @Category(UnitTest.class)
  public void testMapper() throws Exception {

    String outputPath = new Path(testUtils.getOutputHdfs(), "testMapper").toString();
    MrsImagePyramid pyramid = MrsImagePyramid.open(new Path(testUtils.getInputHdfs(),
        ALL_ONES).toString(), getProviderProperties());

    MrsImagePyramidMetadata meta = pyramid.getMetadata();
    
    conf.set(RasterizeVectorDriver.OUTPUT_FILENAME, outputPath.toString());
    conf.set(MapFeatureToTiles.IMAGE_TILE_BOUNDS,
        Base64Utils.encodeObject(meta.getTileBounds(meta.getMaxZoomLevel())));
    conf.setInt(MapFeatureToTiles.ZOOM, pyramid.getMaximumLevel());
    conf.setInt(MapFeatureToTiles.TILE_SIZE, meta.getTilesize());
    

    String baseGeom = "POLYGON ((141.8 -17.5,142.3 -17.5,142.3 -18.0,141.8 -18,141.8 -17.5))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, Geometry> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, Geometry>()
        .withConfiguration(conf)
        .withMapper(new MapFeatureToTiles())
        .withInputKey(new LongWritable())
        .withInputValue(geom);
    List<Pair<TileIdWritable, Geometry>> l = driver.run();

    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(MapFeatureToTiles.VALIDTILES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(MapFeatureToTiles.TOTALTILES).getValue());
    Assert.assertEquals(6, l.size());
    java.util.ListIterator<Pair<TileIdWritable, Geometry>> iter = l.listIterator();
    Assert.assertTrue(iter.hasNext());
    while (iter.hasNext())
    {
      Pair<TileIdWritable, Geometry> item = iter.next();
      Geometry outputGeom = item.getSecond();
      Assert.assertEquals(geom.toString(), outputGeom.toString());
    }
  }  

  @Test
  @Category(UnitTest.class)
  public void testMapperNoIntersection() throws Exception {    
    String outputPath = new Path(testUtils.getOutputHdfs(), "testMapper2").toString();
    MrsImagePyramid pyramid = MrsImagePyramid.open(new Path(testUtils.getInputHdfs(),
        ALL_ONES).toString(), getProviderProperties());
    
    MrsImagePyramidMetadata meta = pyramid.getMetadata();

    conf.set(RasterizeVectorDriver.OUTPUT_FILENAME, outputPath.toString());
    conf.set(MapFeatureToTiles.IMAGE_TILE_BOUNDS,
        Base64Utils.encodeObject(meta.getTileBounds(meta.getMaxZoomLevel())));
    conf.setInt(MapFeatureToTiles.ZOOM, pyramid.getMaximumLevel());
    conf.setInt(MapFeatureToTiles.TILE_SIZE, meta.getTilesize());
    

    String baseGeom = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, Geometry> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, Geometry>()
            .withConfiguration(conf)
            .withMapper(new MapFeatureToTiles())
            .withInputKey(new LongWritable())
            .withInputValue(geom);
    List<Pair<TileIdWritable, Geometry>> l = driver.run();

    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(MapFeatureToTiles.VALIDTILES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(MapFeatureToTiles.TOTALTILES).getValue());
    Assert.assertEquals(0, l.size());
  }  

  @Test
  @Category(UnitTest.class)
  //mapper with filter
  public void testMapperWithFilter() throws Exception {    
    String outputPath = new Path(testUtils.getOutputHdfs(), "testMapperWithFilter").toString();
    MrsImagePyramid pyramid = MrsImagePyramid.open(new Path(testUtils.getInputHdfs(),
        ALL_ONES).toString(), getProviderProperties());

    MrsImagePyramidMetadata meta = pyramid.getMetadata();

    conf.set(RasterizeVectorDriver.OUTPUT_FILENAME, outputPath);
    conf.set(MapFeatureToTiles.IMAGE_TILE_BOUNDS,
        Base64Utils.encodeObject(meta.getTileBounds(meta.getMaxZoomLevel())));
    conf.setInt(MapFeatureToTiles.ZOOM, pyramid.getMaximumLevel());
    conf.setInt(MapFeatureToTiles.TILE_SIZE, meta.getTilesize());
    //MapFeatureToTilesTest test = new MapFeatureToTilesTest();
    Geometry filterOutGeom = GeometryFactory.fromJTS(reader.read(FILTER_GEOM));
    conf.set(MapFeatureToTiles.FEATURE_FILTER, Base64Utils.encodeObject(
        new MapFeatureToTilesTestFilter(filterOutGeom)));


    String baseGeom = "POLYGON ((141.8 -17.5,142.3 -17.5,142.3 -18.0,141.8 -18,141.8 -17.5))";

    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, Geometry> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, Geometry>()
            .withConfiguration(conf)
            .withMapper(new MapFeatureToTiles())
            .withInputKey(new LongWritable())
            .withInputValue(geom);
    List<Pair<TileIdWritable, Geometry>> l = driver.run();

    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(MapFeatureToTiles.VALIDTILES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(MapFeatureToTiles.TOTALTILES).getValue());
    Assert.assertEquals(1, l.size());
    java.util.ListIterator<Pair<TileIdWritable, Geometry>> iter = l.listIterator();
    Assert.assertTrue(iter.hasNext());
    Pair<TileIdWritable, Geometry> item = iter.next();
    Geometry outputGeom = item.getSecond();
    Assert.assertEquals("Expected: " + FILTER_GEOM + " but got: " + outputGeom.toString(), filterOutGeom.toString(), outputGeom.toString());
  }
  
}

