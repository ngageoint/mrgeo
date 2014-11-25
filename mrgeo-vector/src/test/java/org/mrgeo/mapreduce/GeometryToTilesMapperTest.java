package org.mrgeo.mapreduce;

import com.vividsolutions.jts.io.WKTReader;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.featurefilter.BaseFeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.Base64Utils;

import java.util.List;

class FeatureToTilesMapperTest extends BaseFeatureFilter {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public Geometry filterInPlace(Geometry f)
  {
    try {
      WKTReader reader = new WKTReader();
      
      return GeometryFactory.fromJTS(reader.read("POLYGON ((0 0,0 2,2 2,2 0,0 0))"));
             
    }
    catch (Exception e) {
      
    }
    return f; 
  }
  
}


public class GeometryToTilesMapperTest extends LocalRunnerTest
{
  WKTReader reader;
  Geometry filterOutGeom = null;
  
  
  @Before
  public void setUp() throws Exception {
    try {
      reader = new WKTReader();
      filterOutGeom = GeometryFactory.fromJTS(reader.read("POLYGON ((0 0,0 2,2 2,2 0,0 0))"));
    }
    catch (Exception e) {
      
    }
  }
  
  @Test
  @Category(UnitTest.class)
  public void testMapper() throws Exception {    
    conf.set(FeatureToTilesMapper.ZOOM, "1");    
    conf.set(FeatureToTilesMapper.TILE_SIZE, "512");    
    

    String baseGeom = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable>()
        .withConfiguration(conf)
        .withMapper(new FeatureToTilesMapper())
        .withInputKey(new LongWritable())
        .withInputValue(geom);
    List<Pair<TileIdWritable, GeometryWritable>> l = driver.run();

    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.VALIDFEATURES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.TOTALFEATURES).getValue());
    Assert.assertEquals(1, l.size());
    java.util.ListIterator<Pair<TileIdWritable, GeometryWritable>> iter = l.listIterator();
    Assert.assertTrue(iter.hasNext());
    Pair<TileIdWritable, GeometryWritable> item = iter.next();
    Geometry outputGeom = item.getSecond().getGeometry();
    Assert.assertEquals(geom.toString(), outputGeom.toString());
  }
  
  @Test
  @Category(UnitTest.class)
  public void testMapperWithBounds() throws Exception {    
    conf.set(FeatureToTilesMapper.ZOOM, "1");    
    conf.set(FeatureToTilesMapper.TILE_SIZE, "512");
    conf.set(FeatureToTilesMapper.BOUNDS, "-0.5, -0.5, 0.5, 0.5");

    String baseGeom = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable>()
        .withConfiguration(conf)
        .withMapper(new FeatureToTilesMapper())
        .withInputKey(new LongWritable())
        .withInputValue(geom);
    List<Pair<TileIdWritable, GeometryWritable>> l = driver.run();

    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.VALIDFEATURES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.TOTALFEATURES).getValue());
    Assert.assertEquals(1, l.size());
    java.util.ListIterator<Pair<TileIdWritable, GeometryWritable>> iter = l.listIterator();
    Assert.assertTrue(iter.hasNext());
    Pair<TileIdWritable, GeometryWritable> item = iter.next();
    Geometry outputGeom = item.getSecond().getGeometry();
    Assert.assertEquals(geom.toString(), outputGeom.toString());
  } 
  
  @Test
  @Category(UnitTest.class)
  public void testMapperWithOutOfBounds() throws Exception {    
    conf.set(FeatureToTilesMapper.ZOOM, "1");    
    conf.set(FeatureToTilesMapper.TILE_SIZE, "512");
    conf.set(FeatureToTilesMapper.BOUNDS, "9.5, 9.5, 10.5, 10.5");

    String baseGeom = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable>()
            .withConfiguration(conf)
            .withMapper(new FeatureToTilesMapper())
            .withInputKey(new LongWritable())
            .withInputValue(geom);
    List<Pair<TileIdWritable, GeometryWritable>> l = driver.run();

    Assert.assertEquals(0, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.VALIDFEATURES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.TOTALFEATURES).getValue());
    Assert.assertEquals(0, l.size());
    java.util.ListIterator<Pair<TileIdWritable, GeometryWritable>> iter = l.listIterator();
    Assert.assertFalse(iter.hasNext());
  }  

  @Test
  @Category(UnitTest.class)
  //mapper with filter
  public void testMapperWithFilter() throws Exception {    

    conf.set(FeatureToTilesMapper.ZOOM, "1");    
    conf.set(FeatureToTilesMapper.TILE_SIZE, "512");    
    conf.set(FeatureToTilesMapper.FEATURE_FILTER, Base64Utils.encodeObject(new FeatureToTilesMapperTest()));

    String baseGeom = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable>()
            .withConfiguration(conf)
            .withMapper(new FeatureToTilesMapper())
            .withInputKey(new LongWritable())
            .withInputValue(geom);
    List<Pair<TileIdWritable, GeometryWritable>> l = driver.run();

    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.VALIDFEATURES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.TOTALFEATURES).getValue());
    Assert.assertEquals(1, l.size());

    java.util.ListIterator<Pair<TileIdWritable, GeometryWritable>> iter = l.listIterator();
    Assert.assertTrue(iter.hasNext());
    Pair<TileIdWritable, GeometryWritable> item = iter.next();
    Geometry outputGeom = item.getSecond().getGeometry();
    Assert.assertEquals(filterOutGeom.toString(), outputGeom.toString());
  }
  

  @Test
  @Category(UnitTest.class)
  //mapper with no geometry 
  public void testMapperNoGeometry() throws Exception {    
    conf.set(FeatureToTilesMapper.ZOOM, "1");    
    conf.set(FeatureToTilesMapper.TILE_SIZE, "512");    

    //do not set geometry
    Geometry geom = GeometryFactory.createEmptyGeometry();

    MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable> driver =
        new MapDriver<LongWritable, Geometry, TileIdWritable, GeometryWritable>()
            .withConfiguration(conf)
            .withMapper(new FeatureToTilesMapper())
            .withInputKey(new LongWritable())
            .withInputValue(geom);
    List<Pair<TileIdWritable, GeometryWritable>> l = driver.run();
    
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.TOTALFEATURES).getValue());
    Assert.assertEquals(0, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.VALIDFEATURES).getValue());
    Assert.assertEquals(1, driver.getCounters().getGroup("Tiles").findCounter(FeatureToTilesMapper.NOGEOMETRY).getValue());
    Assert.assertEquals(0, l.size());
  }  
}

