package org.mrgeo.mapreduce;

import com.vividsolutions.jts.io.WKTReader;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;

import java.util.List;

public class RandomizeVectorMapperTest extends LocalRunnerTest
{
  @Test
  @Category(UnitTest.class)
  public void testMapper() throws Exception
  {
    WKTReader reader = new WKTReader();
    String baseGeom = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
    Geometry geom = GeometryFactory.fromJTS(reader.read(baseGeom));

    MapDriver<LongWritable, Geometry, LongWritable, GeometryWritable> driver =
        new MapDriver<LongWritable, Geometry, LongWritable, GeometryWritable>()
        .withConfiguration(conf)
        .withMapper(new RandomizeVectorMapper())
        .withInputKey(new LongWritable())
        .withInputValue(geom);
    List<Pair<LongWritable, GeometryWritable>> l = driver.run();

    Assert.assertEquals(1, l.size());
    java.util.ListIterator<Pair<LongWritable, GeometryWritable>> iter = l.listIterator();
    Assert.assertTrue(iter.hasNext());
    Pair<LongWritable, GeometryWritable> item = iter.next();
    Geometry outputGeom = item.getSecond().getGeometry();
    Assert.assertEquals(geom.toString(), outputGeom.toString());
  }  
}
