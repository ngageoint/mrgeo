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
