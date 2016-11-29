/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.geometry;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.tms.Bounds;

@SuppressWarnings("all") // test code, not included in production
public class PointImplTest
{
  private static final double EPSILON = 1e-12;

  @Test
  @Category(UnitTest.class)
  public void clipMinLatMinLon()
  {
    Point p = GeometryFactory.createPoint(-180.0, -90.0);

    Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
    Geometry g = p.clip(bbox);
    Assert.assertNotNull(g);
    Assert.assertTrue(g instanceof Point);
    Point result = (Point)g;
    Assert.assertEquals(-180.0, result.getX(), EPSILON);
    Assert.assertEquals(-90.0, result.getY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void clipMinLatMaxLon()
  {
    Point p = GeometryFactory.createPoint(-180.0, 90.0);

    Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
    Geometry g = p.clip(bbox);
    Assert.assertNotNull(g);
    Assert.assertTrue(g instanceof Point);
    Point result = (Point)g;
    Assert.assertEquals(-180.0, result.getX(), EPSILON);
    Assert.assertEquals(90.0, result.getY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void clipMaxLatMinLon()
  {
    Point p = GeometryFactory.createPoint(180.0, -90.0);

    Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
    Geometry g = p.clip(bbox);
    Assert.assertNotNull(g);
    Assert.assertTrue(g instanceof Point);
    Point result = (Point)g;
    Assert.assertEquals(180.0, result.getX(), EPSILON);
    Assert.assertEquals(-90.0, result.getY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void clipMaxLatMaxLon()
  {
    Point p = GeometryFactory.createPoint(180.0, 90.0);

    Bounds bbox = new Bounds(0.0, 0.0, 180.0, 90.0);
    Geometry g = p.clip(bbox);
    Assert.assertNotNull(g);
    Assert.assertTrue(g instanceof Point);
    Point result = (Point)g;
    Assert.assertEquals(180.0, result.getX(), EPSILON);
    Assert.assertEquals(90.0, result.getY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void clipZeroLatZeroLon()
  {
    Point p = GeometryFactory.createPoint(0.0, 0.0);

    Bounds[] clipRegions = {
        new Bounds(-180.0, -90.0, 0.0, 0.0),
        new Bounds(-180.0, 0.0, 0.0, 90.0),
        new Bounds(0.0, -90.0, 180.0, 0.0),
        new Bounds(0.0, 0.0, 180.0, 90.0),
    };
    int i = 0;
    for (Bounds b : clipRegions)
    {
      Geometry g = p.clip(b);
      Assert.assertNotNull("expected non-null result from clip at bounds index " + i, g);
      Assert.assertTrue("expected point from clip at bounds index " + i, g instanceof Point);
      Point result = (Point)g;
      Assert.assertEquals("incorrect X coordinate at bounds index " + i, 0.0, result.getX(), EPSILON);
      Assert.assertEquals("incorrect X coordinate at bounds index " + i, 0.0, result.getY(), EPSILON);
      i++;
    }
  }
}
