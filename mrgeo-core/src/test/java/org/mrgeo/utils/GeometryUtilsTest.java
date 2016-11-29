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

package org.mrgeo.utils;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.Point;
import org.mrgeo.junit.UnitTest;

@SuppressWarnings("all") // test code, not included in production
public class GeometryUtilsTest
{

  @AfterClass
  public static void tearDownAfterClass() throws Exception
  {
  }

  @Before
  public void setUp() throws Exception
  {
  }

  @After
  public void tearDown() throws Exception
  {
  }





  // Testing note:
  // Imagine that the line formed between the first two points extends
  // infinitely. Imagine standing on that line facing in the direction
  // of travel from the first point to the second point. If the third
  // point is to your left, then the inside function returns true. If
  // the third point is either on the line or to the right, then it
  // returns false.
  @Test
  @Category(UnitTest.class)
  public void testInsideWithDownVertical()
  {
    Point v0 = GeometryFactory.createPoint(10.0, 10.0);
    Point v1 = GeometryFactory.createPoint(10.0, 20.0);
    Point p = GeometryFactory.createPoint(5.0, 15.0);
    Assert.assertTrue(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(15.0, 15.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(10.0, 15.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
  }

  @Test
  @Category(UnitTest.class)
  public void testInsideWithUpVertical()
  {
    Point v0 = GeometryFactory.createPoint(10.0, 20.0);
    Point v1 = GeometryFactory.createPoint(10.0, 10.0);
    Point p = GeometryFactory.createPoint(5.0, 15.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(15.0, 15.0);
    Assert.assertTrue(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(10.0, 15.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
  }

  @Test
  @Category(UnitTest.class)
  public void testInsideWithRightHorizontal()
  {
    Point v0 = GeometryFactory.createPoint(10.0, 10.0);
    Point v1 = GeometryFactory.createPoint(20.0, 10.0);
    Point p = GeometryFactory.createPoint(15.0, 5.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(15.0, 15.0);
    Assert.assertTrue(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(15.0, 10.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
  }

  @Test
  @Category(UnitTest.class)
  public void testInsideWithLeftHorizontal()
  {
    Point v0 = GeometryFactory.createPoint(20.0, 10.0);
    Point v1 = GeometryFactory.createPoint(10.0, 10.0);
    Point p = GeometryFactory.createPoint(15.0, 5.0);
    Assert.assertTrue(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(15.0, 15.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
    p = GeometryFactory.createPoint(15.0, 10.0);
    Assert.assertFalse(GeometryUtils.inside(v0, v1, p));
  }
}
