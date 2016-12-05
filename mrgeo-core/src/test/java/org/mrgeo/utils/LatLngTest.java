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

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

@SuppressWarnings("all") // test code, not included in production
public class LatLngTest
{
private static final double EPSILON = 1e-5;

@Test
@Category(UnitTest.class)
public void testBasics() throws Exception
{
  LatLng p1, p2;

  p1 = new LatLng(0, 0);
  p2 = new LatLng(0, 1);
  Assert.assertEquals(111319.49079326246, LatLng.calculateGreatCircleDistance(p1, p2));

  p1 = new LatLng(0, 0);
  p2 = new LatLng(1, 0);
  Assert.assertEquals(111319.49079326246, LatLng.calculateGreatCircleDistance(p1, p2));
}

@Test
@Category(UnitTest.class)
public void testCalcDestEast() throws Exception
{
  LatLng p1 = new LatLng(0.0, 0.0);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 90.0);
  Assert.assertEquals(0.0, p2.getLat(), EPSILON);
  Assert.assertEquals(1.0, p2.getLng(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestWest() throws Exception
{
  LatLng p1 = new LatLng(0.0, 0.0);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 270.0);
  Assert.assertEquals(0.0, p2.getLat(), EPSILON);
  Assert.assertEquals(-1.0, p2.getLng(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestNorth() throws Exception
{
  LatLng p1 = new LatLng(0.0, 0.0);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 0.0);
  Assert.assertEquals(1.0, p2.getLat(), EPSILON);
  Assert.assertEquals(0.0, p2.getLng(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestSouth() throws Exception
{
  LatLng p1 = new LatLng(0.0, 0.0);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 180.0);
  Assert.assertEquals(-1.0, p2.getLat(), EPSILON);
  Assert.assertEquals(0.0, p2.getLng(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestSouthLimited() throws Exception
{
  LatLng p1 = new LatLng(-89.99, 0.0);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 180.0);
  Assert.assertEquals(0.0, p2.getLng(), EPSILON);
  Assert.assertEquals(-90.0, p2.getLat(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestSouthEastLimited() throws Exception
{
  LatLng p1 = new LatLng(-89.99, 179.99);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 135.0);
  Assert.assertEquals(180.0, p2.getLng(), EPSILON);
  Assert.assertEquals(-90.0, p2.getLat(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestSouthWestLimited() throws Exception
{
  LatLng p1 = new LatLng(-89.99, -179.99);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 225.0);
  Assert.assertEquals(-180.0, p2.getLng(), EPSILON);
  Assert.assertEquals(-90.0, p2.getLat(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestNorthWestLimited() throws Exception
{
  LatLng p1 = new LatLng(89.99, -179.99);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 315.0);
  Assert.assertEquals(-180.0, p2.getLng(), EPSILON);
  Assert.assertEquals(90.0, p2.getLat(), EPSILON);
}

@Test
@Category(UnitTest.class)
public void testCalcDestNorthEastLimited() throws Exception
{
  LatLng p1 = new LatLng(89.99, 179.99);
  LatLng p2 = LatLng.calculateCartesianDestinationPoint(p1, LatLng.METERS_PER_DEGREE, 45.0);
  Assert.assertEquals(180.0, p2.getLng(), EPSILON);
  Assert.assertEquals(90.0, p2.getLat(), EPSILON);
}
}
