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

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.GeometryUtils;
import org.mrgeo.utils.tms.Bounds;

@SuppressWarnings("all") // test code, not included in production
public class GeometryFactoryTest extends LocalRunnerTest
{
private static final double EPSILON = 1e-12;

@Test(expected = IllegalArgumentException.class)
@Category(UnitTest.class)
public void wierdMultiPolygon() throws ParseException
{
  String wkt = "MULTIPOLYGON (((-126.2499876 59.1526579,-126.25 59.1526234,-126.2499876 59.1526579)))";

  WKTReader wktReader = new WKTReader();

  com.vividsolutions.jts.geom.Geometry jtsGeom = wktReader.read(wkt);

  Geometry feature = GeometryFactory.fromJTS(jtsGeom, null);
}
}
