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

package org.mrgeo.geometryfilter;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.GeometryInputStream;
import org.mrgeo.data.shp.ShapefileReader;
import org.mrgeo.geometry.WellKnownProjections;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.geometry.WritablePoint;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import java.io.File;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class ReprojectorTest 
{
  public static void validatePoints(GeometryInputStream uut)
  {
    int count = 0;

    // these values were created by looking at the KML on the DC GIS website
    double[][] expected = { { -77.045802, 38.902854 }, { -76.958985, 38.868362 },
        { -77.045802, 38.902854 }, { -77.042906, 38.905015 }, { -77.048982, 38.903304 },
        { -77.026222, 38.981916 }, { -77.027107, 38.966919 }, { -77.036981, 38.904024 } };

    while (uut.hasNext())
    {
      WritableGeometry g = uut.next();
      Assert.assertTrue(g instanceof WritablePoint);
      WritablePoint p = (WritablePoint) g;
      Assert.assertEquals(expected[count][0], p.getX(), 1e-3);
      Assert.assertEquals(expected[count][1], p.getY(), 1e-3);
      count++;
    }

    Assert.assertEquals(8, count);
  }

  @Test 
  @Category(UnitTest.class)
  public void testPointRead() throws Exception
  {

    ShapefileReader sis =
        new ShapefileReader(new File(TestUtils.composeInputDir(ReprojectorTest.class), "AmbulatoryPt.shp").getCanonicalPath());
    ReprojectedGeometryInputStream uut = new ReprojectedGeometryInputStream(sis,
        WellKnownProjections.WGS84);

    validatePoints(uut);
    
    sis.close();
  }
}
