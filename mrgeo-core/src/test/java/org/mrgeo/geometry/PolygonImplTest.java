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

package org.mrgeo.geometry;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;

import java.util.Collection;

@SuppressWarnings("static-method")
public class PolygonImplTest extends LocalRunnerTest
{
  private static final double COORDINATE_EPSILON = 0.00001;

  @Test
  @Category(UnitTest.class)
  public void testpolyclip()
  {
    // polygon and answer taken from http://rosettacode.org/wiki/Sutherland-Hodgman_polygon_clipping#Python

    // input
    //    {50, 150}, 
    //    {200, 50}, 
    //    {350, 150}, 
    //    {350, 300},
    //    {250, 300}, 
    //    {200, 250}, 
    //    {150, 350}, 
    //    {100, 250}, 
    //    {100, 200}  
    Point[] inputs = {
      GeometryFactory.createPoint(5.0, 15.0),
      GeometryFactory.createPoint(20.0, 5.0),
      GeometryFactory.createPoint(35.0, 15.0),
      GeometryFactory.createPoint(35.0, 30.0),
      GeometryFactory.createPoint(25.0, 30.0),
      GeometryFactory.createPoint(20.0, 25.0),
      GeometryFactory.createPoint(15.0, 35.0),
      GeometryFactory.createPoint(10.0, 25.0),
      GeometryFactory.createPoint(10.0, 20.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    // clip
    //    {100, 100}, 
    //    {300, 100},
    //    {300, 300}, 
    //    {100, 300}
    Point[] clips = {
      GeometryFactory.createPoint(10.0, 10.0),
      GeometryFactory.createPoint(30.0, 10.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(10.0, 30.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    // output (winding is different, but the points are in the same order) 
    //  {100.000000, 116.666667},
    //  {125.000000, 100.000000},
    //  {275.000000, 100.000000},
    //  {300.000000, 116.666667},
    //  {300.000000, 300.000000},
    //  {250.000000, 300.000000},
    //  {200.000000, 250.000000},
    //  {175.000000, 300.000000},
    //  {125.000000, 300.000000},
    //  {100.000000, 250.000000}

    Point[] outputs = {
      GeometryFactory.createPoint(12.500000000,10.000000000),
      GeometryFactory.createPoint(10.000000000,11.666666667),
      GeometryFactory.createPoint(10.000000000,20.000000000),
      GeometryFactory.createPoint(10.000000000,25.000000000),
      GeometryFactory.createPoint(12.500000000,30.000000000),
      GeometryFactory.createPoint(17.500000000,30.000000000),
      GeometryFactory.createPoint(20.000000000,25.000000000),
      GeometryFactory.createPoint(25.000000000,30.000000000),
      GeometryFactory.createPoint(30.000000000,30.000000000),
      GeometryFactory.createPoint(30.000000000,11.666666667),
      GeometryFactory.createPoint(27.500000000,10.000000000),
      GeometryFactory.createPoint(12.500000000,10.000000000),

    };

    Geometry output = input.clip(clip);

    Assert.assertTrue("Wrong geometry type, should be Polygon " + output.getClass().getName(), output instanceof Polygon);
    LinearRing exterior = ((Polygon) output).getExteriorRing();

    for (int i = 0; i < exterior.getNumPoints(); i++)
    {
      Point pt = exterior.getPoint(i);

      Assert.assertEquals("Points x don't match (" + i + ")", outputs[i].getX(), pt.getX(), COORDINATE_EPSILON);
      Assert.assertEquals("Points y don't match (" + i + ")", outputs[i].getY(), pt.getY(), COORDINATE_EPSILON);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testpolyclipAllInside()
  {

    Point[] inputs = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(20.0, 30.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(30.0, 20.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(10.0, 10.0),
      GeometryFactory.createPoint(40.0, 10.0),
      GeometryFactory.createPoint(40.0, 40.0),
      GeometryFactory.createPoint(10.0, 40.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertTrue("Wrong geometry type, should be Polygon", output instanceof Polygon);
    LinearRing exterior = ((Polygon) output).getExteriorRing();

    for (int i = 0; i < exterior.getNumPoints(); i++)
    {
      Point pt = exterior.getPoint(i);
      Point testPt = i == exterior.getNumPoints() - 1 ? inputs[0] : inputs[i];

      Assert.assertEquals("Points x don't match (" + i + ")", testPt.getX(), pt.getX(), COORDINATE_EPSILON);
      Assert.assertEquals("Points y don't match (" + i + ")", testPt.getY(), pt.getY(), COORDINATE_EPSILON);
    }
  } 

  @Test
  @Category(UnitTest.class)
  public void testOutsideTouchingBottom()
  {
    // Input triangle sits below the clipping region with
    // a single point touching the bottom line of the clipping
    // region.
    Point[] inputs = {
      GeometryFactory.createPoint(25.0, 20.0),
      GeometryFactory.createPoint(20.0, 10.0),
      GeometryFactory.createPoint(30.0, 10.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(30.0, 20.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(20.0, 30.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull(output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testOutsideTouchingTop()
  {
    // Input triangle sits above the clipping region with
    // a single point touching the top line of the clipping
    // region.
    Point[] inputs = {
      GeometryFactory.createPoint(25.0, 30.0),
      GeometryFactory.createPoint(20.0, 40.0),
      GeometryFactory.createPoint(30.0, 40.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(30.0, 20.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(20.0, 30.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull(output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testOutsideTouchingLeft()
  {
    // Input triangle sits left of the clipping region with
    // a single point touching the left line of the clipping
    // region.
    Point[] inputs = {
      GeometryFactory.createPoint(20.0, 25.0),
      GeometryFactory.createPoint(10.0, 20.0),
      GeometryFactory.createPoint(10.0, 30.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(30.0, 20.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(20.0, 30.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull(output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testOutsideTouchingRight()
  {
    // Input triangle sits right of the clipping region with
    // a single point touching the right line of the clipping
    // region.
    Point[] inputs = {
      GeometryFactory.createPoint(30.0, 25.0),
      GeometryFactory.createPoint(40.0, 20.0),
      GeometryFactory.createPoint(40.0, 30.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(30.0, 20.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(20.0, 30.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull(output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testOutsideTouchingSpecial()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(-50.437141418, -0.000000000),
      GeometryFactory.createPoint(-50.657783508, 0.152361095),
      GeometryFactory.createPoint(-50.472778320, 0.154166639)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(-67.500000000, -22.500000000),
      GeometryFactory.createPoint(-45.000000000, -22.500000000),
      GeometryFactory.createPoint(-45.000000000, 0.000000000),
      GeometryFactory.createPoint(-67.500000000, 0.000000000)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull(output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testOutsideRectTouchingLeft()
  {
    // Input rectangle right edge is colinear with the left edge of
    // the clipping region. Make sure that no geometry ends up in
    // the clipped output.
    Point[] inputs = {
      GeometryFactory.createPoint(10.0, 25.0),
      GeometryFactory.createPoint(20.0, 25.0),
      GeometryFactory.createPoint(20.0, 30.0),
      GeometryFactory.createPoint(10.0, 30.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(30.0, 20.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(20.0, 30.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull(output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testpolyclipAllOutside()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(10.0, 10.0),
      GeometryFactory.createPoint(10.0, 20.0),
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(20.0, 10.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(40.0, 30.0),
      GeometryFactory.createPoint(40.0, 40.0),
      GeometryFactory.createPoint(30.0, 40.0)
    };
    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertNull("Output should be null", output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testpolyclipWithin()
  {
    // clip is totally within the polygon

    Point[] inputs = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(20.0, 40.0),
      GeometryFactory.createPoint(40.0, 40.0),
      GeometryFactory.createPoint(40.0, 20.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(25.0, 25.0),
      GeometryFactory.createPoint(25.0, 35.0),
      GeometryFactory.createPoint(35.0, 35.0),
      GeometryFactory.createPoint(35.0, 25.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertTrue("Wrong geometry type, should be Polygon", output instanceof Polygon);
    LinearRing exterior = ((Polygon) output).getExteriorRing();

    for (int i = 0; i < exterior.getNumPoints(); i++)
    {
      Point pt = exterior.getPoint(i);
      Point testPt = i == exterior.getNumPoints() - 1 ? clips[0] : clips[i];

      Assert.assertEquals("Points x don't match (" + i + ")", testPt.getX(), pt.getX(), COORDINATE_EPSILON);
      Assert.assertEquals("Points y don't match (" + i + ")", testPt.getY(), pt.getY(), COORDINATE_EPSILON);
    }
  } 

  @Test
  @Category(UnitTest.class)
  public void testpolyclipSawtooth()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(5.0, 25.0),
      GeometryFactory.createPoint(5.0, 18.0),
      GeometryFactory.createPoint(15.0, 18.0),
      GeometryFactory.createPoint(15.0, 16.0),
      GeometryFactory.createPoint(5.0, 16.0),
      GeometryFactory.createPoint(5.0, 14.0),
      GeometryFactory.createPoint(15.0, 14.0),
      GeometryFactory.createPoint(15.0, 12.0),
      GeometryFactory.createPoint(5.0, 12.0),
      GeometryFactory.createPoint(5.0, 5.0),
      GeometryFactory.createPoint(25.0, 5.0),
      GeometryFactory.createPoint(25.0, 25.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(10.0, 10.0),
      GeometryFactory.createPoint(20.0, 10.0),
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(10.0, 20.0),
    };

    Point[] outputs = {
      GeometryFactory.createPoint(10.000000000,16.000000000),
      GeometryFactory.createPoint(15.000000000,16.000000000),
      GeometryFactory.createPoint(15.000000000,18.000000000),
      GeometryFactory.createPoint(10.000000000,18.000000000),
      GeometryFactory.createPoint(10.000000000,20.000000000),
      GeometryFactory.createPoint(20.000000000,20.000000000),
      GeometryFactory.createPoint(20.000000000,10.000000000),
      GeometryFactory.createPoint(10.000000000,10.000000000),
      GeometryFactory.createPoint(10.000000000,12.000000000),
      GeometryFactory.createPoint(15.000000000,12.000000000),
      GeometryFactory.createPoint(15.000000000,14.000000000),
      GeometryFactory.createPoint(10.000000000,14.000000000),
      GeometryFactory.createPoint(10.000000000,16.000000000),
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertTrue("Wrong geometry type, should be Polygon", output instanceof Polygon);
    LinearRing exterior = ((Polygon) output).getExteriorRing();

    for (int i = 0; i < exterior.getNumPoints(); i++)
    {
      Point pt = exterior.getPoint(i);

      Assert.assertEquals("Points x don't match (" + i + ")", outputs[i].getX(), pt.getX(), COORDINATE_EPSILON);
      Assert.assertEquals("Points y don't match (" + i + ")", outputs[i].getY(), pt.getY(), COORDINATE_EPSILON);
    }
  } 

  @Test
  @Category(UnitTest.class)
  public void testpolyclipSawtooth2()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(5.0, 25.0),
      GeometryFactory.createPoint(5.0, 18.0),
      GeometryFactory.createPoint(15.0, 18.0),
      GeometryFactory.createPoint(15.0, 16.0),
      GeometryFactory.createPoint(5.0, 16.0),
      GeometryFactory.createPoint(5.0, 14.0),
      GeometryFactory.createPoint(15.0, 14.0),
      GeometryFactory.createPoint(15.0, 12.0),
      GeometryFactory.createPoint(5.0, 12.0),
      GeometryFactory.createPoint(5.0, 5.0),
      GeometryFactory.createPoint(25.0, 5.0),
      GeometryFactory.createPoint(25.0, 25.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(0.0, 10.0),
      GeometryFactory.createPoint(10.0, 10.0),
      GeometryFactory.createPoint(10.0, 20.0),
      GeometryFactory.createPoint(0.0, 20.0),
    };

    Point[][] outputs = {
      {
        GeometryFactory.createPoint(10.000000000,18.000000000),
        GeometryFactory.createPoint(5.000000000,18.000000000),
        GeometryFactory.createPoint(5.000000000,20.000000000),
        GeometryFactory.createPoint(10.000000000,20.000000000),
        GeometryFactory.createPoint(10.000000000,18.000000000),
      },
      {
        GeometryFactory.createPoint(10.000000000,14.000000000),
        GeometryFactory.createPoint(5.000000000,14.000000000),
        GeometryFactory.createPoint(5.000000000,16.000000000),
        GeometryFactory.createPoint(10.000000000,16.000000000),
        GeometryFactory.createPoint(10.000000000,14.000000000),
      },
      {
        GeometryFactory.createPoint(5.000000000,10.000000000),
        GeometryFactory.createPoint(5.000000000,12.000000000),
        GeometryFactory.createPoint(10.000000000,12.000000000),
        GeometryFactory.createPoint(10.000000000,10.000000000),
        GeometryFactory.createPoint(5.000000000,10.000000000),
      }
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);

    Assert.assertTrue("Wrong geometry type, should be GeometryCollection", output instanceof GeometryCollection);

    GeometryCollection collection = (GeometryCollection) output;
    Assert.assertEquals("Wrong number of geometries", 3, collection.getNumGeometries());

    for (int c = 0; c < collection.getNumGeometries(); c++)
    {
      LinearRing exterior = ((Polygon) collection.getGeometry(c)).getExteriorRing();

      for (int i = 0; i < exterior.getNumPoints(); i++)
      {
        Point pt = exterior.getPoint(i);
        Assert.assertEquals("Points x don't match (" + i + ")", outputs[c][i].getX(), pt.getX(), COORDINATE_EPSILON);
        Assert.assertEquals("Points y don't match (" + i + ")", outputs[c][i].getY(), pt.getY(), COORDINATE_EPSILON);
      }
    }
  } 

  @Test
  @Category(UnitTest.class)
  public void testpolyclipMultiPoints()
  {
    // input
    //    {50, 150}, 
    //    {200, 50}, 
    //    {350, 150}, 
    //    {350, 350},
    //    {250, 300}, 
    //    {200, 250}, 
    //    {150, 350}, 
    //    {100, 250}, 
    //    {100, 200}  
    Point[] inputs = {
      GeometryFactory.createPoint(5.0, 15.0),
      GeometryFactory.createPoint(20.0, 5.0),
      GeometryFactory.createPoint(35.0, 15.0),
      GeometryFactory.createPoint(35.0, 35.0), // moved this point to make it "hang down"
      GeometryFactory.createPoint(25.0, 30.0),
      GeometryFactory.createPoint(20.0, 25.0),
      GeometryFactory.createPoint(15.0, 35.0),
      GeometryFactory.createPoint(10.0, 25.0),
      GeometryFactory.createPoint(10.0, 20.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    // clip (moved the clip 200 in y)
    //    {100, 300}, 
    //    {300, 300},
    //    {300, 500}, 
    //    {100, 500}
    Point[] clips = {
      GeometryFactory.createPoint(10.0, 30.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(30.0, 50.0),
      GeometryFactory.createPoint(10.0, 50.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Point[][] outputs = {
      {
        GeometryFactory.createPoint(25.000000000,30.000000000),
        GeometryFactory.createPoint(30.000000000,32.500000000),
        GeometryFactory.createPoint(30.000000000,30.000000000),
        GeometryFactory.createPoint(25.000000000,30.000000000),
      },
      {
        GeometryFactory.createPoint(12.500000000,30.000000000),
        GeometryFactory.createPoint(15.000000000,35.000000000),
        GeometryFactory.createPoint(17.500000000,30.000000000),
        GeometryFactory.createPoint(12.500000000,30.000000000),
      },
    };

    Geometry output = input.clip(clip);

    Assert.assertTrue("Wrong geometry type, should be GeometryCollection " + output.getClass().getName(), output instanceof GeometryCollection);

    GeometryCollection gc = (GeometryCollection) output;
    for (int g = 0; g < gc.getNumGeometries(); g++)
    {
      Polygon p = (Polygon) gc.getGeometry(g);

      LinearRing exterior = p.getExteriorRing();

      for (int i = 0; i < exterior.getNumPoints(); i++)
      {

        Point pt = exterior.getPoint(i);

        Assert.assertEquals("Points x don't match (" + i + ")", outputs[g][i].getX(), pt.getX(), COORDINATE_EPSILON);
        Assert.assertEquals("Points y don't match (" + i + ")", outputs[g][i].getY(), pt.getY(), COORDINATE_EPSILON);
      }
    }
  }

  private void verifyPointListsEqual(Point[] expected, Collection<Point> actual)
  {
    Assert.assertEquals(expected.length, actual.size());
    int index = 0;
    for (Point p : actual)
    {
      Assert.assertEquals("Points x don't match (" + index + ")", expected[index].getX(), p.getX(), COORDINATE_EPSILON);
      Assert.assertEquals("Points y don't match (" + index + ")", expected[index].getY(), p.getY(), COORDINATE_EPSILON);
      index++;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testClipSidewaysCrownWithoutTriangles()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 0.0),
      GeometryFactory.createPoint(50.0, 0.0),
      GeometryFactory.createPoint(50.0, 10.0),
      GeometryFactory.createPoint(40.0, 10.0),
      GeometryFactory.createPoint(47.0, 5.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(45.0, -5.0),
      GeometryFactory.createPoint(55.0, -5.0),
      GeometryFactory.createPoint(55.0, 15),
      GeometryFactory.createPoint(45.0, 15)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof Polygon);
    
    // Check expected points in output
    Polygon p = (Polygon)output;
    LinearRing ring = p.getExteriorRing();
    Point[] expected = {
        GeometryFactory.createPoint(45.0, 10.0),
        GeometryFactory.createPoint(50.0, 10.0),
        GeometryFactory.createPoint(50.0, 0.0),
        GeometryFactory.createPoint(45.0, 0.0),
        GeometryFactory.createPoint(45.0, 3.571428571),
        GeometryFactory.createPoint(47.0, 5.0),
        GeometryFactory.createPoint(45.0, 6.428571429),
        GeometryFactory.createPoint(45.0, 10.0)
    };
    verifyPointListsEqual(expected, ring.getPoints());
  } 

  // This uses the same shapes as testCLipMultiPolygons1, except the
  // clipping region is mirrored over the vertical axis.
  @Test
  @Category(UnitTest.class)
  public void testClipSidewaysCrownWithTriangles()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 0.0),
      GeometryFactory.createPoint(50.0, 0.0),
      GeometryFactory.createPoint(50.0, 10.0),
      GeometryFactory.createPoint(40.0, 10.0),
      GeometryFactory.createPoint(47.0, 5.0)
    };

    Polygon input = GeometryFactory.createPolygon(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(35.0, -5.0),
      GeometryFactory.createPoint(45.0, -5.0),
      GeometryFactory.createPoint(45.0, 15),
      GeometryFactory.createPoint(35.0, 15)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof GeometryCollection);
    
    // Check expected points in output
    GeometryCollection gc = (GeometryCollection)output;
    // Should be two triangles in the output
    Assert.assertEquals(2, gc.getNumGeometries());
    // Verify the first triangle
    {
      Geometry g = gc.getGeometry(0);
      Assert.assertTrue(g instanceof Polygon);
      Polygon p = (Polygon)g;
      LinearRing ring = p.getExteriorRing();
      Point[] expected = {
          GeometryFactory.createPoint(45.0, 0),
          GeometryFactory.createPoint(40.0, 0.0),
          GeometryFactory.createPoint(45.0, 3.571428571),
          GeometryFactory.createPoint(45.0, 0)
      };
      verifyPointListsEqual(expected, ring.getPoints());
    }
    // Verify the second triangle
    {
      Geometry g = gc.getGeometry(1);
      Assert.assertTrue(g instanceof Polygon);
      Polygon p = (Polygon)g;
      LinearRing ring = p.getExteriorRing();
      Point[] expected = {
          GeometryFactory.createPoint(45.0, 6.428571429),
          GeometryFactory.createPoint(40.0, 10.0),
          GeometryFactory.createPoint(45.0, 10.0),
          GeometryFactory.createPoint(45.0, 6.428571429)
      };
      verifyPointListsEqual(expected, ring.getPoints());
    }
  } 

  // Create a doughnut geometry and clip it to region that bisects it in
  // the middle and extends beyond the right side.
  @Test
  @Category(UnitTest.class)
  public void testClipDoughnutBeyondOutsideEdge()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 50.0),
      GeometryFactory.createPoint(50.0, 50.0),
      GeometryFactory.createPoint(50.0, 60.0),
      GeometryFactory.createPoint(40.0, 60.0)
    };

    WritablePolygon input = GeometryFactory.createPolygon(inputs);

    Point[] interior = {
      GeometryFactory.createPoint(42.0, 52.0),
      GeometryFactory.createPoint(48.0, 52.0),
      GeometryFactory.createPoint(48.0, 58.0),
      GeometryFactory.createPoint(42.0, 58.0),
    };
    LinearRing interiorRing = GeometryFactory.createLinearRing(interior);
    input.addInteriorRing(interiorRing);

    Point[] clips = {
        GeometryFactory.createPoint(45.0, 45.0),
        GeometryFactory.createPoint(45.0, 65.0),
        GeometryFactory.createPoint(52.0, 65.0),
        GeometryFactory.createPoint(52.0, 45.0),
    };

    WritablePolygon clip = GeometryFactory.createPolygon(clips);
    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof Polygon);
    Point[] expected = {
        GeometryFactory.createPoint(45.0, 60.0),
        GeometryFactory.createPoint(50.0, 60.0),
        GeometryFactory.createPoint(50.0, 50.0),
        GeometryFactory.createPoint(45.0, 50.0),
        GeometryFactory.createPoint(45.0, 52.0),
        GeometryFactory.createPoint(48.0, 52.0),
        GeometryFactory.createPoint(48.0, 58.0),
        GeometryFactory.createPoint(45.0, 58.0),
        GeometryFactory.createPoint(45.0, 60.0)
    };
    verifyPointListsEqual(expected, ((Polygon)output).getExteriorRing().getPoints());
  } 

  // Create a doughnut geometry and clip it to region that bisects it in
  // the middle and extends to the right side of the outer ring.
  @Test
  @Category(UnitTest.class)
  public void testClipDoughnutAtOutsideEdge()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 50.0),
      GeometryFactory.createPoint(50.0, 50.0),
      GeometryFactory.createPoint(50.0, 60.0),
      GeometryFactory.createPoint(40.0, 60.0)
    };

    WritablePolygon input = GeometryFactory.createPolygon(inputs);

    Point[] interior = {
      GeometryFactory.createPoint(42.0, 52.0),
      GeometryFactory.createPoint(48.0, 52.0),
      GeometryFactory.createPoint(48.0, 58.0),
      GeometryFactory.createPoint(42.0, 58.0),
    };
    LinearRing interiorRing = GeometryFactory.createLinearRing(interior);
    input.addInteriorRing(interiorRing);

    Point[] clips = {
        GeometryFactory.createPoint(45.0, 45.0),
        GeometryFactory.createPoint(45.0, 65.0),
        GeometryFactory.createPoint(50.0, 65.0),
        GeometryFactory.createPoint(50.0, 45.0),
    };

    WritablePolygon clip = GeometryFactory.createPolygon(clips);
    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof Polygon);
    Point[] expected = {
        GeometryFactory.createPoint(50.0, 50.0),
        GeometryFactory.createPoint(45.0, 50.0),
        GeometryFactory.createPoint(45.0, 52.0),
        GeometryFactory.createPoint(48.0, 52.0),
        GeometryFactory.createPoint(48.0, 58.0),
        GeometryFactory.createPoint(45.0, 58.0),
        GeometryFactory.createPoint(45.0, 60.0),
        GeometryFactory.createPoint(50.0, 60.0),
        GeometryFactory.createPoint(50.0, 50.0)
    };
    verifyPointListsEqual(expected, ((Polygon)output).getExteriorRing().getPoints());
  } 

  // Create a doughnut geometry and clip it to region that bisects it in
  // the middle and extends to the right side of the inner ring.
  @Test
  @Category(UnitTest.class)
  public void testClipDoughnutAtInsideEdge()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 50.0),
      GeometryFactory.createPoint(50.0, 50.0),
      GeometryFactory.createPoint(50.0, 60.0),
      GeometryFactory.createPoint(40.0, 60.0)
    };

    WritablePolygon input = GeometryFactory.createPolygon(inputs);

    Point[] interior = {
      GeometryFactory.createPoint(42.0, 52.0),
      GeometryFactory.createPoint(48.0, 52.0),
      GeometryFactory.createPoint(48.0, 58.0),
      GeometryFactory.createPoint(42.0, 58.0),
    };
    LinearRing interiorRing = GeometryFactory.createLinearRing(interior);
    input.addInteriorRing(interiorRing);

    Point[] clips = {
        GeometryFactory.createPoint(45.0, 45.0),
        GeometryFactory.createPoint(45.0, 65.0),
        GeometryFactory.createPoint(48.0, 65.0),
        GeometryFactory.createPoint(48.0, 45.0),
    };

    WritablePolygon clip = GeometryFactory.createPolygon(clips);
    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof GeometryCollection);

    // Check expected points in output
    GeometryCollection gc = (GeometryCollection)output;
    // Should be two triangles in the output
    Assert.assertEquals(2, gc.getNumGeometries());
    // Verify the first polygon
    {
      Geometry g = gc.getGeometry(0);
      Assert.assertTrue(g instanceof Polygon);
      Polygon p = (Polygon)g;
      LinearRing ring = p.getExteriorRing();
      Point[] expected = {
          GeometryFactory.createPoint(48.0, 50.0),
          GeometryFactory.createPoint(45.0, 50.0),
          GeometryFactory.createPoint(45.0, 52.0),
          GeometryFactory.createPoint(48.0, 52.0),
          GeometryFactory.createPoint(48.0, 50.0)
      };
      verifyPointListsEqual(expected, ring.getPoints());
    }
    // Verify the second polygon
    {
      Geometry g = gc.getGeometry(1);
      Assert.assertTrue(g instanceof Polygon);
      Polygon p = (Polygon)g;
      LinearRing ring = p.getExteriorRing();
      Point[] expected = {
          GeometryFactory.createPoint(45.0, 60.0),
          GeometryFactory.createPoint(48.0, 60.0),
          GeometryFactory.createPoint(48.0, 58.0),
          GeometryFactory.createPoint(45.0, 58.0),
          GeometryFactory.createPoint(45.0, 60.0)
      };
      verifyPointListsEqual(expected, ring.getPoints());
    }
  } 

  // Create a doughnut geometry and clip it to region that bisects it in
  // the middle and extends beyond the right side. It clips vertically
  // between the outside edges of the doughnut.
  @Test
  @Category(UnitTest.class)
  public void testClipDoughnutAtTopOutsideEdge()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 50.0),
      GeometryFactory.createPoint(50.0, 50.0),
      GeometryFactory.createPoint(50.0, 60.0),
      GeometryFactory.createPoint(40.0, 60.0)
    };

    WritablePolygon input = GeometryFactory.createPolygon(inputs);

    Point[] interior = {
      GeometryFactory.createPoint(42.0, 52.0),
      GeometryFactory.createPoint(48.0, 52.0),
      GeometryFactory.createPoint(48.0, 58.0),
      GeometryFactory.createPoint(42.0, 58.0),
    };
    LinearRing interiorRing = GeometryFactory.createLinearRing(interior);
    input.addInteriorRing(interiorRing);

    Point[] clips = {
        GeometryFactory.createPoint(45.0, 50.0),
        GeometryFactory.createPoint(45.0, 60.0),
        GeometryFactory.createPoint(65.0, 60.0),
        GeometryFactory.createPoint(65.0, 50.0),
    };

    WritablePolygon clip = GeometryFactory.createPolygon(clips);
    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof Polygon);
    Point[] expected = {
        GeometryFactory.createPoint(50.0, 50.0),
        GeometryFactory.createPoint(45.0, 50.0),
        GeometryFactory.createPoint(45.0, 52.0),
        GeometryFactory.createPoint(48.0, 52.0),
        GeometryFactory.createPoint(48.0, 58.0),
        GeometryFactory.createPoint(45.0, 58.0),
        GeometryFactory.createPoint(45.0, 60.0),
        GeometryFactory.createPoint(50.0, 60.0),
        GeometryFactory.createPoint(50.0, 50.0)
    };
    verifyPointListsEqual(expected, ((Polygon)output).getExteriorRing().getPoints());
  } 

  // Create a doughnut geometry and clip it to region that bisects it in
  // the middle and extends beyond the right side. It clips vertically
  // between the inside top/bottom edges of the doughnut.
  @Test
  @Category(UnitTest.class)
  public void testClipDoughnutAtTopInsideEdge()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 50.0),
      GeometryFactory.createPoint(50.0, 50.0),
      GeometryFactory.createPoint(50.0, 60.0),
      GeometryFactory.createPoint(40.0, 60.0)
    };

    WritablePolygon input = GeometryFactory.createPolygon(inputs);

    Point[] interior = {
      GeometryFactory.createPoint(42.0, 52.0),
      GeometryFactory.createPoint(48.0, 52.0),
      GeometryFactory.createPoint(48.0, 58.0),
      GeometryFactory.createPoint(42.0, 58.0),
    };
    LinearRing interiorRing = GeometryFactory.createLinearRing(interior);
    input.addInteriorRing(interiorRing);

    Point[] clips = {
        GeometryFactory.createPoint(45.0, 52.0),
        GeometryFactory.createPoint(45.0, 58.0),
        GeometryFactory.createPoint(65.0, 58.0),
        GeometryFactory.createPoint(65.0, 52.0),
    };

    WritablePolygon clip = GeometryFactory.createPolygon(clips);
    Geometry output = input.clip(clip);
    Assert.assertNotNull(output);
    Assert.assertTrue(output instanceof Polygon);
    // TO-DO: Need to determine correct set of expected points
    Point[] expected = {
        GeometryFactory.createPoint(50.0, 58.0),
        GeometryFactory.createPoint(50.0, 52.0),
        GeometryFactory.createPoint(48.0, 52.0),
        GeometryFactory.createPoint(48.0, 58.0),
        GeometryFactory.createPoint(50.0, 58.0)
    };
    verifyPointListsEqual(expected, ((Polygon)output).getExteriorRing().getPoints());
  } 

  // Create a doughnut geometry and clip it to region that bisects it in
  // the middle and extends beyond the right side.
  @Test
  @Category(UnitTest.class)
  public void testClipDoughnutCompletelyInside()
  {
    Point[] inputs = {
      GeometryFactory.createPoint(40.0, 50.0),
      GeometryFactory.createPoint(50.0, 50.0),
      GeometryFactory.createPoint(50.0, 60.0),
      GeometryFactory.createPoint(40.0, 60.0)
    };

    WritablePolygon input = GeometryFactory.createPolygon(inputs);

    Point[] interior = {
      GeometryFactory.createPoint(42.0, 52.0),
      GeometryFactory.createPoint(48.0, 52.0),
      GeometryFactory.createPoint(48.0, 58.0),
      GeometryFactory.createPoint(42.0, 58.0),
    };
    LinearRing interiorRing = GeometryFactory.createLinearRing(interior);
    input.addInteriorRing(interiorRing);

    Point[] clips = {
        GeometryFactory.createPoint(45.0, 54.0),
        GeometryFactory.createPoint(45.0, 57.0),
        GeometryFactory.createPoint(47.0, 57.0),
        GeometryFactory.createPoint(47.0, 54.0),
    };

    WritablePolygon clip = GeometryFactory.createPolygon(clips);
    Geometry output = input.clip(clip);
    Assert.assertNull(output);
  } 
}
