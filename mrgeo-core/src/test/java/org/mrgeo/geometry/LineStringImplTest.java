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
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.GeometryUtils;
import org.mrgeo.utils.tms.Bounds;

@SuppressWarnings("static-method")
public class LineStringImplTest extends LocalRunnerTest
{
  private static final double EPSILON = 1e-12;

  @Test
  @Category(UnitTest.class)
  public void clipAtZeroLatitude()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-0.002, 0.0),
      GeometryFactory.createPoint(0.0, 0.0),
      GeometryFactory.createPoint(0.002, 0.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(-0.002, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(-0.002, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.002, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, 0.0, 180.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.002, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipAtMinLatitude()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-0.002, -90.0),
      GeometryFactory.createPoint(0.0, -90.0),
      GeometryFactory.createPoint(0.002, -90.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(-0.002, p.getX(), EPSILON);
      Assert.assertEquals(-90.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(-90.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
    {
      Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(-90.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.002, p.getX(), EPSILON);
      Assert.assertEquals(-90.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, 0.0, 180.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipAtMaxLatitude()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-0.002, 90.0),
      GeometryFactory.createPoint(0.0, 90.0),
      GeometryFactory.createPoint(0.002, 90.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
    {
      Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(-0.002, p.getX(), EPSILON);
      Assert.assertEquals(90.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(90.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
    {
      Bounds bbox = new Bounds(0.0, 0.0, 180.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(90.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.002, p.getX(), EPSILON);
      Assert.assertEquals(90.0, p.getY(), EPSILON);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipAtZeroLongitude()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(0.0, -0.002),
      GeometryFactory.createPoint(0.0, 0.0),
      GeometryFactory.createPoint(0.0, 0.002)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(-0.002, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.002, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(-0.002, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, 0.0, 180.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(0.0, p.getX(), EPSILON);
      Assert.assertEquals(0.002, p.getY(), EPSILON);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipAtMinLongitude()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-180.0, -0.002),
      GeometryFactory.createPoint(-180.0, 0.0),
      GeometryFactory.createPoint(-180.0, 0.002)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(-180.0, p.getX(), EPSILON);
      Assert.assertEquals(-0.002, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(-180.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNotNull(g);
      Assert.assertTrue(g instanceof LineString);
      LineString ls = (LineString)g;
      Assert.assertEquals(2, ls.getNumPoints());
      Point p = ls.getPoint(0);
      Assert.assertEquals(-180.0, p.getX(), EPSILON);
      Assert.assertEquals(0.0, p.getY(), EPSILON);
      p = ls.getPoint(1);
      Assert.assertEquals(-180.0, p.getX(), EPSILON);
      Assert.assertEquals(0.002, p.getY(), EPSILON);
    }
    {
      Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
    {
      Bounds bbox = new Bounds(0.0, 0.0, 180.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipWithLineTouchingTop()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-90.0, 0.002),
      GeometryFactory.createPoint(-90.0, 0.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipWithLineTouchingBottom()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-90.0, -0.002),
      GeometryFactory.createPoint(-90.0, 0.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, 0.0, 0.0, 90.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipWithLineTouchingLeft()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(-0.002, -45.0),
      GeometryFactory.createPoint(0.0, -45.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(0.0, -90.0, 180.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void clipWithLineTouchingRight()
  {
    Point[] linePoints = {
      GeometryFactory.createPoint(0.0, -45.0),
      GeometryFactory.createPoint(0.002, -45.0)
    };

    LineString line = GeometryFactory.createLineString(linePoints);
    {
      Bounds bbox = new Bounds(-180.0, -90.0, 0.0, 0.0);
      Geometry g = line.clip(bbox);
      Assert.assertNull(g);
    }
  }
  
  @Test
  @Category(UnitTest.class)
  public void testlineclip()
  {
    // polygon and answer taken from http://rosettacode.org/wiki/Sutherland-Hodgman_polygon_clipping

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

    LineString input = GeometryFactory.createLineString(inputs);

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

    Point[][] outputs = {
      {
        GeometryFactory.createPoint(10.0000000000, 11.6666666667),
        GeometryFactory.createPoint(12.5000000000, 10.0000000000),
      },
      {
        GeometryFactory.createPoint(27.5000000000, 10.0000000000),
        GeometryFactory.createPoint(30.0000000000, 11.6666666667),
      },
      {
        GeometryFactory.createPoint(30.0000000000, 30.0000000000),
        GeometryFactory.createPoint(25.0000000000, 30.0000000000),
      },
      {
        GeometryFactory.createPoint(25.0000000000, 30.0000000000),
        GeometryFactory.createPoint(20.0000000000, 25.0000000000),
        GeometryFactory.createPoint(17.5000000000, 30.0000000000),
      },
      {
        GeometryFactory.createPoint(12.5000000000, 30.0000000000),
        GeometryFactory.createPoint(10.0000000000, 25.0000000000), 
      },
      {
        GeometryFactory.createPoint(10.0000000000, 25.0000000000), 
        GeometryFactory.createPoint(10.0000000000, 20.0000000000), 
      },
    };

    Geometry output = GeometryUtils.clip(input, clip);

    Assert.assertTrue("Wrong geometry type, should be GeometryCollection", output instanceof GeometryCollection);
    GeometryCollection gc = (GeometryCollection) output;
    for (int g = 0; g < gc.getNumGeometries(); g++)
    {
      Geometry geom = gc.getGeometry(g);
      Assert.assertTrue("Wrong geometry type, should be LineString", geom instanceof LineString);

      LineString line = (LineString) geom;
      for (int i = 0; i < line.getNumPoints(); i++)
      {
        Point pt = line.getPoint(i);

        Assert.assertEquals("Points x don't match (" + g + ")", outputs[g][i].getX(), pt.getX(), 0.00001);
        Assert.assertEquals("Points y don't match (" + g + ")", outputs[g][i].getY(), pt.getY(), 0.00001);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testlineclipAllInside()
  {

    Point[] inputs = {
      GeometryFactory.createPoint(20.0, 20.0),
      GeometryFactory.createPoint(20.0, 30.0),
      GeometryFactory.createPoint(30.0, 30.0),
      GeometryFactory.createPoint(30.0, 20.0)
    };

    LineString input = GeometryFactory.createLineString(inputs);

    Point[] clips = {
      GeometryFactory.createPoint(10.0, 10.0),
      GeometryFactory.createPoint(40.0, 10.0),
      GeometryFactory.createPoint(40.0, 40.0),
      GeometryFactory.createPoint(10.0, 40.0)
    };

    Polygon clip = GeometryFactory.createPolygon(clips);

    Geometry output = GeometryUtils.clip(input, clip);
    Assert.assertTrue("Wrong geometry type, should be LineString", output instanceof LineString);
    LineString ring = (LineString) output;

    for (int i = 0; i < ring.getNumPoints(); i++)
    {
      Point pt = ring.getPoint(i);
      Assert.assertEquals("Points x don't match (" + i + ")", inputs[i].getX(), pt.getX(), 0.00001);
      Assert.assertEquals("Points y don't match (" + i + ")", inputs[i].getY(), pt.getY(), 0.00001);
    }
  } 

  @Test
  @Category(UnitTest.class)
  public void testlineclipAllOutside()
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

    Geometry output = GeometryUtils.clip(input, clip);
    Assert.assertNull("Output should be null", output);
  } 

  @Test
  @Category(UnitTest.class)
  public void testlineclipMultiPoints()
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

    LineString input = GeometryFactory.createLineString(inputs);

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

    // output    
    //  {175.0 300.0},
    //  {150.0 350.0},
    //  {125.0 300.0},
    // and
    //  {300.0 300.0},
    //  {300.0 325.0},
    //  {250.0 300.0},
    Point[][] outputs = {
      {
        GeometryFactory.createPoint(30.00, 32.50),
        GeometryFactory.createPoint(25.00, 30.00),
      },
      {
      GeometryFactory.createPoint(17.50, 30.00),
      GeometryFactory.createPoint(15.00, 35.00),
      GeometryFactory.createPoint(12.50, 30.00),
    },
    };

    Geometry output = GeometryUtils.clip(input, clip);

    Assert.assertTrue("Wrong geometry type, should be GeometryCollection", output instanceof GeometryCollection);

    GeometryCollection gc = (GeometryCollection) output;
    for (int g = 0; g < gc.getNumGeometries(); g++)
    {
      LineString line = (LineString) gc.getGeometry(g);

      for (int i = 0; i < line.getNumPoints(); i++)
      {

        Point pt = line.getPoint(i);
        
        Assert.assertEquals("Points x don't match (" + i + ")", outputs[g][i].getX(), pt.getX(), 0.00001);
        Assert.assertEquals("Points y don't match (" + i + ")", outputs[g][i].getY(), pt.getY(), 0.00001);
      }
      System.out.println();
    }
  }

}
