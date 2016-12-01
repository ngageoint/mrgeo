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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.geometry.*;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryCollection;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.LineString;
import org.mrgeo.geometry.LinearRing;
import org.mrgeo.geometry.Point;
import org.mrgeo.geometry.Polygon;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeometryUtils
{
private static Logger log = LoggerFactory.getLogger(GeometryUtils.class);

  final private static double epsilon = 0.00000001;

  @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "Checking stored type 1st")
  public static Geometry clip(final Geometry geometry, final Polygon clip)
  {
    switch (geometry.type())
    {
    case COLLECTION:
      return clip((GeometryCollection) geometry, clip);
    case LINEARRING:
      return clip((LinearRing) geometry, clip);
    case LINESTRING:
      return clip((LineString) geometry, clip);
    case POINT:
      return clip((Point) geometry, clip);
    case POLYGON:
      return clip((Polygon) geometry, clip);
    default:
      return null;
    }
  }

  public static Geometry clip(final GeometryCollection collection, final Polygon clip)
  {
    final WritableGeometryCollection clipped = GeometryFactory.createGeometryCollection(collection
      .getAllAttributes());
    final com.vividsolutions.jts.geom.Polygon jtsClip = clip.toJTS();

    for (final Geometry g : collection.getGeometries())
    {
      final com.vividsolutions.jts.geom.Geometry jtsClipped = intersect(jtsClip, g.toJTS());

      if (!jtsClipped.isEmpty())
      {
        if (jtsClipped instanceof com.vividsolutions.jts.geom.GeometryCollection)
        {
          final com.vividsolutions.jts.geom.GeometryCollection jtsColl = (com.vividsolutions.jts.geom.GeometryCollection) jtsClipped;
          for (int j = 0; j < jtsColl.getNumGeometries(); j++)
          {
            final com.vividsolutions.jts.geom.Geometry jg = jtsColl.getGeometryN(j);
            if (!jg.isEmpty())
            {
              clipped.addGeometry(GeometryFactory.fromJTS(jg));
            }
          }
        }
        else
        {
          clipped.addGeometry(GeometryFactory.fromJTS(jtsClipped));
        }
      }
    }

    if (clipped.isEmpty())
    {
      return null;
    }

    return clipped;
  }

  public static Geometry clip(final LinearRing ring, final Polygon clip)
  {
    final com.vividsolutions.jts.geom.Geometry jtsClipped = intersect(clip.toJTS(), ring.toJTS());

    if (jtsClipped.isEmpty())
    {
      return null;
    }

    final WritableGeometry clipped = GeometryFactory.fromJTS(jtsClipped);
    clipped.setAttributes(ring.getAllAttributes());
    return clipped;
  }

  public static Geometry clip(final LineString line, final Polygon clip)
  {
    final com.vividsolutions.jts.geom.Geometry jtsClipped = intersect(clip.toJTS(), line.toJTS());

    if (jtsClipped.isEmpty())
    {
      return null;
    }

    final WritableGeometry clipped = GeometryFactory.fromJTS(jtsClipped);
    clipped.setAttributes(line.getAllAttributes());
    return clipped;
  }

  public static Geometry clip(final Point point, final Polygon clip)
  {
    if (inside(clip, point))
    {
      return point;
    }
    return null;
  }

  public static Geometry clip(final Polygon poly, final Polygon clip)
  {
    final com.vividsolutions.jts.geom.Geometry jtsClipped = intersect(clip.toJTS(), poly.toJTS());

    if (jtsClipped.isEmpty())
    {
      return null;
    }

    final WritableGeometry clipped = GeometryFactory.fromJTS(jtsClipped);
    clipped.setAttributes(poly.getAllAttributes());

    return clipped;
  }

  public static boolean colinear(final Point p0, final Point p1, final Point p2)
  {
    return FloatUtils.isEqual((p1.getY() - p0.getY()) * (p2.getX() - p1.getX()), (p2.getY() - p1.getY()) *
      (p1.getX() - p0.getX()));
  }

  // Take the point P and form a ray that
  // passes through the polygon but does not pass through a
  // vertex in the polygon.
  // we can do that by finding a point outside the polygon
  // We do this by finding the largest x coordinate in the
  // polygon and adding 1 to get the line x = xmax. Then we
  // need to pick a y value
  public static Point computeIntersection(final Point v0s, final Point v0e, final Point v1s,
    final Point v1e)
  {
    final Point dc = GeometryFactory.createPoint(v0s.getX() - v0e.getX(), v0s.getY() - v0e.getY());
    final Point dp = GeometryFactory.createPoint(v1s.getX() - v1e.getX(), v1s.getY() - v1e.getY());

    final double n1 = v0s.getX() * v0e.getY() - v0s.getY() * v0e.getX();
    final double n2 = v1s.getX() * v1e.getY() - v1s.getY() * v1e.getX();
    final double n3 = 1.0 / (dc.getX() * dp.getY() - dc.getY() * dp.getX());

    return GeometryFactory.createPoint((n1 * dp.getX() - n2 * dc.getX()) * n3,
      (n1 * dp.getY() - n2 * dc.getY()) * n3);
  }

  public static boolean contains(final Point v0, final Point v1, final Point p)
  {
    return Math.abs(dist(v0, v1) - (dist(v0, p) + dist(v1, p))) < epsilon;
  }

  public static double dist(final Point p0, final Point p1)
  {
    return Math.sqrt(dist2(p0, p1));
  }

  public static double dist2(final Point p0, final Point p1)
  {
    final double dx = p1.getX() - p0.getX();
    final double dy = p1.getY() - p0.getY();
    return (dx * dx) + (dy * dy);
  }

  public static boolean inside(final Polygon poly, final Point p)
  {
    final LinearRing ring = poly.getExteriorRing();
    final Bounds b = ring.getBounds();
    if (b.contains(p.getX(), p.getY()))
    {
      boolean odd = false;

      final double x = p.getX();
      final double y = p.getY();

      Point r1 = ring.getPoint(0);
      for (int i = 1; i < ring.getNumPoints(); i++)
      {
        final Point r2 = ring.getPoint(i);

        final double r1x = r1.getX();
        final double r1y = r1.getY();

        final double r2x = r2.getX();
        final double r2y = r2.getY();

        if ((r1y < y && r2y >= y || r2y < y && r1y >= y) && (r1x <= x || r2x <= x))
        {
          odd ^= (r1x + (y - r1y) / (r2y - r1y) * (r2x - r1x) < x);
        }

        r1 = r2;
      }

      return odd;
    }

    return false;
  }

  public static boolean intersects(final Polygon polygon, final Geometry geometry)
  {
    if (geometry.type() == Geometry.Type.COLLECTION && geometry instanceof GeometryCollection)
    {
      GeometryCollection gc = (GeometryCollection)geometry;
      for (Geometry geom: gc.getGeometries())
      {
        if (intersects(polygon, geom))
        {
          return true;
        }
      }
      return false;
    }
    else
    {
      final com.vividsolutions.jts.geom.Polygon jtsPoly = polygon.toJTS();
      final com.vividsolutions.jts.geom.Geometry jtsGeom = geometry.toJTS();

      return jtsGeom.within(jtsPoly) || jtsGeom.contains(jtsPoly) || jtsGeom.intersects(jtsPoly);
    }
  }

  public static boolean intersects(final Point A, final Point B, final Point C, final Point D)
  {
    return intersects(A, B, C, D, null);
  }

  // inspired by: http://www.gamedev.net/topic/222263-test-if-two-2d-line-segments-overlap/
  public static boolean intersects(final Point A, final Point B, final Point C, final Point D,
    final WritablePoint intersection)
  {

    final double ax = A.getX();
    final double ay = A.getY();

    final double bx = B.getX();
    final double by = B.getY();

    final double lbx = bx - ax;
    final double lby = by - ay;

    final double cx = C.getX();
    final double cy = C.getY();

    final double ldx = D.getX() - C.getX();
    final double ldy = D.getY() - C.getY();

    // final double dx = D.getX();
    final double dy = D.getY();

    final double pdDotb = -ldx * lby + lbx * ldy;
    final double pdDotc = ldx * (ay - cy) - ldy * (ax - cx);
    final double pbDotc = -lby * (ax - cx) + lbx * (ay - cy);

    // parallel?
    if (FloatUtils.isEqual(pdDotb, 0.0))
    {
      // collinear?
      if (FloatUtils.isEqual(pdDotc, 0.0))
      {
        final double v;
        final double w;

        if (!FloatUtils.isEqual(lbx, 0.0))
        {
          v = (cx - ax) / lbx;
          w = ay + (lby * (cx - ax) / lbx);

          // overlapping?
          if (FloatUtils.isEqual(cy, w) && v >= 0 && v <= 1)
          {
            if (intersection != null)
            {
              final double x = ax + (v * lbx);
              final double y = w;

              intersection.setX(x);
              intersection.setY(y);
            }
            return true;
          }
        }
        else
        {
          // vertical line, this is now a 1D problem
          double y;

          double a, b, c, d;

          // make sure the points are in order...
          if (ay < by)
          {
            a = ay;
            b = by;
          }
          else
          {
            a = by;
            b = ay;
          }
          if (cy < dy)
          {
            c = cy;
            d = dy;
          }
          else
          {
            c = dy;
            d = cy;
          }

          if (a >= c && a <= d)
          {
            y = a;
          }
          else if (b >= c && b <= d)
          {
            y = b;
          }
          else if (c >= a && c <= b)
          {
            y = c;
          }
          else if (d >= a && d <= b)
          {
            y = d;
          }
          else
          {
            // don't overlap
            return false;
          }

          if (intersection != null)
          {
            intersection.setX(ax);
            intersection.setY(y);
          }

          return true;

        }
      }

      return false;
    }

    final double u = pbDotc / pdDotb;
    final double t = pdDotc / pdDotb;

    // Check for intersection
    if ((u >= 0 && u <= 1) && (t >= 0 && t <= 1))
    {
      final double x = ax + (t * lbx);
      final double y = ay + (t * lby);
      if (intersection != null)
      {
        intersection.setX(x);
        intersection.setY(y);
      }
      return true;
    }

    return false;
  }

  public static boolean isOn(final LinearRing clip, final Point p)
  {
    Point c1 = clip.getPoint(0);
    for (int j = 1; j < clip.getNumPoints(); j++)
    {
      final Point c2 = clip.getPoint(j);
      if (contains(c1, c2, p))
      {
        return true;
      }

      c1 = c2;
    }

    return false;
  }

  public static Polygon toPoly(final Bounds bounds)
  {
    final WritablePolygon poly = GeometryFactory.createPolygon();
    poly.setExteriorRing(toRing(bounds));
    return poly;
  }

  public static LinearRing toRing(final Bounds bounds)
  {
    final WritableLinearRing ring = GeometryFactory.createLinearRing();
    ring.addPoint(bounds.w, bounds.s);
    ring.addPoint(bounds.w, bounds.n);
    ring.addPoint(bounds.e, bounds.n);
    ring.addPoint(bounds.e, bounds.s);

    ring.closeRing();
    return ring;
  }

  static boolean inside(final Point v0, final Point v1, final Point p)
  {
    return ((v1.getX() - v0.getX()) * (p.getY() - v0.getY())) > ((v1.getY() - v0.getY()) * (p
      .getX() - v0.getX()));
  }


@SuppressWarnings("squid:S1166") // Exception caught and handled
static com.vividsolutions.jts.geom.Geometry intersect(
    final com.vividsolutions.jts.geom.Polygon jtsClip,
    final com.vividsolutions.jts.geom.Geometry jtsGeom)
  {
    com.vividsolutions.jts.geom.Geometry jtsClipped;
    try
    {
      jtsClipped = jtsGeom.intersection(jtsClip);
    }
    catch (final TopologyException e)
    {
      final com.vividsolutions.jts.geom.Geometry g = TopologyPreservingSimplifier.simplify(jtsGeom,
          1E-8);
      try
      {
        jtsClipped = g.intersection(jtsClip);
      }
      catch (final TopologyException e1)
      {
        log.error("JTS Topology problem: clip area: " + jtsClip.toString() + " geom: " + jtsGeom.toString() + " message: " + e1.getMessage());
        log.error("Exception thrown", e1);

        return new com.vividsolutions.jts.geom.GeometryFactory().createPoint((Coordinate)null);
      }
    }

    return jtsClipped;
  }

}
