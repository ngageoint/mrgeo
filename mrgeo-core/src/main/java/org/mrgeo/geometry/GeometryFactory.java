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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collection;
import java.util.Map;

public class GeometryFactory
{
  private GeometryFactory()
  {

  }

  public static WritableGeometry createGeometry(final Geometry.Type type)
  {
    switch (type)
    {
    case COLLECTION:
      return new GeometryCollectionImpl();
    case LINEARRING:
      return new LinearRingImpl();
    case LINESTRING:
      return new LineStringImpl();
    case POINT:
      return new PointImpl();
    case POLYGON:
      return new PolygonImpl();
    default:
      break;
    }

    return null;
  }

  public static WritableGeometry createGeometry(final String className)
  {
    if (className.equals(WritablePoint.class.getCanonicalName()))
    {
      return new PointImpl();
    }
    else if (className.equals(WritableLineString.class.getCanonicalName()))
    {
      return new LineStringImpl();
    }
    else if (className.equals(WritableLinearRing.class.getCanonicalName()))
    {
      return new LinearRingImpl();
    }
    else if (className.equals(WritablePolygon.class.getCanonicalName()))
    {
      return new PolygonImpl();
    }
    else if (className.equals(WritableGeometryCollection.class.getCanonicalName()))
    {
      return new GeometryCollectionImpl();
    }

    return null;
  }

  public static WritableGeometry createEmptyGeometry() { return createPoint(); }
  public static WritableGeometry createEmptyGeometry(final Map<String, String> attributes) { return createPoint(attributes); }

  public static WritableGeometryCollection createGeometryCollection()
  {
    return new GeometryCollectionImpl();
  }

  public static WritableGeometryCollection createGeometryCollection(
    final Map<String, String> attributes)
  {
    return new GeometryCollectionImpl(attributes);
  }

  public static WritableLinearRing createLinearRing()
  {
    return new LinearRingImpl();
  }

  public static WritableLinearRing createLinearRing(final Map<String, String> attributes)
  {
    return new LinearRingImpl(attributes);
  }

  public static WritableLinearRing createLinearRing(final Point... points)
  {
    return new LinearRingImpl(points);
  }

  public static WritableLinearRing createLinearRing(final LineString lineString)
  {
    LinearRingImpl lr = new LinearRingImpl(lineString.getAllAttributes());
    lr.setPoints(lineString.getPoints());
    return lr;
  }

  public static WritableLineString createLineString()
  {
    return new LineStringImpl();
  }

  public static WritableLineString createLineString(final Map<String, String> attributes)
  {
    return new LineStringImpl(attributes);
  }

  public static WritableLineString createLineString(final Point... points)
  {
    return new LineStringImpl(points);
  }

  public static WritablePoint createPoint()
  {
    return new PointImpl();
  }

  public static WritablePoint createPoint(final double x, final double y, final Map<String, String> attributes)
  {
    return new PointImpl(x, y, attributes);
  }
  public static WritablePoint createPoint(final double x, final double y)
  {
    return new PointImpl(x, y);
  }

  public static WritablePoint createPoint(final double x, final double y, final double z)
  {
    return new PointImpl(x, y, z);
  }

  public static WritablePoint createPoint(final double x, final double y, final double z,
    final Map<String, String> attributes)
  {
    return new PointImpl(x, y, z, attributes);
  }

  public static WritablePoint createPoint(final Map<String, String> attributes)
  {
    return new PointImpl(attributes);
  }

  public static WritablePolygon createPolygon()
  {
    return new PolygonImpl();
  }

  public static WritablePolygon createPolygon(final Collection<Point> points)
  {
    return new PolygonImpl(points);
  }

  public static WritablePolygon createPolygon(final Map<String, String> attributes)
  {
    return new PolygonImpl(attributes);
  }

  public static WritablePolygon createPolygon(final Point... points)
  {
    return new PolygonImpl(points);
  }

  public static WritableGeometry fromJTS(final com.vividsolutions.jts.geom.Geometry jtsGeometry)
  {
    return fromJTS(jtsGeometry, null);
  }

  public static WritableGeometry fromJTS(final com.vividsolutions.jts.geom.Geometry jtsGeometry, final Map<String, String> attributes)
  {
    if (jtsGeometry instanceof com.vividsolutions.jts.geom.Point)
    {
      final WritablePoint pt = createPoint();
      pt.fromJTS((com.vividsolutions.jts.geom.Point) jtsGeometry);
      if (attributes != null)
      {
        pt.setAttributes(attributes);
      }
      return pt;
    }
    // make sure LinearRing is before LineString!
    else if (jtsGeometry instanceof com.vividsolutions.jts.geom.LinearRing)
    {
      final WritableLinearRing ring = createLinearRing();
      ring.fromJTS((com.vividsolutions.jts.geom.LineString) jtsGeometry);
      if (attributes != null)
      {
        ring.setAttributes(attributes);
      }
      return ring;
    }
    else if (jtsGeometry instanceof com.vividsolutions.jts.geom.LineString)
    {
      final WritableLineString line = createLineString();
      line.fromJTS((com.vividsolutions.jts.geom.LineString) jtsGeometry);
      if (attributes != null)
      {
        line.setAttributes(attributes);
      }
      return line;
    }
    else if (jtsGeometry instanceof com.vividsolutions.jts.geom.Polygon)
    {
      final WritablePolygon polygon = createPolygon();
      polygon.fromJTS((com.vividsolutions.jts.geom.Polygon) jtsGeometry);
      if (attributes != null)
      {
        polygon.setAttributes(attributes);
      }
      return polygon;
    }
    else if (jtsGeometry instanceof com.vividsolutions.jts.geom.GeometryCollection)
    {
      // if we only have 1 geometry in the collection, collapse it to that type
      if (((com.vividsolutions.jts.geom.GeometryCollection) jtsGeometry).getNumGeometries() == 1)
      {
        return fromJTS(((com.vividsolutions.jts.geom.GeometryCollection) jtsGeometry)
          .getGeometryN(0), attributes);
      }

      final WritableGeometryCollection collection = createGeometryCollection();
      collection.fromJTS((com.vividsolutions.jts.geom.GeometryCollection) jtsGeometry);
      if (attributes != null)
      {
        collection.setAttributes(attributes);
      }
      return collection;
    }
    return null;
  }

}
