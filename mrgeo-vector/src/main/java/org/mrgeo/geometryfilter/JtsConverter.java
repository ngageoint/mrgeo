/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;

import java.util.Vector;

public class JtsConverter
{

  public static Geometry convertToJts(org.mrgeo.geometry.Geometry in)
  {
    if (in instanceof org.mrgeo.geometry.GeometryCollection)
    {
      return convertToJts((org.mrgeo.geometry.GeometryCollection) in);
    }
    if (in instanceof org.mrgeo.geometry.LinearRing)
    {
      return convertToJts((org.mrgeo.geometry.LinearRing) in);
    }
    if (in instanceof org.mrgeo.geometry.LineString)
    {
      return convertToJts((org.mrgeo.geometry.LineString) in);
    }
    if (in instanceof org.mrgeo.geometry.Polygon)
    {
      return convertToJts((org.mrgeo.geometry.Polygon) in);
    }
    if (in instanceof org.mrgeo.geometry.Point)
    {
      return convertToJts((org.mrgeo.geometry.Point) in);
    }
    throw new IllegalArgumentException(String.format("The %s geometry type is not supported.", in
        .getClass().getCanonicalName()));
  }

  public static GeometryCollection convertToJts(org.mrgeo.geometry.GeometryCollection in)
  {
    Vector<Geometry> geometries = new Vector<Geometry>();
    for (org.mrgeo.geometry.Geometry g : in.getGeometries())
    {
      geometries.add(convertToJts(g));
    }
    return new GeometryCollection(geometries.toArray(new Geometry[0]), defaultGf());
  }

  public static Polygon convertToJts(org.mrgeo.geometry.Polygon in)
  {
    Vector<LinearRing> holes = new Vector<LinearRing>();
    for (int i = 0; i < in.getNumInteriorRings(); i++)
    {
      holes.add(convertToJts(in.getInteriorRing(i)));
    }
    return new Polygon(convertToJts(in.getExteriorRing()), holes.toArray(new LinearRing[0]),
        defaultGf());
  }

  public static LinearRing convertToJts(org.mrgeo.geometry.LinearRing in)
  {
    Vector<Coordinate> c = new Vector<Coordinate>();
    for (org.mrgeo.geometry.Point p : in.getPoints())
    {
      c.add(convertToCoordinate(p));
    }

    if (!c.firstElement().equals2D(c.lastElement()))
    {
      // close the ring
      c.add(c.firstElement());
    }

    return new LinearRing(new CoordinateArraySequence(c.toArray(new Coordinate[0])), defaultGf());
  }

  public static LineString convertToJts(org.mrgeo.geometry.LineString in)
  {
    Vector<Coordinate> c = new Vector<Coordinate>();
    for (org.mrgeo.geometry.Point p : in.getPoints())
    {
      c.add(convertToCoordinate(p));
    }
    return new LineString(new CoordinateArraySequence(c.toArray(new Coordinate[0])), defaultGf());
  }

  public static Point convertToJts(org.mrgeo.geometry.Point in)
  {
    return defaultGf().createPoint(convertToCoordinate(in));
  }

  public static Coordinate convertToCoordinate(org.mrgeo.geometry.Point p)
  {
    return new Coordinate(p.getX(), p.getY(), p.getZ());
  }

  public static GeometryFactory defaultGf()
  {
    return new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326);
  }
}
