/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.esri.geom;

import java.util.Vector;

/**
 * Coordinate Algebra Geometry for data in Decimal Degrees (i.e., projection
 * Geographic) In some cases, measurements returned in km.
 */
@SuppressWarnings("unchecked")
public class SphericalTools
{
  public final static byte KM = 1;
  public final static byte PERCENT = 2;

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#contains(esri.geom.JShape, esri.geom.JShape)
   */
  public static boolean contains(JShape shp1, JShape shp2)
  {
    switch (shp1.getType())
    {
    case JShape.POINT:
    {
      switch (shp2.getType())
      {
      case JShape.POINT:
      {
        JPoint p1 = (JPoint) shp1;
        JPoint p2 = (JPoint) shp2;
        if (p1.equals(p2))
        {
          return true;
        }
        return false;
      }
      default:
      {
        return false;
      }
      }
    }
    case JShape.POLYLINE:
    {
      return false;
    }
    case JShape.POLYGON:
    {
      switch (shp2.getType())
      {
      case JShape.POINT:
      {
        JPolygon poly = (JPolygon) shp1;
        JPoint p2 = (JPoint) shp2;
        if (poly.contains(p2.getCoord()))
        {
          return true;
        }
        return false;
      }
      default:
      {
        throw new InternalError("Contains algorithm not implemented for shape types!");
      }
      }
    }
    default:
    {
      throw new InternalError("Contains algorithm not implemented for shape types!");
    }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getCoordOnLine(esri.geom.JLine, double, boolean)
   */
  public static Coord getCoordOnLine(JPolyLine line, double length, boolean reverse)
  {
    int numparts = line.getPartCount();
    if (numparts != 1)
      throw new RuntimeException("getCoordOnLine not supported for other than 1 # of parts");
    // set array
    Coord[] c = new Coord[line.getPointCount()];
    for (int i = 0; i < line.getPointCount(); i++)
      c[i] = line.getPoint(i);
    // reverse?
    if (reverse)
    {
      Coord[] temp = new Coord[c.length];
      for (int j = 0; j < c.length; j++)
        temp[j] = c[(c.length - 1) - j];
      c = temp;
    }
    // calculate
    if (length <= 0)
      return c[0]; // return first
    double distance = 0;
    for (int i = 0; i < c.length - 1; i++)
    {
      double potential = getDistance(c[i], c[i + 1]);
      if (distance + potential >= length)
        return getCoordOnSegment(c[i], c[i + 1], potential - distance, KM);
      distance += potential;
    }
    return c[c.length - 1]; // return last
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getCoordOnSegment(esri.geom.Coord, esri.geom.Coord,
   * double, int)
   */
  public static Coord getCoordOnSegment(Coord c1, Coord c2, double value, int flag)
  {
    // coordinate c1 and c2 form a line segment, and the coordinate is sought a
    // distance from c1 in percentage or kilometers
    // WARNING! limit is c1 and c2 in result (see early exit below)
    double percent;
    switch (flag)
    {
    case KM:
    {
      double distance_Total = getDistance(c1, c2); // calc total length of line
                                                   // in Km
      percent = value / distance_Total; // percentage along line to c2
      break;
    }
    case PERCENT:
    {
      percent = value;
      break;
    }
    default:
    {
      return null;
    }
    }
    // check percent for early exit
    if (percent <= 0.0)
      return (Coord) c1.clone();
    if (percent >= 1.0)
      return (Coord) c2.clone();
    // create coordinate
    Coord temp = new Coord();
    temp.x = c1.x + percent * (c2.x - c1.x);
    temp.y = c1.y + percent * (c2.y - c1.y);
    // return coordinate
    return temp;
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getCoordsOnLine(esri.geom.JLine, double, boolean)
   */
  public static Coord[] getCoordsOnLine(JPolyLine line, double length, boolean reverse)
  {
    int numparts = line.getPartCount();
    if (numparts != 1)
      throw new RuntimeException("getCoordsOnLine not supported for other than 1 # of parts");
    // set array
    Coord[] c = new Coord[line.getPointCount()];
    for (int i = 0; i < line.getPointCount(); i++)
      c[i] = line.getPoint(i);
    // reverse?
    if (reverse)
    {
      Coord[] temp = new Coord[c.length];
      for (int j = 0; j < c.length; j++)
        temp[j] = c[(c.length - 1) - j];
      c = temp;
    }
    // check if whole line anyway
    boolean complete = false;
    if (length >= getLength(line))
      complete = true;
    // calculate
    @SuppressWarnings("rawtypes")
    Vector v = new Vector(1);
    v.add(c[0]); // add first
    double distance = 0;
    for (int i = 1; i < c.length; i++)
    {
      if (!complete)
      {
        double potential = getDistance(c[i - 1], c[i]); // KM
        if (distance + potential >= length)
        {
          Coord c2 = getCoordOnSegment(c[i - 1], c[i], (length - distance), KM);
          v.add(c2);
          break;
        }
        distance += potential;
      }
      v.add(c[i]);
    }
    if (complete)
      v.add(c[c.length - 1]); // add last
    // convert vector to array
    Coord[] temp = new Coord[v.size()];
    for (int i = 0; i < v.size(); i++)
    {
      temp[i] = (Coord) v.get(i);
    }
    // return
    return temp;
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getDistance(esri.geom.Coord, esri.geom.Coord)
   */
  public static double getDistance(Coord c1, Coord c2)
  {
    // WARNING: returns in KM!
    // calculates distance between 2 coordinate points provided in decimal
    // degrees!
    if (c1 == null || c2 == null)
      return 0;
    if (c1.equals(c2))
      return 0;
    // perform accurate spherical measurement using the "Haversine Formula"
    double conversion = Math.PI / 180;
    double x1 = c1.x * conversion; // rad/deg
    double y1 = c1.y * conversion; // rad/deg
    double x2 = c2.x * conversion; // rad/deg
    double y2 = c2.y * conversion; // rad/deg
    double dx = x2 - x1;
    double dy = y2 - y1;
    double a = Math.pow(Math.sin(dy / 2), 2) + Math.cos(y1) * Math.cos(y2)
        * Math.pow(Math.sin(dx / 2), 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    double R = 6378.137 - 21 * Math.sin((y1 + y2) / 2); // utilize best-fit R
                                                        // for average lat
    double d = R * c; // km!
    return d;
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getDistance(esri.geom.Coord, esri.geom.JShape)
   */
  public static double getDistance(Coord c, JShape shp)
  {
    switch (shp.getType())
    {
    case JShape.POINT:
      return getDistance(c, ((JPoint) shp).getCoord());
    case JShape.POLYLINE:
      return getDistanceToLine(c, (JPolyLine) shp);
    case JShape.POLYGON:
      return getDistanceToPolygon(c, (JPolygon) shp);
    default:
    {
      throw new InternalError("Distance algorithm not implemented for shape types!");
    }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getDistance(esri.geom.JShape, esri.geom.JShape)
   */
  public static double getDistance(JShape shp1, JShape shp2)
  {
    switch (shp1.getType())
    {
    case JShape.POINT:
    {
      switch (shp2.getType())
      {
      case JShape.POINT:
        return getDistance(((JPoint) shp1).getCoord(), ((JPoint) shp2).getCoord());
      case JShape.POLYLINE:
        return getDistanceToLine(((JPoint) shp1).getCoord(), (JPolyLine) shp2);
      case JShape.POLYGON:
        return getDistanceToPolygon(((JPoint) shp1).getCoord(), (JPolygon) shp2);
      default:
      {
        throw new InternalError("Distance algorithm not implemented for shape types!");
      }
      }
    }
    case JShape.POLYLINE:
    {
      switch (shp2.getType())
      {
      case JShape.POINT:
        return getDistanceToLine(((JPoint) shp2).getCoord(), (JPolyLine) shp1);
      default:
      {
        throw new InternalError("Distance algorithm not implemented for shape types!");
      }
      }
    }
    case JShape.POLYGON:
    {
      switch (shp2.getType())
      {
      case JShape.POINT:
        return getDistanceToPolygon(((JPoint) shp2).getCoord(), (JPolygon) shp1);
      default:
      {
        throw new InternalError("Distance algorithm not implemented for shape types!");
      }
      }
    }
    default:
    {
      throw new InternalError("Distance algorithm not implemented for shape types!");
    }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getDistanceToLine(esri.geom.Coord, esri.geom.JLine)
   */
  public static double getDistanceToLine(Coord c, JPolyLine line)
  {
    double shortest = Double.MAX_VALUE;
    for (int i = 0; i < line.getPartCount(); i++)
    {
      if (i == line.getPartCount() - 1)
      {
        for (int j = line.getPart(line.getPartCount() - 1); j < line.getPointCount() - 1; j++)
        {
          double tempd = getDistanceToSegment(c, line.getPoint(j), line.getPoint(j + 1));
          if (tempd < shortest)
            shortest = tempd;
        }
      }
      else
      {
        for (int j = line.getPart(i); j < line.getPart(i + 1) - 1; j++)
        {
          double tempd = getDistanceToSegment(c, line.getPoint(j), line.getPoint(j + 1));
          if (tempd < shortest)
            shortest = tempd;
        }
      }
    }
    return shortest;
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getDistanceToPolygon(esri.geom.Coord,
   * esri.geom.JPolygon)
   */
  public static double getDistanceToPolygon(Coord c, JPolygon poly)
  {
    // first check if inside
    if (poly.contains(c))
      return 0;

    // check to all line segments
    double shortest = Double.MAX_VALUE;
    for (int i = 0; i < poly.getPartCount(); i++)
    {
      if (i == poly.getPartCount() - 1)
      {
        for (int j = poly.getPart(poly.getPartCount() - 1); j < poly.getPointCount() - 1; j++)
        {
          double tempd = getDistanceToSegment(c, poly.getPoint(j), poly.getPoint(j + 1));
          if (tempd < shortest)
            shortest = tempd;
        }
      }
      else
      {
        for (int j = poly.getPart(i); j < poly.getPart(i + 1) - 1; j++)
        {
          double tempd = getDistanceToSegment(c, poly.getPoint(j), poly.getPoint(j + 1));
          if (tempd < shortest)
            shortest = tempd;
        }
      }
    }
    return shortest;
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getDistanceToSegment(esri.geom.Coord, esri.geom.Coord,
   * esri.geom.Coord)
   */
  public static double getDistanceToSegment(Coord p3, Coord p1, Coord p2)
  {
    // p3 is search point; p1, p2 are endpoints of line segment (a 2 point
    // segment!)
    // http://astronomy.swin.edu.au/pbourke/geometry/pointline/ (reference)
    // calc variable "u"
    double distance = Math.sqrt(Math.pow(p2.x - p1.x, 2) + Math.pow(p2.y - p1.y, 2));
    double denominator = Math.pow(distance, 2);
    double numerator = (p3.x - p1.x) * (p2.x - p1.x) + (p3.y - p1.y) * (p2.y - p1.y);
    double u = -1;
    if (denominator == 0)
    {
      u = 0;
    }
    else
    {
      u = numerator / denominator;
    }
    if (u >= 0 && u <= 1)
    {
      // calc point on the line
      Coord p4 = new Coord();
      p4.x = p1.x + u * (p2.x - p1.x);
      p4.y = p1.y + u * (p2.y - p1.y);
      return getDistance(p3, p4);
    }
    // use endpoints
    double d1 = getDistance(p3, p1);
    double d2 = getDistance(p3, p2);
    distance = (d1 < d2) ? d1 : d2;
    return distance;
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#getLength(esri.geom.JLine)
   */
  public static double getLength(JPolyLine line)
  {
    // WARNING: returns in KM!
    double sum = 0;
    for (int i = 1; i < line.getPointCount(); i++)
    {
      Coord c1 = line.getPoint(i - 1);
      Coord c2 = line.getPoint(i);
      sum += getDistance(c1, c2);
    }
    return sum;
  }

  public static Coord getPointFromNotation(String s) throws Exception
  {
    // latitude
    int latN = s.indexOf('N');
    int latS = s.indexOf('S');
    if ((latN > 0 && latS > 0) || (latN == 0 && latS == 0))
      throw new Exception("Invalid N-S format");
    int latPos = Math.max(latN, latS);
    String lat = s.substring(0, latPos);
    double latitude = Double.parseDouble(lat);
    if (latS > 0)
      latitude = latitude * -1;
    // longitude
    int lonE = s.indexOf('E');
    int lonW = s.indexOf('W');
    if ((lonE > 0 && lonW > 0) || (lonE == 0 && lonW == 0))
      throw new Exception("Invalid E-W format!");
    int lonPos = Math.max(lonE, lonW);
    String lon = s.substring(latPos + 1, lonPos);
    double longitude = Double.parseDouble(lon);
    if (lonW > 0)
      longitude = longitude * -1;
    // create point
    return new Coord(longitude, latitude);
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#intersects(esri.geom.JShape, esri.geom.JShape)
   */
  public static boolean intersects(JShape shp1, JShape shp2)
  {
    throw new InternalError("Contains algorithm not implemented for shape types!");
  }

  /*
   * (non-Javadoc)
   * 
   * @see esri.geom.CAG#touches(esri.geom.JShape, esri.geom.JShape)
   */
  public static boolean touches(JShape shp, JShape shp2)
  {
    throw new InternalError("Contains algorithm not implemented for shape types!");
  }
}
