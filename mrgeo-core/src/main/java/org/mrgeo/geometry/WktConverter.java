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

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;


public class WktConverter
{

public static String toWkt(Geometry g)
{
  if (g instanceof Point)
  {
    return toWkt((Point) g);
  }
  if (g instanceof LineString)
  {
    return toWkt((LineString) g);
  }
  if (g instanceof Polygon)
  {
    return toWkt((Polygon) g);
  }
  if (g instanceof GeometryCollection)
  {
    return toWkt((GeometryCollection) g);
  }
  throw new IllegalArgumentException("Geometry is not of a recognized type.");
}

public static String toWkt(GeometryCollection gc)
{
  StringBuffer result;
  if (gc.isValid() == false)
  {
    result = new StringBuffer("GEOMETRYCOLLECTION EMPTY");
  }
  else
  {
    // 1st figure out if all the collection is the same type
    Geometry type = null;
    for (Geometry g : gc.getGeometries())
    {
      if (type == null)
      {
        type = g;
      }
      else if (!type.getClass().isInstance(g))
      {
        type = null;
        break;
      }
    }

    if (type == null)
    {
      result = new StringBuffer("GEOMETRYCOLLECTION EMPTY");
    }
    else
    {
      String strip = null;
      if (GeometryCollection.class.isInstance(type))
      {
        result = new StringBuffer("GEOMETRYCOLLECTION(");
      }
      else if (Point.class.isInstance(type))
      {
        result = new StringBuffer("MULTIPOINT(");
        strip = "POINT";
      }
      else if (LineString.class.isInstance(type))
      {
        result = new StringBuffer("MULTILINESTRING(");
        strip = "LINESTRING";
      }
      else if (Polygon.class.isInstance(type))
      {
        result = new StringBuffer("MULTIPOLYGON(");
        strip = "POLYGON";
      }
      else
      {
        result = new StringBuffer("GEOMETRYCOLLECTION EMPTY");
      }

      String sep = "";
      for (Geometry g : gc.getGeometries())
      {
        result.append(sep);

        // strip the name if it a multi... type
        result.append(StringUtils.removeStartIgnoreCase(toWkt(g), strip));

        sep = ",";
      }
      result.append(")");
    }
  }

  return result.toString();
}

public static String toWkt(LineString ls)
{
  StringBuffer result;
  if (ls.isValid() == false)
  {
    result = new StringBuffer("LINESTRING EMPTY");
  }
  else
  {
    result = new StringBuffer("LINESTRING");
    result.append(createPointString(ls.getPoints()));
  }

  return result.toString();
}

public static String toWkt(Point p)
{
  return String.format("POINT(%.9f %.9f)", p.getX(), p.getY());
}

public static String toWkt(Polygon p)
{
  StringBuffer result;
  if (p.isValid() == false)
  {
    result = new StringBuffer("POLYGON EMPTY");
  }
  else
  {
    result = new StringBuffer("POLYGON(");
    result.append(createPointString(p.getExteriorRing().cw().getPoints()));
    for (int i = 0; i < p.getNumInteriorRings(); i++)
    {
      result.append(",");
      result.append(createPointString(p.getInteriorRing(i).ccw().getPoints()));
    }
    result.append(")");
  }

  return result.toString();
}

private static StringBuffer createPointString(Collection<Point> collection)
{
  StringBuffer result = new StringBuffer("(");
  String divider = "";
  for (Point p : collection)
  {
    result.append(divider);
    divider = ",";
    result.append(String.format("%.9f %.9f", p.getX(), p.getY()));
  }
  result.append(")");
  return result;
}
}
