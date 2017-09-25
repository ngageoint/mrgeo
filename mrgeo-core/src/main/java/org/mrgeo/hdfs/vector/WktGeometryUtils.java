/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.vector;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

public class WktGeometryUtils
{
private static WKTReader wktReader = new WKTReader();

static public String wktGeometryFixer(String wktGeometry)
{
  StringBuilder fixedGeometry = new StringBuilder();

  String regex = "[\\(\\[]";
  String[] ga = wktGeometry.split(regex, 2);

  if (ga.length > 1)
  {
    String geometryType =  ga[0].trim().toLowerCase();

    if (geometryType.equals("esrienvelope"))
    {
      String[] points = ga[1].split(",");
      if (points.length == 4)
      {
        fixedGeometry.append("POLYGON((");
        fixedGeometry.append(points[0]).append(" ").append(points[1]).append(",");
        fixedGeometry.append(points[2]).append(" ").append(points[1]).append(",");
        fixedGeometry.append(points[2]).append(" ").append(points[3]).append(",");
        fixedGeometry.append(points[0]).append(" ").append(points[3]).append(",");
        fixedGeometry.append(points[0]).append(" ").append(points[1]);
        fixedGeometry.append("))");
        return fixedGeometry.toString();
      }
    }
    else
    {
      fixedGeometry.append(geometryType.toUpperCase());
      fixedGeometry.append('(');

      StringBuilder subgeo = new StringBuilder();
      for (char ch: ga[1].toCharArray())
      {
        if (ch == '(' || ch == '[')
        {
          fixedGeometry.append('(');
        }
        else if (ch == ')' || ch == ']')
        {
          if (subgeo.length() > 0)
          {
            fixedGeometry.append(parsePoints(subgeo.toString(), geometryType));
          }
          fixedGeometry.append(')');
          subgeo = new StringBuilder();
        }
        else if (ch == ',')
        {
          if (subgeo.length() == 0)
          {
            fixedGeometry.append(ch);
          }
          else
          {
            subgeo.append(ch);
          }
        }
        else
        {
          subgeo.append(ch);
        }
      }
      return fixedGeometry.toString();
    }
  }
  return wktGeometry;
}


@SuppressWarnings("squid:S1166") // Exception caught and handled
static public boolean isValidWktGeometry(String wktGeometry)
{
  String fixedWktGeometry = wktGeometryFixer(wktGeometry);
  if (fixedWktGeometry.toUpperCase().contains("POINT") || fixedWktGeometry.toUpperCase().contains("LINESTRING")
      || fixedWktGeometry.toUpperCase().contains("LINEARRING") || fixedWktGeometry.toUpperCase().contains("POLYGON")
      || fixedWktGeometry.toUpperCase().contains("MULTIPOINT") ||
      fixedWktGeometry.toUpperCase().contains("MULTILINESTRING")
      || fixedWktGeometry.toUpperCase().contains("MULTIPOLYGON") ||
      fixedWktGeometry.toUpperCase().contains("GEOMETRYCOLLECTION"))
  {
    try
    {
      Geometry geom = wktReader.read(fixedWktGeometry);
      if (geom != null)
      {
        return true;
      }
    }
    catch (Exception ignored)
    {
      return false;
    }
  }
  return false;
}


static private String parsePoints(String geometryStr, String geometryType)
{
  StringBuilder geometry = new StringBuilder();

  String regex = "\\,|\\  |\\\t";
  String[] pointsArray = geometryStr.split(regex);

  for (String pa : pointsArray)
  {
    if (geometry.length() > 0)
    {
      geometry.append(",");
    }
    geometry.append(pa);
  }

  int len = pointsArray.length;

  if (geometryType.contains("polygon"))
  {
    if (!pointsArray[0].trim().equalsIgnoreCase(pointsArray[pointsArray.length - 1].trim()))
    {
      geometry.append(",").append(pointsArray[0]); //close the polygon
      len++;
    }

    for (int i = len; i < 4; i++)
    {
      geometry.append(",").append(pointsArray[0]); //pad the polygon to 4 points
    }
  }
  return geometry.toString();
}
}
