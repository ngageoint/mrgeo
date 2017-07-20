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
  String geometryStr = wktGeometry;
  String regex = "\\(|\\[|\\]|\\)";
  String[] geometryArray = geometryStr.split(regex);
  StringBuilder fixedGeometry = new StringBuilder();
  String geometryType = "";
  if (geometryArray.length > 1)
  {
    geometryType = geometryArray[0];
    if (geometryType.equalsIgnoreCase("EsriEnvelope"))
    {
      String[] points = geometryArray[1].split(",");
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
      int parenthesesCount = 0;
      for (int i = 1; i < geometryArray.length; i++)
      {
        String tmpGeom = geometryArray[i];
        if (!tmpGeom.isEmpty() && !tmpGeom.equals(","))
        {
          if (fixedGeometry.length() > 0)
          {
            fixedGeometry.append(",");
          }
          String tmpFixed = parsePoints(geometryArray[i], geometryType);
          for (int j = 0; j < parenthesesCount; j++)
          {
            tmpFixed = "(" + tmpFixed + ")";
          }
          fixedGeometry.append(tmpFixed);
        }

        //only count one time. e.g MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))
        //will generate array "MULTIPOLYGON, , , 0 0,10 0,10 10,0 10,0 0,...", there are two empty string
        //in the array that indicate two parentheses we need to add back after parsing.
        if (tmpGeom.isEmpty() && fixedGeometry.length() == 0)
        {
          parenthesesCount++;
        }
      }
    }
  }
  if (!geometryType.isEmpty() && fixedGeometry.length() != 0)
  {
    return geometryType + "(" + fixedGeometry + ")";
  }
  return geometryStr;
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
  StringBuilder geometry = new StringBuilder(geometryStr);
  String regex = "\\,|\\  |\\\t";
  String[] pointsArray = geometryStr.split(regex);

  if (geometryStr.contains("  ") || geometryStr.contains("\t"))
  {
    geometry = new StringBuilder();
    for (String pa : pointsArray)
    {
      if (geometry.length() > 0)
      {
        geometry.append(",");
      }
      geometry.append(pa);
    }
  }

  if (geometryType.toLowerCase().contains("polygon"))
  {
    if (!pointsArray[0].trim().equalsIgnoreCase(pointsArray[pointsArray.length - 1].trim()))
    {
      geometry.append(",").append(pointsArray[0]); //close the polygon
    }
  }
  return geometry.toString();
}
}
