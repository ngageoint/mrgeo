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

package org.mrgeo.data.geojson;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.LineString;
import org.mrgeo.geometry.Point;
import org.mrgeo.geometry.Polygon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoJSONConverter
{

  private static String getGeometryType(Geometry geometry) {
    String geometryType = "";
    // determine the type
    if (geometry instanceof Point) {
      geometryType = "Point";
    }
    else if (geometry instanceof LineString) {
      geometryType = "LineString";
    }
    else if (geometry instanceof Polygon) {
      geometryType = "Polygon";
    }
    /* else if (nextGeometry instanceof GeometryCollection) {
      geometryType = "GeometryCollection";
    } */
    return geometryType;
  }
  
  private static List<Object> getCoordinates(Geometry geometry) {
    List<Object> coordList = new ArrayList<Object>();

    if (geometry instanceof Point)
    {
      Point p = (Point) geometry;
      coordList.add(p.getX());
      coordList.add(p.getY());
    }
    if (geometry instanceof LineString)
    {
      LineString ls = (LineString) geometry;
      int numPoints = ls.getNumPoints();
      for (int i=0; i<numPoints; i++) {
        Point p = ls.getPoint(i);
        List<Double> xy = new ArrayList<Double>();
        xy.add(p.getX());
        xy.add(p.getY());
        coordList.add(xy);
      }
    }
    if (geometry instanceof Polygon)
    {
      Polygon poly = (Polygon) geometry;
      if (poly.isValid()) {
        // Add the exterior ring first
        List<Double> exteriorXYList = new ArrayList<Double>();
        int numPoints = poly.getExteriorRing().getNumPoints();
        for (int i=0; i<numPoints; i++) {
          Point p = poly.getExteriorRing().getPoint(i);
          exteriorXYList.add(p.getX());
          exteriorXYList.add(p.getY());            
        }
        coordList.add(exteriorXYList);
        // Now add the interior rings
        for (int j = 0; j < poly.getNumInteriorRings(); j++)
        {
          List<Double> interiorXYList = new ArrayList<Double>();
          int npts = poly.getInteriorRing(j).getNumPoints();
          for (int k=0; k<npts; k++) {
            Point p = poly.getInteriorRing(j).getPoint(k);
            interiorXYList.add(p.getX());
            interiorXYList.add(p.getY());             
          }
          coordList.add(interiorXYList);
        }
      }
    }
    /* if (nextGeometry instanceof GeometryCollection)
    {
      return toGeoJSON((GeometryCollection) g);
    } */
    return coordList;
  }
  
  public static Map<String, Object> toGeoJSON(Geometry geometry) {
    Map<String, Object> nextFeatureMap = new HashMap<String, Object>(); 
    nextFeatureMap.put("type", "Feature");
    
    // form the geometry
    Map<String, Object> geometryMap = new HashMap<String, Object>();
    nextFeatureMap.put("geometry", geometryMap);
    geometryMap.put("type", GeoJSONConverter.getGeometryType(geometry));
    
    // determine the coordinates
    geometryMap.put("coordinates", GeoJSONConverter.getCoordinates(geometry));
    
    return nextFeatureMap;
  }
}
