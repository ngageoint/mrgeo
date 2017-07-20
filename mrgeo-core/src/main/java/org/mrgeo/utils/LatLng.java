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

package org.mrgeo.utils;

/**
 * @author jason.surratt
 */
public class LatLng
{
/**
 * This is the mean earth radius vs. the radius at the equator
 */
public final static double EARTH_RADIUS_MEAN = 6371009;
/**
 * This is the earth radius at the equator
 */
public final static double EARTH_RADIUS_EQUATOR = 6378137;
/**
 * While it is technically probably less correct, WMS uses the radius at the equator for its
 * calculations.
 */
public final static double EARTH_RADIUS = EARTH_RADIUS_EQUATOR;
public final static double METERS_PER_DEGREE = (EARTH_RADIUS * 2.0 * Math.PI) / 360.0;
final static double EPSILON = 1.0 / EARTH_RADIUS_EQUATOR;
double lat;
double lng;

public LatLng()
{
  lat = Double.NaN;
  lng = Double.NaN;
}

public LatLng(double lat, double lng)
{
  this.lat = lat;
  this.lng = lng;
}

/**
 * Calculate the great circle distance in meters. See
 * http://en.wikipedia.org/wiki/Great_circle_distance for an explanation.
 */
public static double calculateGreatCircleDistance(LatLng p1, LatLng p2)
{

  if (FloatUtils.isEqual(p1.getLat(), p2.getLat()) &&
      FloatUtils.isEqual(p1.getLng(), p2.getLng()))
  {
    return 0.0;
  }
  double dsigma = Math.acos(Math.cos(p1.getLatAsRadians()) *
      Math.cos(p2.getLatAsRadians()) * Math.cos(p1.getLngAsRadians() - p2.getLngAsRadians()) +
      Math.sin(p1.getLatAsRadians()) * Math.sin(p2.getLatAsRadians()));
  return dsigma * EARTH_RADIUS;
}

public static LatLng calculateCartesianDestinationPoint(LatLng startPoint,
    double distanceInMeters,
    double bearing)
{
  double distanceInDegrees = distanceInMeters / METERS_PER_DEGREE;
  double polarAngle = (450.0 - bearing) % 360.0;
  double polarAngleInRad = Math.toRadians(polarAngle);
  double lat = startPoint.getLat() + distanceInDegrees * Math.sin(polarAngleInRad);
  double lon = startPoint.getLng() + distanceInDegrees * Math.cos(polarAngleInRad);
  // Limit to world bounds
  lat = Math.max(-90.0, Math.min(90.0, lat));
  lon = Math.max(-180.0, Math.min(180.0, lon));
  return new LatLng(lat, lon);
}

public double getLat()
{
  return lat;
}

public void setLat(double lat)
{
  this.lat = lat;
}

public double getLng()
{
  return lng;
}

public void setLng(double lng)
{
  this.lng = lng;
}

@Override
public String toString()
{
  return String.format("x: %f, y: %f", getX(), getY());
}

double getLatAsRadians()
{
  return lat / 180.0 * Math.PI;
}

double getLngAsRadians()
{
  return lng / 180.0 * Math.PI;
}

double getX()
{
  return lng;
}

public void setX(double x)
{
  lng = x;
}

double getY()
{
  return lat;
}

public void setY(double y)
{
  lat = y;
}

}
