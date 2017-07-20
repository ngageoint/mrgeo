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


// Taken from: http://www.tom-carden.co.uk/p5/geohashing/applet/Geohash.java
// and tweaked a little for clarity.  Added methods to encode/decode to long, and decode to Bounds

//Geohash.java
//Geohash library for Java
//ported from David Troy's Geohash library for Javascript
//- http://github.com/davetroy/geohash-js/tree/master
//(c) 2008 David Troy
//(c) 2008 Tom Carden
//Distributed under the MIT License

import org.mrgeo.utils.tms.Bounds;

public class GeoHash
{
private static int BITS[] = {16, 8, 4, 2, 1};

private static String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

private static int RIGHT = 0;
private static int LEFT = 1;
private static int TOP = 2;
private static int BOTTOM = 3;

private static int EVEN = 0;
private static int ODD = 1;

private static String[][] NEIGHBORS;
private static String[][] BORDERS;

private static int precision = 28; // ~0.15m (6.6cm) accuracy

static
{
  NEIGHBORS = new String[4][2];
  BORDERS = new String[4][2];

  NEIGHBORS[BOTTOM][EVEN] = "bc01fg45238967deuvhjyznpkmstqrwx";
  NEIGHBORS[TOP][EVEN] = "238967debc01fg45kmstqrwxuvhjyznp";
  NEIGHBORS[LEFT][EVEN] = "p0r21436x8zb9dcf5h7kjnmqesgutwvy";
  NEIGHBORS[RIGHT][EVEN] = "14365h7k9dcfesgujnmqp0r2twvyx8zb";

  BORDERS[BOTTOM][EVEN] = "bcfguvyz";
  BORDERS[TOP][EVEN] = "0145hjnp";
  BORDERS[LEFT][EVEN] = "prxz";
  BORDERS[RIGHT][EVEN] = "028b";

  NEIGHBORS[BOTTOM][ODD] = NEIGHBORS[LEFT][EVEN];
  NEIGHBORS[TOP][ODD] = NEIGHBORS[RIGHT][EVEN];
  NEIGHBORS[LEFT][ODD] = NEIGHBORS[BOTTOM][EVEN];
  NEIGHBORS[RIGHT][ODD] = NEIGHBORS[TOP][EVEN];

  BORDERS[BOTTOM][ODD] = BORDERS[LEFT][EVEN];
  BORDERS[TOP][ODD] = BORDERS[RIGHT][EVEN];
  BORDERS[LEFT][ODD] = BORDERS[BOTTOM][EVEN];
  BORDERS[RIGHT][ODD] = BORDERS[TOP][EVEN];
}

public static String calculateAdjacent(String srcHash, int dir)
{
  srcHash = srcHash.toLowerCase();
  char lastChr = srcHash.charAt(srcHash.length() - 1);
  int type = (srcHash.length() % 2) != 0 ? ODD : EVEN;
  String base = srcHash.substring(0, srcHash.length() - 1);
  if (BORDERS[dir][type].indexOf(lastChr) != -1)
  {
    base = calculateAdjacent(base, dir);
  }
  return base + BASE32.charAt(NEIGHBORS[dir][type].indexOf(lastChr));
}

public static LatLng decode(long hash)
{
  double[] ll = decodeBitsToArray(hash);
  return new LatLng(ll[0], ll[1]);
}

public static Bounds decode(String geohash)
{
  double[][] bounds = decodeToArray(geohash);
  return new Bounds(bounds[1][0], bounds[0][0], bounds[1][1], bounds[0][1]);
}

public static double[] decodeBitsToArray(long hash)
{
  double minLat = -90.0;
  double maxLat = 90.0;
  double minLon = -180.0;
  double maxLon = 180.0;
  //final int i = 0;

  double lat = 0;
  double lon = 0;

  // calculate the precision
  long bits = 1L << ((precision * 2) - 1);

  while (true)
  {
    double midLat = minLat + ((maxLat - minLat) / 2);
    double midLon = minLon + ((maxLon - minLon) / 2);

    if ((hash & bits) != 0)
    {
      minLat = midLat;
    }
    else
    {
      maxLat = midLat;
    }

    bits >>>= 1;

    if ((hash & bits) != 0)
    {
      minLon = midLon;
    }
    else
    {
      maxLon = midLon;
    }

    if (bits > 0)
    {
      bits >>>= 1;
    }
    else
    {
      break;
    }
    lat = midLat;
    lon = midLon;
  }

  // stable rounding - see testBijection
  return new double[]{lat, lon};
}

public static double[][] decodeToArray(String geohash)
{
  boolean is_even = true;
  double[] lat = new double[3];
  double[] lon = new double[3];

  lat[0] = -90.0;
  lat[1] = 90.0;
  lon[0] = -180.0;
  lon[1] = 180.0;
  // double lat_err = 90.0;
  // double lon_err = 180.0;

  for (int i = 0; i < geohash.length(); i++)
  {
    char c = geohash.charAt(i);
    int cd = BASE32.indexOf(c);
    for (int mask : BITS)
    {
      if (is_even)
      {
        // lon_err /= 2.0;
        refine_interval(lon, cd, mask);
      }
      else
      {
        // lat_err /= 2.0;
        refine_interval(lat, cd, mask);
      }
      is_even = !is_even;
    }
  }
  lat[2] = (lat[0] + lat[1]) / 2.0;
  lon[2] = (lon[0] + lon[1]) / 2.0;

  return new double[][]{lat, lon};
}

public static String encode(double latitude, double longitude)
{
  boolean is_even = true;
  double lat[] = new double[2];
  double lon[] = new double[2];
  int bit = 0;
  int ch = 0;
  StringBuilder geohash = new StringBuilder();

  lat[0] = -90.0;
  lat[1] = 90.0;
  lon[0] = -180.0;
  lon[1] = 180.0;

  while (geohash.length() < precision)
  {
    if (is_even)
    {
      double mid = (lon[0] + lon[1]) / 2.0;
      if (longitude > mid)
      {
        ch |= BITS[bit];
        lon[0] = mid;
      }
      else
      {
        lon[1] = mid;
      }
    }
    else
    {
      double mid = (lat[0] + lat[1]) / 2.0;
      if (latitude > mid)
      {
        ch |= BITS[bit];
        lat[0] = mid;
      }
      else
      {
        lat[1] = mid;
      }
    }
    is_even = !is_even;
    if (bit < 4)
    {
      bit++;
    }
    else
    {
      geohash.append(BASE32.charAt(ch));
      bit = 0;
      ch = 0;
    }
  }
  return geohash.toString();
}

// This algorithm is from:
// http://karussell.wordpress.com/2012/05/23/spatial-keys-memory-efficient-geohashes/
public static long encodeBits(double latitude, double longitude)
{
  long hash = 0;
  double minLat = -90.0;
  double maxLat = 90.0;
  double minLon = -180.0;
  double maxLon = 180.0;
  int i = 0;
  while (true)
  {
    double midLat = minLat + ((maxLat - minLat) / 2);
    double midLon = minLon + ((maxLon - minLon) / 2);

    if (latitude > midLat)
    {
      hash |= 1;
      minLat = midLat;
    }
    else
    {
      maxLat = midLat;
    }
    hash <<= 1;
    if (longitude > midLon)
    {
      hash |= 1;
      minLon = midLon;
    }
    else
    {
      maxLon = midLon;
    }

    if (++i < precision)
    {
      hash <<= 1;
    }
    else
    {
      break;
    }
  }
  return hash;
}

public static int getPrecision()
{
  return precision;
}

public static void setPrecision(int p)
{
  precision = p;
}

private static void refine_interval(double[] interval, int cd, int mask)
{
  if ((cd & mask) > 0)
  {
    interval[0] = (interval[0] + interval[1]) / 2.0;
  }
  else
  {
    interval[1] = (interval[0] + interval[1]) / 2.0;
  }
}

}
