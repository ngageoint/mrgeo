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

package org.mrgeo.utils.tms;

import com.vividsolutions.jts.geom.Envelope;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.*;

public class Bounds implements Externalizable
{
public static final Bounds WORLD = new Bounds(-180, -90, 180, 90);

public double w;
public double s;
public double n;
public double e;

private boolean set = false;
private int individualParamSet = 0;


public Bounds()
{
  clear();
}

public Bounds(final double w, final double s, final double e, final double n)
{
  this.n = n;
  this.s = s;
  this.e = e;
  this.w = w;

  set = true;
}

@Override
public Bounds clone()
{
  return new Bounds(w, s, e, n);
}

public Envelope toEnvelope()
{
  return new Envelope(w, e, s, n);
}


public static Bounds combine(Bounds... bounds)
{
  Bounds answer = null;
  for (Bounds b : bounds)
  {
    if (answer == null)
    {
      answer = new Bounds(b.w, b.s, b.e, b.n);
    }
    else
    {
      answer.expand(b);
    }
  }
  return answer;
}

public void clear()
{
  w = Double.MAX_VALUE;
  s = Double.MAX_VALUE;
  e = -Double.MAX_VALUE;
  n = -Double.MAX_VALUE;

  set = false;
}


// Getters/Setters are _ONLY_ for JSON serialization, they should never be used outside there.  So they are private...
@SuppressWarnings("unused")
private double getE()
{
  return e;
}

@SuppressWarnings("unused")
private double getN()
{
  return n;
}

@SuppressWarnings("unused")
private double getW()
{
  return w;
}

@SuppressWarnings("unused")
private double getS()
{
  return s;
}

@SuppressWarnings("unused")
private void setMaxX(double maxX)
{
  individualParamSet |= 1;
  set = set | individualParamSet == 15;

  this.e = maxX;
}

@SuppressWarnings("unused")
private void setMaxY(double maxY)
{
  individualParamSet |= 2;
  set = set | individualParamSet == 15;

  this.n = maxY;
}

@SuppressWarnings("unused")
private void setMinX(double minX)
{
  individualParamSet |= 4;
  set = set | individualParamSet == 15;

  this.w = minX;
}

@SuppressWarnings("unused")
private void setMinY(double minY)
{
  individualParamSet |= 8;
  set = set | individualParamSet == 15;

  this.s = minY;
}

@SuppressWarnings("unused")
private void setE(double e)
{
  individualParamSet |= 1;
  set = set | individualParamSet == 15;

  this.e = e;
}

@SuppressWarnings("unused")
private void setN(double n)
{
  individualParamSet |= 2;
  set = set | individualParamSet == 15;

  this.n = n;
}

@SuppressWarnings("unused")
private void setW(double w)
{
  individualParamSet |= 4;
  set = set | individualParamSet == 15;

  this.w = w;
}

@SuppressWarnings("unused")
private void setS(double s)
{
  individualParamSet |= 8;
  set = set | individualParamSet == 15;

  this.s = s;
}

public boolean contains(final Bounds b)
{
  return contains(b, true);
}

@JsonIgnore
public boolean isValid()
{
  return (set && (w != Double.MAX_VALUE) && (s != Double.MAX_VALUE)
      && (e != -Double.MAX_VALUE) && (n != -Double.MAX_VALUE));
}

/**
 * Is the bounds fully contained within this bounds. Edges are included iff includeAdjacent is
 * true
 */
public boolean contains(final Bounds b, final boolean includeAdjacent)
{
  if (includeAdjacent)
  {
    return (b.w >= w && b.s >= s && b.e <= e && b.n <= n);
  }
  return (b.w > w && b.s > s && b.e < e && b.n < n);
}

public boolean contains(double longitude, double latitude)
{
  return contains(longitude, latitude, true);
}

/**
 * Is the bounds fully contained within this bounds. Edges are included iff includeAdjacent is
 * true
 */
public boolean contains(double longitude, double latitude, final boolean includeAdjacent)
{
  if (includeAdjacent)
  {
    return (longitude >= w && latitude >= s && longitude <= e && latitude <= n);
  }
  return (longitude > w && latitude > s && longitude < e && latitude < n);
}

@Override
public boolean equals(final Object obj)
{
  if (this == obj)
  {
    return true;
  }
  if (obj == null)
  {
    return false;
  }
  if (getClass() != obj.getClass())
  {
    return false;
  }
  final Bounds other = (Bounds) obj;

  return Double.doubleToLongBits(e) == Double.doubleToLongBits(other.e) &&
      Double.doubleToLongBits(n) == Double.doubleToLongBits(other.n) &&
      Double.doubleToLongBits(s) == Double.doubleToLongBits(other.s) &&
      Double.doubleToLongBits(w) == Double.doubleToLongBits(other.w);

}

public void expand(final Bounds b)
{
  if (n < b.n)
  {
    n = b.n;
  }
  if (s > b.s)
  {
    s = b.s;
  }

  if (w > b.w)
  {
    w = b.w;
  }

  if (e < b.e)
  {
    e = b.e;
  }
}

public void expand(final double x, final double y)
{
  if (n < y)
  {
    n = y;
  }
  if (s > y)
  {
    s = y;
  }

  if (w > x)
  {
    w = x;
  }

  if (e < x)
  {
    e = x;
  }

}

public void
expand(final double west, final double south, final double east, final double north)
{
  if (n < north)
  {
    n = north;
  }
  if (s > south)
  {
    s = south;
  }

  if (w > west)
  {
    w = west;
  }

  if (e < east)
  {
    e = east;
  }

}

public void expandBy(final double v)
{
  n += v;
  s -= v;
  w -= v;
  e += v;
}

public void expandBy(final double x, final double y)
{
  n += y;
  s -= y;

  w -= x;
  e += x;
}

public void
expandBy(final double west, final double south, final double east, final double north)
{
  n += north;
  s -= south;
  w -= west;
  e += east;
}

@Override
public int hashCode()
{
  final int prime = 31;
  int result = 1;
  long temp;
  temp = Double.doubleToLongBits(e);
  result = prime * result + (int) (temp ^ (temp >>> 32));
  temp = Double.doubleToLongBits(n);
  result = prime * result + (int) (temp ^ (temp >>> 32));
  temp = Double.doubleToLongBits(s);
  result = prime * result + (int) (temp ^ (temp >>> 32));
  temp = Double.doubleToLongBits(w);
  result = prime * result + (int) (temp ^ (temp >>> 32));
  return result;
}

public boolean intersects(final Bounds b)
{
  return intersects(b, true);
}

public boolean intersects(final double w, final double s, final double e, final double n)
{
  return intersects(new Bounds(w, s, e, n));
}

/**
 * If the two boundaries are adjacent, this would return true iff includeAdjacent is true
 */
public boolean intersects(final Bounds b, final boolean includeAdjacent)
{
  final Bounds
      intersectBounds = new Bounds(Math.max(this.w, b.w), Math.max(this.s, b.s), Math
      .min(this.e, b.e), Math.min(this.n, b.n));
  if (includeAdjacent)
  {
    return (intersectBounds.w <= intersectBounds.e && intersectBounds.s <= intersectBounds.n);
  }
  return (intersectBounds.w < intersectBounds.e && intersectBounds.s < intersectBounds.n);
}

@Override
public String toString()
{
  return "Bounds [w=" + w + ", s=" + s + ", e=" + e + ", n=" + n + "]";
}

public String toCommaString()
{
  return w + "," + s + "," + e + "," + n;
}

public static Bounds fromCommaString(String str)
{
  String[] split = str.split(",");

  double w = Double.parseDouble(split[0]);
  double s = Double.parseDouble(split[1]);
  double e = Double.parseDouble(split[2]);
  double n = Double.parseDouble(split[3]);

  return new Bounds(w, s, e, n);
}

public Bounds union(final Bounds b)
{
  return new Bounds(Math.min(this.w, b.w), Math.min(this.s, b.s), Math.max(
      this.e, b.e), Math.max(this.n, b.n));
}

public Bounds intersection(final Bounds b)
{
  return intersection(b, true);
}

/**
 * If the two boundaries are adjacent, this would return true iff includeAdjacent is true
 */
public Bounds intersection(final Bounds b, final boolean includeAdjacent)
{

  final Bounds
      intersectBounds = new Bounds(Math.max(this.w, b.w), Math.max(this.s, b.s), Math
      .min(this.e, b.e), Math.min(this.n, b.n));

  if (includeAdjacent)
  {
    if (intersectBounds.w <= intersectBounds.e && intersectBounds.s <= intersectBounds.n)
    {
      return intersectBounds;
    }
  }
  else if (intersectBounds.w < intersectBounds.e && intersectBounds.s < intersectBounds.n)
  {
    return intersectBounds;
  }

  return null;
}

public double width()
{
  return e - w;
}

public double height()
{
  return n - s;
}


@Override
public void writeExternal(ObjectOutput out) throws IOException
{
  out.writeBoolean(set);
  if (set)
  {
    out.writeDouble(w);
    out.writeDouble(s);
    out.writeDouble(e);
    out.writeDouble(n);
  }
}

@Override
public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
{
  set = in.readBoolean();
  if (set)
  {
    w = in.readDouble();
    s = in.readDouble();
    e = in.readDouble();
    n = in.readDouble();
  }
}

}
