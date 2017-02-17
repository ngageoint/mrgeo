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

package org.mrgeo.utils.tms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.vividsolutions.jts.geom.Envelope;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.Map;

public class Bounds implements Serializable, Cloneable
{
public static final Bounds WORLD = new Bounds(-180, -90, 180, 90);
private static final long serialVersionUID = 1L;
final public double w;
final public double s;
final public double n;
final public double e;

public Bounds(final double w, final double s, final double e, final double n)
{
  this.n = n;
  this.s = s;
  this.e = e;
  this.w = w;

  //set = true;
}

@JsonCreator
public Bounds(Map<String, Object> delegate)
{
  double n = Double.NaN;
  double s = Double.NaN;
  double e = Double.NaN;
  double w = Double.NaN;

  if (delegate.containsKey("w"))
  {
    w = Double.parseDouble(delegate.get("w").toString());
  }
  else if (delegate.containsKey("minX"))
  {
    w = Double.parseDouble(delegate.get("minX").toString());
  }

  if (delegate.containsKey("s"))
  {
    s = Double.parseDouble(delegate.get("s").toString());
  }
  else if (delegate.containsKey("minY"))
  {
    s = Double.parseDouble(delegate.get("minY").toString());
  }

  if (delegate.containsKey("e"))
  {
    e = Double.parseDouble(delegate.get("e").toString());
  }
  else if (delegate.containsKey("maxX"))
  {
    e = Double.parseDouble(delegate.get("maxX").toString());
  }

  if (delegate.containsKey("n"))
  {
    n = Double.parseDouble(delegate.get("n").toString());
  }
  else if (delegate.containsKey("maxY"))
  {
    n = Double.parseDouble(delegate.get("maxY").toString());
  }

  this.n = n;
  this.s = s;
  this.e = e;
  this.w = w;
}

@JsonIgnore
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
      answer = answer.expand(b);
    }
  }
  return answer;
}

@JsonIgnore
public static Bounds fromCommaString(String str)
{
  String[] split = str.split(",");

  double w = Double.parseDouble(split[0]);
  double s = Double.parseDouble(split[1]);
  double e = Double.parseDouble(split[2]);
  double n = Double.parseDouble(split[3]);

  return new Bounds(w, s, e, n);
}

@Override
@SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "No super.clone() to call")
public Bounds clone()
{
  return new Bounds(w, s, e, n);
}

public Envelope toEnvelope()
{
  return new Envelope(w, e, s, n);
}

public boolean contains(final Bounds b)
{
  return contains(b, true);
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

public Bounds expand(final Bounds b)
{
  if (b != null)
  {
    return expand(b.w, b.s, b.e, b.n);
  }
  else
  {
    return this;
  }
}

public Bounds expand(final double x, final double y)
{
  return expand(x, y, x, y);
}

public Bounds
expand(final double west, final double south, final double east, final double north)
{
  double nn = n;
  double ns = s;
  double ne = e;
  double nw = w;

  if (nn < north)
  {
    nn = north;
  }
  if (ns > south)
  {
    ns = south;
  }

  if (nw > west)
  {
    nw = west;
  }

  if (ne < east)
  {
    ne = east;
  }

  return new Bounds(nw, ns, ne, nn);

}

public Bounds expandBy(final double v)
{
  return expandBy(v, v, v, v);
}

public Bounds expandBy(final double x, final double y)
{
  return expandBy(x, y, x, y);
}

public Bounds
expandBy(final double west, final double south, final double east, final double north)
{
  return new Bounds(w - west, s - south, e + east, n + north);
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

public LatLon center()
{
  return new LatLon(s + ((n - s) / 2), w + ((e - w) / 2));
}
}
