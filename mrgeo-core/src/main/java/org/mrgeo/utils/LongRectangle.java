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

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.NotImplementedException;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;

//NOTE: This class is json serialized, so there are @JsonIgnore annotations on the
//getters/setters that should not be automatically serialized

@SuppressWarnings("static-method")
public class LongRectangle implements Serializable, Cloneable
{
private static final long serialVersionUID = 1L;

private long minX, minY;
private long maxX, maxY;
private boolean set;

public LongRectangle()
{
  set = false;
}

public LongRectangle(long minX, long minY, long maxX, long maxY)
{
  this.minX = minX;
  this.minY = minY;

  this.maxX = maxX;
  this.maxY = maxY;

  set = true;
}

public LongRectangle(LongRectangle copy)
{
  minX = copy.minX;
  minY = copy.minY;

  maxX = copy.maxX;
  maxY = copy.maxY;

  set = true;
}

@SuppressWarnings("unused")
static public void intersect(LongRectangle src1, LongRectangle src2,
    LongRectangle dest)
{
  throw new NotImplementedException("intersects() not implemented");
}

public static LongRectangle fromDelimitedString(String rect)
{
  String[] args = rect.split(",");
  if (args.length != 4)
  {
    throw new IllegalArgumentException(
        "Delimited LongRectangle should be in the format of \"minx,miny,maxx,maxy\" (delimited by \",\") ");
  }

  return new LongRectangle(Long.parseLong(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]),
      Long.parseLong(args[3]));
}

@SuppressWarnings("unused")
static void union(LongRectangle src1, LongRectangle src2, LongRectangle dest)
{
  throw new NotImplementedException("union() not implemented");

}

public void add(long x, long y)
{
  if (!set)
  {
    minX = maxX = x;
    minY = maxY = y;

    set = true;
  }
  else
  {
    if (x < minX)
    {
      minX = x;
    }
    if (x > maxX)
    {
      maxX = x;
    }

    if (y < minY)
    {
      minY = y;
    }
    if (y > maxY)
    {
      maxY = y;
    }
  }

}

public void add(LongRectangle r)
{
  add(r.minX, r.minY);
  add(r.maxX, r.maxY);
}

@Override
@SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "No super.clone() to call")
public Object clone()
{
  return new LongRectangle(minX, minY, maxX, maxY);
}

public boolean contains(long x, long y)
{
  return outcode(x, y) == 0;
}

public boolean contains(LongRectangle r)
{
  return outcode(r.minX, r.minY) == 0 && outcode(r.maxX, r.maxY) == 0;
}

@SuppressWarnings("unused")
public LongRectangle createIntersection(LongRectangle r)
{
  throw new NotImplementedException("createIntersection() not implemented");
}

@SuppressWarnings("unused")
public LongRectangle createUnion(LongRectangle r)
{
  throw new NotImplementedException("createUnion() not implemented");
}

@Override
public boolean equals(Object obj)
{
  if (obj instanceof LongRectangle)
  {
    LongRectangle r = (LongRectangle) obj;

    return (r.minX == minX && r.maxX == maxX && r.minY == minY && r.maxY == maxY);
  }

  return false;
}

@JsonIgnore
public LongRectangle getBounds()
{
  return (LongRectangle) clone();
}

@JsonIgnore
public long getCenterX()
{
  return minX + (maxX - minX) / 2;
}

@JsonIgnore
public long getCenterY()
{
  return minY + (maxY - minY) / 2;
}

@JsonIgnore
public long getHeight()
{
  //add 1 as maxY is inclusive
  return maxY - minY + 1;
}

public long getMaxX()
{
  return maxX;
}

public void setMaxX(long x)
{
  maxX = x;
}

public long getMaxY()
{
  return maxY;
}

public void setMaxY(long y)
{
  maxY = y;
}

public long getMinX()
{
  return minX;
}

public void setMinX(long x)
{
  minX = x;
}

public long getMinY()
{
  return minY;
}

public void setMinY(long y)
{
  minY = y;
}

@JsonIgnore
public long getWidth()
{
  //add 1 as maxX is inclusive
  return maxX - minX + 1;
}

public void grow(long h, long v)
{
  minX -= h;
  maxX += h;

  minY -= v;
  maxY += v;
}

@SuppressWarnings("unused")
public boolean intersects(long srcMinX, long srcMinY, long srcMaxX, long srcMaxY)
{
  throw new NotImplementedException("intersects() not implemented");
}

@SuppressWarnings("unused")
public boolean intersects(LongRectangle r)
{
  throw new NotImplementedException("intersects() not implemented");
}

@SuppressWarnings("unused")
public boolean intersectsLine(long x1, long y1, long x2, long y2)
{
  throw new NotImplementedException("intersectsLine() not implemented");
}

@JsonIgnore
public boolean isEmpty()
{
  return set;
}

public void setRect(long minX, long minY, long maxX, long maxY)
{
  this.minX = minX;
  this.minY = minY;

  this.maxX = maxX;
  this.maxY = maxY;

  set = true;
}

@Override
public int hashCode()
{
  return super.hashCode();
}

public String toDelimitedString()
{
  return String.format("%s,%s,%s,%s", minX, minY, maxX, maxY);
}

@Override
public String toString()
{
  return "Rectangle: (" + minX + ", " + minY + ") (" + maxX + ", " + maxY + ")";
}

private int outcode(long x, long y)
{
  int outcode = 0;
  if (x < minX)
  {
    outcode |= Rectangle2D.OUT_LEFT;
  }
  else if (x > maxX)
  {
    outcode |= Rectangle2D.OUT_RIGHT;
  }

  if (y < minY)
  {
    outcode |= Rectangle2D.OUT_TOP;
  }
  else if (y > maxY)
  {
    outcode |= Rectangle2D.OUT_BOTTOM;
  }

  return outcode;
}
}
