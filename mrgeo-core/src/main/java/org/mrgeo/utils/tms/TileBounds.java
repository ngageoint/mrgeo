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

import org.mrgeo.utils.LongRectangle;

import java.io.Serializable;

public class TileBounds implements Serializable
{
private static final long serialVersionUID = 1L;

public long n;
public long s;
public long e;
public long w;

public TileBounds(long tx, long ty)
{
  n = ty;
  s = ty;
  e = tx;
  w = tx;
}

public TileBounds(long w, long s, long e, long n)
{
  this.n = n;
  this.s = s;
  this.e = e;
  this.w = w;
}

public TileBounds(Tile t)
{
  n = t.ty;
  s = t.ty;
  e = t.tx;
  w = t.tx;
}

public static TileBounds convertFromLongRectangle(LongRectangle rectangle)
{
  return new TileBounds(rectangle.getMinX(), rectangle.getMinY(), rectangle.getMaxX(),
      rectangle.getMaxY());
}

public static LongRectangle convertToLongRectangle(TileBounds bounds)
{
  return new LongRectangle(bounds.w, bounds.s, bounds.e, bounds.n);
}

public static TileBounds fromCommaString(String str)
{
  String[] split = str.split(",");

  long w = Long.parseLong(split[0]);
  long s = Long.parseLong(split[1]);
  long e = Long.parseLong(split[2]);
  long n = Long.parseLong(split[3]);

  return new TileBounds(w, s, e, n);
}

public long width()
{
  return e - w + 1;
}

public long height()
{
  return n - s + 1;
}

public LongRectangle toLongRectangle()
{
  return new LongRectangle(w, s, e, n);
}

public void expand(long tx, long ty)
{
  if (n < ty)
  {
    n = ty;
  }
  if (s > ty)
  {
    s = ty;
  }

  if (w > tx)
  {
    w = tx;
  }

  if (e < tx)
  {
    e = tx;
  }

}

public void expand(long west, long south, long east, long north)
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

public void expand(Tile t)
{
  if (n < t.ty)
  {
    n = t.ty;
  }
  if (s > t.ty)
  {
    s = t.ty;
  }

  if (w > t.tx)
  {
    w = t.tx;
  }

  if (e < t.tx)
  {
    e = t.tx;
  }

}

public void expand(TileBounds b)
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

@Override
public String toString()
{
  return "TileBounds [w=" + w + ", s=" + s + ", e=" + e + ", n=" + n + "]";
}

public String toCommaString()
{
  return w + "," + s + "," + e + "," + n;
}

public boolean contains(long tx, long ty)
{
  return contains(new Tile(tx, ty), true);
}

public boolean contains(long tx, long ty, boolean includeAdjacent)
{
  return contains(new Tile(tx, ty), includeAdjacent);
}

public boolean contains(Tile tile)
{
  return contains(tile, true);
}

public boolean contains(Tile tile, boolean includeAdjacent)
{
  if (includeAdjacent)
  {
    return (tile.tx >= w && tile.ty >= s && tile.tx <= e && tile.ty <= n);
  }
  else
  {
    return (tile.tx > w && tile.ty > s && tile.tx < e && tile.ty < n);
  }
}

public TileBounds intersection(TileBounds b)
{
  return intersection(b, true);
}

/**
 * If the two boundaries are adjacent, this would return true iff includeAdjacent is true
 */
public TileBounds intersection(TileBounds b, boolean includeAdjacent)
{

  TileBounds
      intersectBounds = new TileBounds(Math.max(w, b.w), Math.max(s, b.s), Math
      .min(e, b.e), Math.min(n, b.n));
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

}
