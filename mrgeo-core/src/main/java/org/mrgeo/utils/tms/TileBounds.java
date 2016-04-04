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

import org.mrgeo.utils.LongRectangle;

public class TileBounds
{
  public long n;
  public long s;
  public long e;
  public long w;

  public TileBounds(final long tx, final long ty)
  {
    this.n = ty;
    this.s = ty;
    this.e = tx;
    this.w = tx;
  }

  public TileBounds(final long w, final long s, final long e, final long n)
  {
    this.n = n;
    this.s = s;
    this.e = e;
    this.w = w;
  }

  public TileBounds(final Tile t)
  {
    this.n = t.ty;
    this.s = t.ty;
    this.e = t.tx;
    this.w = t.tx;
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

  public static TileBounds convertFromLongRectangle(final LongRectangle rectangle)
  {
    return new TileBounds(rectangle.getMinX(), rectangle.getMinY(), rectangle.getMaxX(),
        rectangle.getMaxY());
  }

  public static LongRectangle convertToLongRectangle(final TileBounds bounds)
  {
    return new LongRectangle(bounds.w, bounds.s, bounds.e, bounds.n);
  }

  public void expand(final long tx, final long ty)
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

  public void expand(final long west, final long south, final long east, final long north)
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

  public void expand(final Tile t)
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

  public void expand(final TileBounds b)
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

  public static TileBounds fromCommaString(String str) {
    String[] split = str.split(",");

    long w = Long.parseLong(split[0]);
    long s = Long.parseLong(split[1]);
    long e = Long.parseLong(split[2]);
    long n = Long.parseLong(split[3]);

    return new TileBounds(w, s, e, n);
  }

  public boolean contains(final long tx, final long ty) {
    return contains(new Tile(tx, ty), true);
  }

  public boolean contains(final long tx, final long ty, final boolean includeAdjacent) {
    return contains(new Tile(tx, ty), includeAdjacent);
  }

  public boolean contains(final Tile tile) {
    return contains(tile, true);
  }

  public boolean contains(final Tile tile, final boolean includeAdjacent) {
    if (includeAdjacent)
    {
      return (tile.tx >= w && tile.ty >= s && tile.tx <= e && tile.ty <= n);
    }
    else
    {
      return (tile.tx > w && tile.ty > s && tile.tx < e && tile.ty < n);
    }
  }

  public TileBounds intersection(final TileBounds b)
  {
    return intersection(b, true);
  }

  /**
   * If the two boundaries are adjacent, this would return true iff includeAdjacent is true
   *
   */
  public TileBounds intersection(final TileBounds b, final boolean includeAdjacent)
  {

    final TileBounds
        intersectBounds = new TileBounds(Math.max(this.w, b.w), Math.max(this.s, b.s), Math
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

}
