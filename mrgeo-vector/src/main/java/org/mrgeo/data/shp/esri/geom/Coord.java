/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.esri.geom;

public final class Coord extends java.lang.Object implements Cloneable, java.io.Serializable
{
  static final long serialVersionUID = 1L;
  public double x;
  public double y;

  /** Creates new Coord */
  public Coord()
  {
    x = 0;
    y = 0;
  }

  public Coord(double x, double y)
  {
    this.x = x;
    this.y = y;
  }

  @Override
  public Object clone()
  {
    try
    {
      return super.clone();
    }
    catch (CloneNotSupportedException e)
    {
      // note: we don't propagate this exception because Coord is final
      throw new InternalError(e.toString());
    }
  }


  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj != null)
    {
      try
      {
        Coord p = (Coord) obj;
        if (p.x == x && p.y == y)
          return true;
      }
      catch (ClassCastException e)
      {
      }
    }
    return false;
  }

  @Override
  public String toString()
  {
    return x + "," + y;
  }
}
