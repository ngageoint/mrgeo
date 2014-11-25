/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.esri.geom;

public class JPoint extends JShape
{
  @SuppressWarnings("hiding")
  static final long serialVersionUID = 1L;
  protected Coord p;

  /** Creates new JPoint */
  public JPoint()
  {
    super(POINT);
    p = new Coord(0, 0);
    updateExtent();
    status = READY;
  }

  public JPoint(Coord p)
  {
    super(POINT);
    this.p = p;
    updateExtent();
    status = READY;
  }

  public JPoint(double x, double y)
  {
    super(POINT);
    p = new Coord(x, y);
    updateExtent();
    status = READY;
  }

  @Override
  public byte check(boolean clean)
  {
    if (p == null)
    {
      if (clean)
      {
        p = new Coord(0, 0);
        status = READY;
      }
      else
      {
        status = ERROR;
      }
    }
    else
    {
      status = READY;
    }
    if (clean)
      updateExtent();
    return status;
  }

  @Override
  public void debug()
  {
    if (extent != null)
      System.out.println("Extent: " + extent.toString());
    System.out.println("Point: " + toString());
  }

  public Coord getCoord()
  {
    return p;
  }

  @Override
  public int getRecordLength()
  {
    return 20; // fixed
  }

  public double getX()
  {
    if (p != null)
    {
      return p.x;
    }
    return 0;
  }

  public double getY()
  {
    if (p != null)
    {
      return p.y;
    }
    return 0;
  }

  public void set(Coord p)
  {
    if (p != null)
    {
      status = READY;
    }
    else
    {
      status = ERROR;
    }
    this.p = p;
  }

  public void set(double x, double y)
  {
    p.x = x;
    p.y = y;
    updateExtent();
    status = READY;
  }

  @Override
  public String toString()
  {
    if (p != null)
    {
      return p.toString();
    }
    return null;
  }

  @Override
  public void updateExtent()
  {
    if (extent == null)
      extent = new JExtent();
    extent.setExtent(p.x - 0.001, p.y - 0.001, p.x + 0.001, p.y + 0.001);
  }
}
