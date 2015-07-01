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

package org.mrgeo.data.shp.esri.geom;

public class JPointZ extends JShape
{
  private static final long serialVersionUID = 1L;
  protected double m;
  protected Coord p;
  protected double z;

  /** Creates new JPoint */
  public JPointZ()
  {
    super(POINTZ);
    p = new Coord(0, 0);
    z = 0;
    m = 0;
    updateExtent();
    status = READY;
  }

  public JPointZ(Coord p, double z, double m)
  {
    super(POINTZ);
    this.p = p;
    this.z = z;
    this.m = m;
    updateExtent();
    status = READY;
  }

  public JPointZ(double x, double y, double z, double m)
  {
    super(POINTZ);
    p = new Coord(x, y);
    this.z = z;
    this.m = m;
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
    System.out.println("PointZ: " + toString() + " z:" + z + " m:" + m);
  }

  public Coord getCoord()
  {
    return p;
  }

  public double getM()
  {
    return m;
  }

  @Override
  public int getRecordLength()
  {
    return 36; // fixed
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

  public double getZ()
  {
    return z;
  }

  public void set(Coord p, double z)
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
    this.z = z;
  }

  public void set(double x, double y, double z)
  {
    p.x = x;
    p.y = y;
    this.z = z;
    updateExtent();
    status = READY;
  }

  public void setM(double m)
  {
    this.m = m;
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
