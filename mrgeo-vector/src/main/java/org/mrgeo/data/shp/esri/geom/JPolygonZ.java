/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

public class JPolygonZ extends JShape
{
  public final static int NEW_PART = 1; // new part flag
  public final static int PREV_PART = 0; // previous part flag
  @SuppressWarnings("hiding")
  static final long serialVersionUID = 1L;
  protected Double area;
  protected Coord centroid;
  protected double[] marr;
  protected double[] mrange;
  protected int nparts; // number of parts
  protected int npoints; // number of points
  protected int[] part; // array of parts
  protected Coord[] point; // array of simple points
  protected double[] zarr;
  protected double[] zrange;

  /** Creates new JPolygon */
  public JPolygonZ()
  {
    super(POLYGONZ);
    nparts = 0;
    npoints = 0;
    part = new int[0];
    point = new Coord[0];
    zarr = new double[0];
    marr = new double[0];
    centroid = null;
  }

  public void add(Coord p, double z, double m)
  {
    if (nparts == 0)
    {
      add(p, z, m, NEW_PART);
    }
    else
    {
      add(p, z, m, PREV_PART);
    }
  }

  public synchronized boolean add(Coord p, double z, double m, int flag)
  {
    if (p == null)
      return false;
    synchronized (this)
    {
      if (flag == NEW_PART)
      {
        // expand array of parts
        if (part.length == 0)
        {
          part = new int[++nparts];
        }
        else
        {
          int[] temp = new int[++nparts];
          System.arraycopy(part, 0, temp, 0, part.length);
          part = temp;
        }
        // add part
        part[part.length - 1] = point.length;
      }
      else
      {
        // ignore redundant coords!
        if (point[point.length - 1].equals(p))
          return false;
      }
      // expand array of points
      if (point.length == 0)
      {
        point = new Coord[++npoints];
        zarr = new double[npoints];
        marr = new double[npoints];
        centroid = (Coord) p.clone(); // default centroid is first point until
                                      // updated
      }
      else
      {
        Coord[] temp = new Coord[++npoints];
        System.arraycopy(point, 0, temp, 0, point.length);
        point = temp;
        double[] temp2 = new double[npoints];
        System.arraycopy(zarr, 0, temp2, 0, zarr.length);
        zarr = temp2;
        double[] temp3 = new double[npoints];
        System.arraycopy(marr, 0, temp3, 0, marr.length);
        marr = temp3;
      }
      // add point
      point[point.length - 1] = p;
      zarr[point.length - 1] = z;
      marr[point.length - 1] = m;
    }
    return true;
  }

  public void add(JPoint p, double z, double m)
  {
    add(p.getCoord(), z, m);
  }

  @Override
  public byte check(boolean clean)
  {
    // check if polygon is topographically sound
    synchronized (this)
    {
      for (int p = 0; p < part.length; p++)
      {
        int limit = ((p + 1) == part.length) ? point.length : part[p + 1];
        // part length?
        if (part[p] + 1 == limit)
        {
          // part has only one point!
          if (clean)
          {
            remove(part[p]);
            return check(clean); // re-check
          }
          return ERROR;
        }
        if (part[p] + 2 == limit)
        {
          // part has only two points!
          if (clean)
          {
            remove(part[p]);
            remove(part[p]);
            return check(clean); // re-check
          }
          return ERROR;
        }
        if (part[p] + 3 == limit && point[part[p]].equals(point[limit - 1]))
        {
          // part has only two unique points! (third being a close poly attempt
          // to the first)
          if (clean)
          {
            remove(part[p]);
            remove(part[p]);
            remove(part[p]);
            return check(clean); // re-check
          }
          return ERROR;
        }
        // closed?
        if (!point[part[p]].equals(point[limit - 1]))
          insert((Coord) point[part[p]].clone(), 0, 0, limit);
        status = READY;
      }
    }
    if (clean)
      updateExtent();
    return status;
  }

  public boolean contains(Coord c)
  {
    // fast answer (test MBR)
    if (c.x < this.extent.min.x)
      return false;
    if (c.x > this.extent.max.x)
      return false;
    if (c.y < this.extent.min.y)
      return false;
    if (c.y > this.extent.max.y)
      return false;

    // must now evaluate in detail
    int counter = 0;
    double xinters;
    Coord p1, p2;

    for (int p = 0; p < part.length; p++)
    {
      int limit = ((p + 1) == part.length) ? point.length : part[p + 1];
      // analyze
      p1 = point[part[p]];
      for (int i = (part[p] + 1); i < limit; i++)
      {
        p2 = point[i % point.length];
        if (c.y > Math.min(p1.y, p2.y))
        {
          if (c.y <= Math.max(p1.y, p2.y))
          {
            if (c.x <= Math.max(p1.x, p2.x))
            {
              if (p1.y != p2.y)
              {
                xinters = (c.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x;
                if (p1.x == p2.x || c.x <= xinters)
                  counter++;
              }
            }
          }
        }
        p1 = p2;
      }

      // return
      if (counter % 2 != 0)
        return true;
    }

    // default (not in any parts)
    return false;
  }

  @Override
  public void debug()
  {
    if (extent != null)
      System.out.println("Extent: " + extent.toString());
    System.out.println(toString());
    for (int p = 0; p < part.length; p++)
    {
      System.out.println("Part[" + p + "]: " + part[p]);
      int limit = ((p + 1) == part.length) ? point.length : part[p + 1];
      for (int i = part[p]; i < limit; i++)
        System.out.println("  PointZ[" + i + "]: " + point[i].toString() + "," + zarr[i] + ","
            + marr[i]);
    }
  }

  public double getArea()
  {
    if (area == null)
    {
      int hi, lo, j;
      double tempPart;
      double tempArea = 0;

      for (int p = 0; p < part.length; p++)
      {
        hi = ((p + 1) == part.length) ? point.length : part[p + 1];
        lo = part[p];
        tempPart = 0;
        for (int i = lo; i < hi - 1; i++)
        {
          j = i + 1;
          tempPart += point[i].x * point[j].y;
          tempPart -= point[i].y * point[j].x;
        }
        tempArea += tempPart;
      }

      tempArea = tempArea / 2;
      tempArea = Math.abs(tempArea);
      area = new Double(tempArea);
    }

    return area.doubleValue();
  }

  public Coord getCentroid()
  {
    if (centroid == null)
    {
      int hi, lo, j;
      double tempPartX, tempPartY;
      double tempAreaX = 0, tempAreaY = 0;

      for (int p = 0; p < part.length; p++)
      {
        hi = ((p + 1) == part.length) ? point.length : part[p + 1];
        lo = part[p];
        tempPartX = 0;
        tempPartY = 0;
        for (int i = lo; i < hi - 1; i++)
        {
          j = i + 1;
          tempPartX += (point[i].x + point[j].x)
              * (point[i].x * point[j].y - point[j].x * point[i].y);
          tempPartY += (point[i].y + point[j].y)
              * (point[i].x * point[j].y - point[j].x * point[i].y);
        }
        tempAreaX += tempPartX;
        tempAreaY += tempPartY;
      }

      tempAreaX = tempAreaX / (6 * getArea());
      tempAreaY = tempAreaY / (6 * getArea());
      tempAreaX = Math.abs(tempAreaX);
      tempAreaY = Math.abs(tempAreaY);
      centroid = new Coord(tempAreaX, tempAreaY);
    }
    return centroid;
  }

  public double getM(int i)
  {
    return marr[i];
  }

  public int getPart(int i)
  {
    return part[i];
  }

  public int getPartCount()
  {
    return part.length;
  }

  public Coord getPoint(int i)
  {
    return point[i];
  }

  public int getPointCount()
  {
    return point.length;
  }

  @Override
  public int getRecordLength()
  {
    int totParts = getPartCount();
    int totPoints = getPointCount();
    int recordLength = 44 + (totParts * 4) + (totPoints * 16);
    recordLength += 16 + (totPoints * 2); // Z
    recordLength += 16 + (totPoints * 2); // M
    return recordLength;
  }

  public double getZ(int i)
  {
    return zarr[i];
  }

  @SuppressWarnings("unused")
  public synchronized boolean insert(Coord p, double z, double m, int i)
  {
    // valid?
    if (i < 0 || i > point.length)
      return false;
    // insert coord into array
    synchronized (this)
    {
      Coord[] temp = new Coord[++npoints];
      System.arraycopy(point, 0, temp, 0, i);
      temp[i] = p;
      System.arraycopy(point, i, temp, i + 1, point.length - i);
      point = temp;
      double[] temp2 = new double[npoints];
      System.arraycopy(zarr, 0, temp2, 0, i);
      temp[i] = p;
      System.arraycopy(zarr, i, temp2, i + 1, zarr.length - i);
      zarr = temp2;
      double[] temp3 = new double[npoints];
      System.arraycopy(marr, 0, temp3, 0, i);
      temp[i] = p;
      System.arraycopy(marr, i, temp3, i + 1, marr.length - i);
      marr = temp3;
      // shift part indices for all successive parts
      for (int j = part.length - 1; j >= 0; j--)
      {
        if (part[j] <= i)
        {
          for (int k = j + 1; k < part.length; k++)
            part[k]++;
          break;
        }
      }
      status = UNKNOWN;
    }
    return true;
  }

  public synchronized boolean remove(int i)
  {
    // valid?
    if (i < 0 || i > point.length - 1)
      return false;
    // shift parts
    boolean ok = false;
    // determine what part index is in
    synchronized (this)
    {
      for (int p = part.length - 1; p >= 0; p--)
      {
        if (part[p] <= i)
        {
          int limit = ((p + 1) == part.length) ? point.length : part[p + 1];
          if (limit - 1 == i && part[p] == i)
          {
            // only point in part, remove part
            int[] temp = new int[part.length - 1];
            if (part.length > 1)
            {
              System.arraycopy(part, 0, temp, 0, p);
              System.arraycopy(part, p + 1, temp, p, part.length - p - 1);
            }
            if (p < part.length - 1)
              temp[p]--; // decrement that part index if not last
            part = temp;
          }
          // shift part indices by 1 for all successive parts
          for (int k = p + 1; k < part.length; k++)
            part[k]--;
          // done!
          ok = true;
          break;
        }
      }
      // check part step ok
      if (!ok)
        return false;
      // shift coords
      if (point.length == 1)
      {
        point = new Coord[0];
      }
      else
      {
        Coord[] temp = new Coord[point.length - 1];
        System.arraycopy(point, 0, temp, 0, i);
        System.arraycopy(point, i + 1, temp, i, point.length - i - 1);
        point = temp;
        double[] temp2 = new double[point.length - 1];
        System.arraycopy(zarr, 0, temp2, 0, i);
        System.arraycopy(zarr, i + 1, temp2, i, zarr.length - i - 1);
        zarr = temp2;
        double[] temp3 = new double[point.length - 1];
        System.arraycopy(marr, 0, temp3, 0, i);
        System.arraycopy(marr, i + 1, temp3, i, marr.length - i - 1);
        marr = temp3;
      }
      // reset sizes, extent, and return
      nparts = part.length;
      npoints = point.length;
      status = UNKNOWN;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "parts:" + nparts + "|points:" + npoints;
  }

  @SuppressWarnings({ "static-method", "unused" })
  public JPolygon union(JPolygon p)
  {
    return null;
  }

  @Override
  public void updateExtent()
  {
    JExtent temp = new JExtent(Double.MAX_VALUE, Double.MAX_VALUE, Double.MIN_VALUE,
        Double.MIN_VALUE);
    if (status != READY)
      check(false); // close polygon if not closed
    synchronized (point)
    {
      for (int i = 0; i < point.length - 1; i++)
      { // assume first & last points are the same
        if (point[i].x < temp.min.x)
          temp.min.x = point[i].x;
        if (point[i].x > temp.max.x)
          temp.max.x = point[i].x;
        if (point[i].y < temp.min.y)
          temp.min.y = point[i].y;
        if (point[i].y > temp.max.y)
          temp.max.y = point[i].y;
      }
      // calc area & centroid
      area = null;
      getArea();
      centroid = null;
      getCentroid();
    }
    extent = temp;
  }
}
