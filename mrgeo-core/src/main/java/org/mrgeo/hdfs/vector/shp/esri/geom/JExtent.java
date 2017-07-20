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

package org.mrgeo.hdfs.vector.shp.esri.geom;

public final class JExtent implements Cloneable, java.io.Serializable
{
static final long serialVersionUID = 1L;
Coord max;
Coord min;

/**
 * Creates new JExtent
 */
public JExtent()
{
  this(0, 0, 0, 0);
}

public JExtent(double minx, double miny, double maxx, double maxy)
{
  min = new Coord(minx, miny);
  max = new Coord(maxx, maxy);
}

public static void intersection(JExtent e1, JExtent e2, JExtent result)
{
  if ((e1 != null) && (e2 != null))
  {
    if (e2.min.x < e1.max.x && e2.max.x > e1.min.x && e2.min.y < e1.max.y && e2.max.y > e1.min.y)
    {
      if (result != null)
      {
        result.setExtent(Math.max(e1.min.x, e2.min.x), Math.max(e1.min.y, e2.min.y), Math.min(
            e1.max.x, e2.max.x), Math.min(e1.max.y, e2.max.y));
      }
    }
  }
}

public static boolean intersects(JExtent e1, JExtent e2)
{
  if ((e1 != null) && (e2 != null))
  {
    if (e2.min.x < e1.max.x && e2.max.x > e1.min.x && e2.min.y < e1.max.y && e2.max.y > e1.min.y)
    {
      return true;
    }
  }
  return false;
}

public static void union(JExtent e1, JExtent e2, JExtent result)
{
  if ((e1 != null) && (e2 != null))
  {
    if (result != null)
    {
      result.setExtent(Math.min(e1.min.x, e2.min.x), Math.min(e1.min.y, e2.min.y), Math.max(
          e1.max.x, e2.max.x), Math.max(e1.max.y, e2.max.y));
    }
  }
}

@Override
@SuppressWarnings("squid:S1166") // Exception caught and handled
public Object clone()
{
  try
  {
    JExtent extent = (JExtent) super.clone();
    extent.min = (Coord) min.clone();
    extent.max = (Coord) max.clone();
    return extent;
  }
  catch (CloneNotSupportedException e)
  {
    // note: we don't propagate this exception because JExtent is final
    throw new InternalError();
  }
}

public double getCenterX()
{
  return min.x + (max.x - min.x) / 2;
}

public double getCenterY()
{
  return min.y + (max.y - min.y) / 2;
}

public double getHeight()
{
  return max.y - min.y;
}

public double getMaxX()
{
  return max.x;
}

public double getMaxY()
{
  return max.y;
}

public double getMinX()
{
  return min.x;
}

public double getMinY()
{
  return min.y;
}

public double getWidth()
{
  return max.x - min.x;
}

public double getX()
{
  return min.x;
}

public double getY()
{
  return min.y; // max??
}

public void setExtent(double minx, double miny, double maxx, double maxy)
{
  min.x = minx;
  min.y = miny;
  max.x = maxx;
  max.y = maxy;
}

public void setExtent(JExtent src)
{
  if (src == null)
  {
    min.x = 0;
    min.y = 0;
    max.x = 0;
    max.y = 0;
    return;
  }
  min.x = src.min.x;
  min.y = src.min.y;
  max.x = src.max.x;
  max.y = src.max.y;
}

@Override
public String toString()
{
  return min + "|" + max;
}
}
