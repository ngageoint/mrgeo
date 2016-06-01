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

package org.mrgeo.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import org.mrgeo.utils.tms.Bounds;

import java.io.*;
import java.util.*;


/**
 * @author jason.surratt
 *
 */
public class LineStringImpl extends GeometryImpl implements WritableLineString
{
private static final long serialVersionUID = 1L;

List<WritablePoint> points = new ArrayList<>();

LineStringImpl()
{
}

LineStringImpl(Map<String, String>attributes)
{
  this.attributes.putAll(attributes);
}

LineStringImpl(Point... points)
{
  for (Point p : points)
  {
    this.points.add((WritablePoint) p.asWritable());
  }
}

public static Class[] getClasses()
{
  return new Class[]{LineStringImpl.class, ArrayList.class};
}

/*
 * (non-Javadoc)
 *
 * @see
 * com.spadac.Geometry.WritableLinearRing#addPoint(com.spadac.Geometry.Point)
 */
@Override
public void addPoint(Point p)
{
//    if (points.isEmpty() || (p.getGeohashBits() != points.lastElement().getGeohashBits()))
//    {
  points.add((WritablePoint) p.createWritableClone());
//    }
//    else
//    {
//      System.out.println("Skipping " + p + " points size is " + points.size());
//    }
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.Geometry#createWritableClone()
 */
@Override
public WritableGeometry createWritableClone()
{
  LineStringImpl result = new LineStringImpl();
  for (Point p : points)
  {
    result.points.add((WritablePoint) p.createWritableClone());
  }

  result.attributes.putAll(attributes);

  return result;
}

@Override
public void filter(PointFilter pf)
{
  for (WritablePoint p : points)
  {
    pf.filter(p);
  }
}



/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.LinearRing#getNumPoints()
 */
@Override
public int getNumPoints()
{
  return points.size();
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.LinearRing#getPoint(int)
 */
@Override
public Point getPoint(int i)
{
  return points.get(i);
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.LinearRing#getPoints()
 */
@Override
public Vector<Point> getPoints()
{
  Vector<Point> result = new Vector<Point>();
  for (Point p : points)
  {
    result.add(p);
  }
  return result;
}

@Override
public boolean isValid()
{
  return points.size() >= 2;
}


/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.WritableLinearRing#setPoints(java.util.Collection)
 */
@Override
public void setPoints(Collection<Point> points)
{
  this.points.clear();
  for (Point p : points)
  {
    this.points.add((WritablePoint) p.createWritableClone());
  }
}
@Override
public void read(DataInputStream stream) throws IOException
{
  int size = stream.readInt();
  points = new ArrayList<>(size);

  for (int i = 0; i < size; i++)
  {
    PointImpl p = new PointImpl();
    p.read(stream);

    addPoint(p);
  }
}

@Override
public void write(DataOutputStream stream) throws IOException
{
  stream.writeInt(points.size());
  for (Point p : points)
  {
    p.write(stream);
  }
}

private synchronized void writeObject(ObjectOutputStream stream) throws IOException
{
  DataOutputStream dos = new DataOutputStream(stream);
  write(dos);
  writeAttributes(dos);
}

private synchronized void readObject(ObjectInputStream stream) throws IOException
{
  DataInputStream dis = new DataInputStream(stream);
  read(dis);
  readAttributes(dis);
}

@Override
public Type type()
{
  return Geometry.Type.LINESTRING;
}

@Override
public void fromJTS(com.vividsolutions.jts.geom.LineString jtsLine)
{
  points.clear();

  for (int i = 0; i < jtsLine.getNumPoints(); i++)
  {
    WritablePoint pt = new PointImpl();
    pt.fromJTS(jtsLine.getPointN(i));

    addPoint(pt);
  }
}

@Override
public com.vividsolutions.jts.geom.LineString toJTS()
{
  Coordinate[] coordinates = new Coordinate[getNumPoints()];
  for (int i = 0; i < getNumPoints(); i++)
  {
    Point pt = getPoint(i);
    coordinates[i] = new Coordinate(pt.getX(), pt.getY());
  }
  com.vividsolutions.jts.geom.GeometryFactory factory = new com.vividsolutions.jts.geom.GeometryFactory();

  return factory.createLineString(coordinates);
}

@Override
public Bounds getBounds()
{
  if (bounds == null)
  {
    for (Point pt: points)
    {
      if (bounds == null)
      {
        bounds = new Bounds(pt.getX(), pt.getY(), pt.getX(), pt.getY());
      }
      else
      {
        bounds = bounds.expand(pt.getX(), pt.getY());
      }
    }
  }

  return bounds;
}


@Override
public void addPoint(double x, double y)
{
  points.add(GeometryFactory.createPoint(x, y));
}

@Override
public boolean isEmpty()
{
  return points.isEmpty();
}


@Override
public Geometry clip(Polygon geom)
{
  Geometry clipped = super.clip(geom);

  // got a linestring back...  just return it
  if (clipped instanceof LineString && !clipped.isEmpty())
  {
    return clipped;
  }
  else if (clipped instanceof GeometryCollection)
  {
    WritableGeometryCollection collection = (WritableGeometryCollection) clipped;
    // got a collection.  Need to make sure they are only linestrings.
    Iterator<? extends Geometry> iter = collection.iterator();
    while (iter.hasNext())
    {
      Geometry g = iter.next();
      if (!(g instanceof LineString) || g.isEmpty())
      {
        iter.remove();
      }
    }
    if (collection.getNumGeometries() == 0)
    {
      return null;
    }
    else if (collection.getNumGeometries() == 1)
    {
      return collection.getGeometry(0);
    }
    return collection;
  }
  return null;
}
}
