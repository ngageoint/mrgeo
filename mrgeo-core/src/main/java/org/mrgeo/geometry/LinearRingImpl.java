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

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import org.mrgeo.utils.FloatUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;

/**
 * @author jason.surratt
 *
 */
public class LinearRingImpl extends LineStringImpl implements WritableLinearRing
{
private static final long serialVersionUID = 1L;

LinearRingImpl()
{
}

LinearRingImpl(Map<String, String> attributes)
{
  this.attributes.putAll(attributes);
}

LinearRingImpl(Point... points)
{
  super(points);
  closeRing();
}

public static Class[] getClasses()
{
  return new Class[]{LinearRingImpl.class};
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.WritableLinearRing#closeRing()
 */
@Override
public void closeRing()
{
  Point start = points.get(0);
  Point end = points.get(points.size() - 1);
  if (!(FloatUtils.isEqual(start.getX(), end.getX()) &&
      FloatUtils.isEqual(start.getY(), end.getY()) &&
      FloatUtils.isEqual(start.getZ(), end.getZ())))
  {
    addPoint(start);
  }
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.Geometry#createWritableClone()
 */
@Override
public WritableGeometry createWritableClone()
{
  LinearRingImpl result = new LinearRingImpl();
  for (Point p : points)
  {
    result.points.add((WritablePoint) p.createWritableClone());
  }
  return result;
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.WritableLinearRing#setPoints(java.util.Collection)
 */
@Override
public void setPoints(Collection<Point> points)
{
  super.setPoints(points);
  closeRing();
}

@Override
public void read(DataInputStream stream) throws IOException
{
  super.read(stream);
  closeRing();
}

private void readObject(ObjectInputStream stream) throws IOException
{
  DataInputStream dis = new DataInputStream(stream);
  read(dis);
  readAttributes(dis);
}

@Override
public void fromJTS(LineString jtsRing)
{
  super.fromJTS(jtsRing);
  closeRing();
}

@Override
public com.vividsolutions.jts.geom.LinearRing toJTS()
{
  if (getNumPoints() < 3)
  {
    System.out.println("too few points");
  }

  Coordinate[] coordinates = new Coordinate[getNumPoints()];
  for (int i = 0; i < getNumPoints(); i++)
  {
    Point pt = getPoint(i);
    coordinates[i] = new Coordinate(pt.getX(), pt.getY());
  }

  com.vividsolutions.jts.geom.GeometryFactory factory = new com.vividsolutions.jts.geom.GeometryFactory();

  return factory.createLinearRing(coordinates);
}

@Override
public boolean isCW()
{
  return !isCCW();
}

@Override
public boolean isCCW()
{
  Coordinate[] coordinates = new Coordinate[getNumPoints()];
  for (int i = 0; i < getNumPoints(); i++)
  {
    Point pt = getPoint(i);
    coordinates[i] = new Coordinate(pt.getX(), pt.getY());
  }

  return CGAlgorithms.isCCW(coordinates);
}

@Override
public LinearRing ccw()
{
  if (isCCW())
  {
    return this;
  }

  return reverse();
}

@Override
public LinearRing reverse()
{
  WritableLinearRing rev = GeometryFactory.createLinearRing(getAllAttributes());
  for (int i = getNumPoints() - 1; i > 0; i--)
  {
    rev.addPoint(getPoint(i));
  }

  return rev;

}

@Override
public LinearRing cw()
{
  if (isCW())
  {
    return this;
  }

  return reverse();
}


}
