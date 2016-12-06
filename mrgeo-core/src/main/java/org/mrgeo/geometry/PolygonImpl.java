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


import org.mrgeo.utils.tms.Bounds;

import java.io.*;
import java.util.*;


/**
 * @author jason.surratt
 */
public class PolygonImpl extends GeometryImpl implements WritablePolygon
{
private static final long serialVersionUID = 1L;

WritableLinearRing exteriorRing = null;
List<WritableLinearRing> interiorRings = new ArrayList<>();

PolygonImpl()
{

}

PolygonImpl(Map<String, String> attributes)
{
  this.attributes.putAll(attributes);
}

PolygonImpl(Point... points)
{
  exteriorRing = new LinearRingImpl(points);
}

PolygonImpl(Collection<Point> points)
{
  exteriorRing = new LinearRingImpl();
  exteriorRing.setPoints(points);
}

public static Class[] getClasses()
{
  return new Class[]{PolygonImpl.class};
}

@Override
public void addInteriorRing(LinearRing lr)
{
  interiorRings.add((WritableLinearRing) lr.createWritableClone());
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.Geometry#createWritableClone()
 */
@Override
public WritableGeometry createWritableClone()
{
  PolygonImpl result = new PolygonImpl();
  result.setExteriorRing(getExteriorRing());
  result.interiorRings = new Vector<>();
  for (LinearRing r : interiorRings)
  {
    result.interiorRings.add((WritableLinearRing) r.createWritableClone());
  }
  result.attributes.putAll(attributes);
  return result;
}

@Override
public void filter(PointFilter pf)
{
  if (exteriorRing != null)
  {
    exteriorRing.filter(pf);
  }
  for (WritableLinearRing r : interiorRings)
  {
    r.filter(pf);
  }
}


/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.Polygon#getExteriorRing()
 */
@Override
public LinearRing getExteriorRing()
{
  return exteriorRing;
}

@Override
public void setExteriorRing(LinearRing ring)
{
  exteriorRing = (WritableLinearRing) ring.createWritableClone();
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.Polygon#getInteriorRing(int)
 */
@Override
public LinearRing getInteriorRing(int i)
{
  return interiorRings.get(i);
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.Geometry.Polygon#getNumInteriorRings()
 */
@Override
public int getNumInteriorRings()
{
  return interiorRings.size();
}

@Override
public boolean isValid()
{
  boolean result = true;
  if (exteriorRing == null || exteriorRing.getNumPoints() == 0)
  {
    result = false;
  }
  else if (!exteriorRing.isValid())
  {
    result = false;
  }
  else
  {
    for (LinearRing lr : interiorRings)
    {
      if (!lr.isValid())
      {
        result = false;
      }
    }
  }
  return result;
}

@Override
public void read(DataInputStream stream) throws IOException
{
  exteriorRing = GeometryFactory.createLinearRing();
  exteriorRing.read(stream);

  int rings = stream.readInt();
  interiorRings = new ArrayList<>(rings);
  for (int i = 0; i < rings; i++)
  {
    WritableLinearRing ring = GeometryFactory.createLinearRing();
    ring.read(stream);
    addInteriorRing(ring);
  }
}

@Override
public void write(DataOutputStream stream) throws IOException
{
  exteriorRing.write(stream);

  stream.writeInt(interiorRings.size());
  for (LinearRing ring : interiorRings)
  {
    ring.write(stream);
  }
}

@Override
public Type type()
{
  return Geometry.Type.POLYGON;
}

@Override
public void fromJTS(com.vividsolutions.jts.geom.Polygon jtsPolygon)
{
  interiorRings.clear();

  WritableLinearRing ring = new LinearRingImpl();

  if (jtsPolygon.isEmpty())
  {
    setExteriorRing(ring);
    return;
  }

  ring.fromJTS(jtsPolygon.getExteriorRing());

  setExteriorRing(ring);

  for (int i = 0; i < jtsPolygon.getNumInteriorRing(); i++)
  {
    ring = new LinearRingImpl();
    ring.fromJTS(jtsPolygon.getInteriorRingN(i));

    addInteriorRing(ring);
  }
}

@Override
public com.vividsolutions.jts.geom.Polygon toJTS()
{
  com.vividsolutions.jts.geom.GeometryFactory factory = new com.vividsolutions.jts.geom.GeometryFactory();

  com.vividsolutions.jts.geom.LinearRing exterior = exteriorRing.toJTS();
  com.vividsolutions.jts.geom.LinearRing[] interior = null;

  if (getNumInteriorRings() > 0)
  {
    interior = new com.vividsolutions.jts.geom.LinearRing[getNumInteriorRings()];
    for (int i = 0; i < getNumInteriorRings(); i++)
    {
      interior[i] = getInteriorRing(i).toJTS();
    }
  }

  return factory.createPolygon(exterior, interior);

}

@Override
public Bounds getBounds()
{
  if (bounds == null)
  {
    if (exteriorRing != null)
    {
      bounds = exteriorRing.getBounds();
    }
  }
  return bounds;
}

@Override
public boolean isEmpty()
{
  return exteriorRing == null || exteriorRing.isEmpty();
}

@Override
public Geometry clip(Polygon geom)
{
  Geometry clipped = super.clip(geom);

  // got a polygon back...  just return it
  if (clipped instanceof Polygon && !clipped.isEmpty())
  {
    return clipped;
  }
  else if (clipped instanceof GeometryCollection)
  {
    WritableGeometryCollection collection = (WritableGeometryCollection) clipped;
    // got a collection.  Need to make sure they are only polygons.
    Iterator<? extends Geometry> iter = collection.iterator();
    while (iter.hasNext())
    {
      Geometry g = iter.next();
      if (!(g instanceof Polygon) || g.isEmpty())
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

private void writeObject(ObjectOutputStream stream) throws IOException
{
  DataOutputStream dos = new DataOutputStream(stream);
  write(dos);
  writeAttributes(dos);
}

private void readObject(ObjectInputStream stream) throws IOException
{
  DataInputStream dis = new DataInputStream(stream);
  read(dis);
  readAttributes(dis);
}


}
