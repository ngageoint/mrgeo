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

import com.vividsolutions.jts.geom.GeometryFactory;
import org.mrgeo.utils.tms.Bounds;

import java.io.*;
import java.util.*;


/**
 * @author jason.surratt
 * 
 */
public class GeometryCollectionImpl extends GeometryImpl implements WritableGeometryCollection
{
  List<WritableGeometry> geometries = new ArrayList<WritableGeometry>();

  List<String> roles = new ArrayList<String>();

  GeometryCollectionImpl()
  {

  }
  
  GeometryCollectionImpl(Map<String, String> attributes)
  {
    this.attributes.putAll(attributes);
  }

  @Override
  public Collection<? extends Geometry> getGeometries()
  {
    return geometries;
  }

  @Override
  public Iterator<? extends Geometry> iterator()
  {
    return geometries.iterator();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.Geometry#createWritableClone()
   */
  @Override
  public WritableGeometry createWritableClone()
  {
    GeometryCollectionImpl result = new GeometryCollectionImpl();
    for (Geometry r : geometries)
    {
      result.addGeometry(r.createWritableClone());
    }
    result.attributes.putAll(attributes);
    result.roles.addAll(roles);
    return result;
  }

  @Override
  public void filter(PointFilter pf)
  {
    for (WritableGeometry g : geometries)
    {
      g.filter(pf);
    }
  }


  @Override
  public boolean isValid()
  {
    boolean result = geometries.size() > 0;
    for (WritableGeometry g : geometries)
    {
      if (g.isValid() == false)
      {
        result = false;
      }
    }

    return result;
  }

  @Override
  public void addGeometry(WritableGeometry g)
  {
    geometries.add(g);
  }
  
  /**
   * Do not call this method within the middle of iterating
   * the collection returned from getGeometries(). It will
   * throw a ConcurrentModificationException. Instead, use
   * iterator() and call remove() on the returned Iterator
   * after iterating to a geometry that needs to be removed.
   */
  @Override
  public boolean removeGeometry(Geometry g)
  {
    return geometries.remove(g);
  }

  @Override
  public void read(DataInputStream stream) throws IOException
  {
    int size = stream.readInt();
    geometries = new ArrayList<>(size);

    Geometry.Type[] types = Geometry.Type.values(); 
    for (int i = 0; i < size; i++)
    {
      Geometry.Type type = types[stream.readInt()];

      WritableGeometry geometry = null;
      switch (type)
      {
      case COLLECTION:
        geometry = new GeometryCollectionImpl();
        break;
      case LINEARRING:
        geometry = new LinearRingImpl();
        break;
      case LINESTRING:
        geometry = new LineStringImpl();
        break;
      case POINT:
        geometry = new PointImpl();
        break;
      case POLYGON:
        geometry = new PolygonImpl();
        break;
      default:
        throw new IOException("Unsupported geometry type");
      }

      // TODO: exception
      geometry.read(stream);
      addGeometry(geometry);
    }
  }

  @Override
  public void write(DataOutputStream stream) throws IOException
  {
    stream.writeInt(geometries.size());
    for (Geometry geometry: geometries)
    {
      if (geometry instanceof Point)
      {
        stream.writeInt(Geometry.Type.POINT.ordinal());
      }
      else if (geometry instanceof LineString)
      {
        stream.writeInt(Geometry.Type.LINESTRING.ordinal());
      }
      else if (geometry instanceof LinearRing)
      {
        stream.writeInt(Geometry.Type.LINEARRING.ordinal());
      }
      else if (geometry instanceof Polygon)
      {
        stream.writeInt(Geometry.Type.POLYGON.ordinal());
      }
      else if (geometry instanceof GeometryCollection)
      {
        stream.writeInt(Geometry.Type.COLLECTION.ordinal());
      }
      else
      {
        throw new IOException("Unsupported geometry type");
      }
      geometry.write(stream);
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
    return Geometry.Type.COLLECTION;
  }

  @Override
  public com.vividsolutions.jts.geom.GeometryCollection toJTS()
  {
    GeometryFactory factory = new GeometryFactory();

    com.vividsolutions.jts.geom.Geometry[] jtsGeometries = new com.vividsolutions.jts.geom.Geometry[geometries.size()];
//    LinkedList<com.vividsolutions.jts.geom.Geometry> jtsGeometries = new LinkedList<com.vividsolutions.jts.geom.Geometry>();
    int i = 0;
    for (Geometry geometry : geometries)
    {
//      jtsGeometries.add(geometry.toJTS());
      jtsGeometries[i++] = geometry.toJTS();
    }

//    return factory.createGeometryCollection(
//      (com.vividsolutions.jts.geom.Geometry[]) jtsGeometries.toArray());
    return factory.createGeometryCollection(jtsGeometries);
  }

  @Override
  public void fromJTS(com.vividsolutions.jts.geom.GeometryCollection collection)
  {
    for (int i = 0; i < collection.getNumGeometries(); i++)
    {
      com.vividsolutions.jts.geom.Geometry jtsGeometry = collection.getGeometryN(i);

      if (jtsGeometry instanceof com.vividsolutions.jts.geom.Point)
      {
        WritablePoint pt = new PointImpl();
        pt.fromJTS((com.vividsolutions.jts.geom.Point) jtsGeometry);
        addGeometry(pt);
      }
      // make sure LinearRing is before LineString!
      else if (jtsGeometry instanceof com.vividsolutions.jts.geom.LinearRing)
      {
        WritableLinearRing ring = new LinearRingImpl();
        ring.fromJTS((com.vividsolutions.jts.geom.LineString) jtsGeometry);
        addGeometry(ring);
      }
      else if (jtsGeometry instanceof com.vividsolutions.jts.geom.LineString)
      {
        WritableLineString string = new LineStringImpl();
        string.fromJTS((com.vividsolutions.jts.geom.LineString) jtsGeometry);
        addGeometry(string);
      }
      else if (jtsGeometry instanceof com.vividsolutions.jts.geom.Polygon)
      {
        WritablePolygon polygon = new PolygonImpl();
        polygon.fromJTS((com.vividsolutions.jts.geom.Polygon) jtsGeometry);
        addGeometry(polygon);
      }
    }
  }

  @Override
  public int getNumGeometries()
  {
    return geometries.size();
  }

  @Override
  public Geometry getGeometry(int index)
  {
    if (index < geometries.size())
    {
      return geometries.get(index);
    }
    return null;
  }

  @Override
  public String getRole(int index)
  {
    if (index < roles.size())
    {
      return roles.get(index);
    }
    return null;
  }

  @Override
  public void setRole(int index, String role)
  {
    roles.add(index, role);
  }

  @Override
  public Bounds getBounds()
  {
    if (bounds == null)
    {
      bounds = new Bounds();

      for (Geometry geometry: geometries)
      {
        Bounds b = geometry.getBounds();
        if (b != null)
        {
          bounds.expand(b);
        }
      }
    }

    return bounds;
  }

  @Override
  public boolean isEmpty()
  {
    return geometries.isEmpty();
  }

  @Override
  public Geometry clip(Polygon geom)
  {
    Geometry clipped = super.clip(geom);
    
    if (clipped instanceof GeometryCollection)
    {
      WritableGeometryCollection collection = (WritableGeometryCollection) clipped;
      // got a collection.  Need to make sure they are all valid.
      Iterator<? extends Geometry> iter = collection.iterator();
      while (iter.hasNext())
      {
        Geometry g = iter.next();
        if (g.isEmpty())
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
        WritableGeometry g = (WritableGeometry) collection.getGeometry(0);
        g.setAttributes(collection.getAllAttributes());
        return g;
      }
      return collection;
    }
    return null;
  }

}
