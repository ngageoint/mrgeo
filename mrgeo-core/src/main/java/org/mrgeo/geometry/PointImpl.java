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

package org.mrgeo.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.GeoHash;

import java.io.*;
import java.util.Map;


/**
 * @author jason.surratt
 * 
 */
public class PointImpl extends GeometryImpl implements WritablePoint
{
  
  double x;
  double y;
  double z;
  
  boolean hash = false;
  long geohash;

  PointImpl(Map<String, String> attributes)
  {
    this();
    this.attributes.putAll(attributes);
  }
  
    PointImpl()
    {
    this.x = Double.NaN;
    this.y = Double.NaN;
    this.z = 0.0;
      }

  PointImpl(double x, double y)
  {
    this.x = x;
    this.y = y;
    this.z = 0;
      }

  PointImpl(double x, double y, double z)
  {
    this.x = x;
    this.y = y;
    this.z = z;
    
  }

  PointImpl(double x, double y, double z, Map<String, String> attributes)
  {
    this.x = x;
    this.y = y;
    this.z = z;

    this.attributes.clear();
    this.attributes.putAll(attributes);
  }

  PointImpl(double x, double y, Map<String, String> attributes)
  {
    this.x = x;
    this.y = y;
    this.z = 0;

    this.attributes.clear();
    this.attributes.putAll(attributes);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.Geometry#createWritableClone()
   */
  @Override
  public WritableGeometry createWritableClone()
  {
    return new PointImpl(x, y, z, attributes);
  }

  @Override
  public void filter(PointFilter pf)
  {
    pf.filter(this);
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.Point#getX()
   */
  @Override
  public double getX()
  {
    return x;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.Point#getY()
   */
  @Override
  public double getY()
  {
    return y;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.Point#getZ()
   */
  @Override
  public double getZ()
  {
    return z;
  }

  @Override
  public boolean isValid()
  {
    return Double.isNaN(x) == false && Double.isNaN(y) == false && Double.isNaN(z) == false;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.WritablePoint#setX(double)
   */
  @Override
  public void setX(double x)
  {
    this.x = x;
    hash = false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.WritablePoint#setY(double)
   */
  @Override
  public void setY(double y)
  {
    this.y = y;
    hash = false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.WritablePoint#setZ(double)
   */
  @Override
  public void setZ(double z)
  {
    this.z = z;
  }

  @Override
  public void write(DataOutputStream stream) throws IOException
  {
    stream.writeDouble(x);
    stream.writeDouble(y);
    stream.writeDouble(z);
  }
  
  @Override
  public void read(DataInputStream stream) throws IOException
  {
    x = stream.readDouble();
    y = stream.readDouble();
    z = stream.readDouble();
    hash = false;
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
    return Geometry.Type.POINT;
  }

  @Override
  public Point toJTS()
  {
    GeometryFactory factory = new GeometryFactory();
    return factory.createPoint(new Coordinate(x, y));
  }

  @Override
  public void fromJTS(Point jtsPoint)
  {
    x = jtsPoint.getX();
    y = jtsPoint.getY();
    
    hash = false;
  }

  @Override
  public long getGeohashBits()
  {
    if (!hash)
    {
      geohash = GeoHash.encodeBits(y,  x);
      hash = false;
    }
    return geohash;
  }

  @Override
  public String getGeohash()
  {
    return GeoHash.encode(y, x);
  }

  @Override
  public Bounds getBounds()
  {
    if (bounds == null)
    {
      bounds = new Bounds(x,  y, x, y);
    }
    
    return bounds;
  }

  @Override
  public Geometry clip(Bounds bbox)
  {
    if (bbox.containsPoint(x, y))
    {
      return this;
    }
    return null;
  }

  @Override
  public boolean isEmpty()
  {
    return Double.isNaN(x) && Double.isNaN(y);
  }
  

}
