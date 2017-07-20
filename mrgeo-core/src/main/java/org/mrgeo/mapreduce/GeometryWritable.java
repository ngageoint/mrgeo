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

package org.mrgeo.mapreduce;

import org.apache.hadoop.io.Writable;
import org.mrgeo.geometry.*;
import org.mrgeo.utils.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Not to be confused with WritableGeometry the GeometryWritable class provides the ability to be
 * written using the Hadoop Writable interface.
 *
 * @author jason.surratt
 */
public class GeometryWritable implements Writable
{
private static final String CHAR_ENCODING = "UTF8";

// This is exposed for performance reasons. Be sure you understand all the
// implications before you change the value.
public Geometry geometry = null;

public GeometryWritable()
{

}

public GeometryWritable(final Geometry g)
{
  geometry = g;
}

private static void readAttributes(final DataInput in, final WritableGeometry g)
    throws IOException
{
  final int size = in.readInt();
  for (int i = 0; i < size; i++)
  {
    String key = in.readUTF();
    int attrByteLen = in.readInt();
    byte[] attrBytes = new byte[attrByteLen];
    in.readFully(attrBytes);
    String value = new String(attrBytes, CHAR_ENCODING);
    g.setAttribute(key, value);
  }
}

private static void readLineString(final DataInput in, final WritableLineString ls)
    throws IOException
{
  final int size = in.readInt();
  for (int i = 0; i < size; i++)
  {
    final WritablePoint p = GeometryFactory.createPoint();
    readPoint(in, p);
    ls.addPoint(p);
  }
}

private static void readPoint(final DataInput in, final WritablePoint result) throws IOException
{
  result.setX(in.readDouble());
  result.setY(in.readDouble());
  result.setZ(in.readDouble());
}

private static void readPolygon(final DataInput in, final WritablePolygon p) throws IOException
{
  WritableLinearRing lr = GeometryFactory.createLinearRing();
  readLineString(in, lr);
  p.setExteriorRing(lr);
  final int interiorSize = in.readInt();
  for (int i = 0; i < interiorSize; i++)
  {
    lr = GeometryFactory.createLinearRing();
    readLineString(in, lr);
    p.addInteriorRing(lr);
  }
}

private static void writeAttributes(final DataOutput out, final Geometry g) throws IOException
{
  out.writeInt(g.getAllAttributes().size());
  for (Map.Entry attr : g.getAllAttributesSorted().entrySet())
  {
    out.writeUTF(attr.getKey().toString());
    // Cannot use writeUTF for the value because there is a limit of 64K
    // characters that it will write to the stream, and geometry values can
    // get longer than that.
    StringUtils.write(attr.getValue().toString(), out);
//      out.writeUTF(attr.getValue().toString());
  }
}

private static void writeLineString(final DataOutput out, final LineString ls) throws IOException
{
  out.writeInt(ls.getNumPoints());
  for (int i = 0; i < ls.getNumPoints(); i++)
  {
    writePoint(out, ls.getPoint(i));
  }
}

private static void writePoint(final DataOutput out, final Point p) throws IOException
{
  out.writeDouble(p.getX());
  out.writeDouble(p.getY());
  out.writeDouble(p.getZ());
}

private static void writePolygon(final DataOutput out, final Polygon p) throws IOException
{
  writeLineString(out, p.getExteriorRing());
  out.writeInt(p.getNumInteriorRings());
  for (int i = 0; i < p.getNumInteriorRings(); i++)
  {
    writeLineString(out, p.getInteriorRing(i));
  }
}

public Geometry getGeometry()
{
  return geometry;
}

@Override
public void readFields(final DataInput in) throws IOException
{
  geometry = readGeometry(in);
  readAttributes(in, (WritableGeometry) geometry);
}

public void set(final WritableGeometry g)
{
  geometry = g;
}

@Override
public String toString()
{
  final StringBuilder result = new StringBuilder();
  result.append("[ ");
  final Map<String, String> attr = geometry.getAllAttributes();
  for (final String key : geometry.getAllAttributes().keySet())
  {
    result.append(key + ": " + attr.get(key));
    result.append(", ");
  }
  result.append("geom: " + WktConverter.toWkt(geometry));
  result.append(" ]");
  return result.toString();
}

@Override
public void write(final DataOutput out) throws IOException
{
  writeGeometry(out, geometry);
  writeAttributes(out, geometry);
}

private WritableGeometry readGeometry(final DataInput in) throws IOException
{
  WritableGeometry result;

  final Geometry.Type type = Geometry.Type.values()[in.readInt()];
  switch (type)
  {
  case COLLECTION:
    result = GeometryFactory.createGeometryCollection();
    readGeometryCollection(in, (WritableGeometryCollection) result);
    break;
  case LINEARRING:
    result = GeometryFactory.createLinearRing();
    readLineString(in, (WritableLineString) result);
    break;
  case LINESTRING:
    result = GeometryFactory.createLineString();
    readLineString(in, (WritableLineString) result);
    break;
  case POINT:
    result = GeometryFactory.createPoint();
    readPoint(in, (WritablePoint) result);
    break;
  case POLYGON:
    result = GeometryFactory.createPolygon();
    readPolygon(in, (WritablePolygon) result);
    break;
  default:
    throw new IllegalArgumentException("Unsupported geometry type");
  }

  if (result.isValid() == false)
  {
    System.out.println("invalid geom");
  }

  return result;
}

private void readGeometryCollection(final DataInput in, final WritableGeometryCollection g)
    throws IOException
{
  final int size = in.readInt();
  for (int i = 0; i < size; i++)
  {
    g.addGeometry(readGeometry(in));
  }
}

private void writeGeometry(final DataOutput out, final Geometry g) throws IOException
{
  out.writeInt(g.type().ordinal());
  if (g instanceof Point)
  {
    writePoint(out, (Point) g);
  }
  else if (g instanceof LinearRing)
  {
    writeLineString(out, (LineString) g);
  }
  else if (g instanceof LineString)
  {
    writeLineString(out, (LineString) g);
  }
  else if (g instanceof Polygon)
  {
    writePolygon(out, (Polygon) g);
  }
  else if (g instanceof GeometryCollection)
  {
    writeGeometryCollection(out, (GeometryCollection) g);
  }
  else
  {
    throw new IllegalArgumentException("Unsupported geometry type");
  }
}

private void writeGeometryCollection(final DataOutput out, final GeometryCollection gc)
    throws IOException
{
  out.writeInt(gc.getGeometries().size());
  for (final Geometry g : gc.getGeometries())
  {
    writeGeometry(out, g);
  }
}
}
