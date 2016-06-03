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

package org.mrgeo.hdfs.vector.shp.esri.geom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public abstract class JShape implements Serializable
{
public final static byte EDIT = 3; // the shape is being edited but can be
// drawn
public final static byte ERROR = 2; // the shape cannot be displayed
// (topologically inconsistent)
// based on ESRI header standard
public final static byte NULL = 0;
public final static byte POINT = 1;
public final static byte POINTZ = 11;
public final static byte POLYGON = 5;
public final static byte POLYGONZ = 15;
public final static byte POLYLINE = 3;
public final static byte POLYLINEZ = 13;
public final static byte READY = 1; // the shape is ready (no topological
// errors)
public final static byte SELECTED = 4; // the shape is selected and ready
static final long serialVersionUID = 1L;
// status flags
public final static byte UNKNOWN = 0;

public static String getTypeLiteral(byte type)
{
  switch (type)
  {
  case NULL:
    return "NULL";
  case POINT:
    return "POINT";
  case POLYLINE:
    return "POLYLINE";
  case POLYGON:
    return "POLYGON";
  case POINTZ:
    return "POINTZ";
  case POLYLINEZ:
    return "POLYLINEZ";
  case POLYGONZ:
    return "POLYGONZ";
  default:
    return "UNKNOWN";
  }
}

@SuppressWarnings("rawtypes")
protected List data; // data list
protected JExtent extent; // the extent of the shape
protected int id; // shape id
protected byte status; // status flag

// class vars
protected byte type; // shape type

/** Creates new JShape */
public JShape(byte type)
{
  this.type = type;
  extent = null;
  id = 0;
  status = UNKNOWN;
  data = null;
}

@SuppressWarnings("rawtypes")
public void addData(List v)
{
  addData(v, false);
}

@SuppressWarnings("rawtypes")
public void addData(List v, boolean clear)
{
  if (v == null)
    return;
  if (data == null)
  {
    data = new ArrayList(v.size());
  }
  else
  {
    if (clear)
      data.clear();
  }
  for (int i = 0; i < v.size(); i++)
  {
    try
    {
      addData((Serializable) v.get(i));
    }
    catch (Exception e)
    {
      addData((Serializable) null);
    }
  }
}

@SuppressWarnings("rawtypes")
public void addData(Serializable obj)
{
  if (obj == null)
    return;
  if (data == null)
    data = new ArrayList(1);
  data.add(obj);
}

public final byte check()
{
  return check(false);
}

public abstract byte check(boolean clean); // check shape status

public void clrData()
{
  data.clear();
  data = null;
}

public void debug()
{
  if (extent != null)
    System.out.println("Extent: " + extent.toString());
}

@SuppressWarnings("rawtypes")
public List getData()
{
  return data;
}

public Serializable getData(int i)
{
  try
  {
    return (Serializable) data.get(i);
  }
  catch (Exception e)
  {
    return null;
  }
}

public JExtent getExtent()
{
  return extent;
}

public final int getId()
{
  return id;
}

public abstract int getRecordLength();

public final byte getStatus()
{
  return status;
}

public final byte getType()
{
  return type;
}

public boolean intersects(JExtent other)
{
  return JExtent.intersects(extent, other);
}

public boolean isSelected()
{
  if (status == SELECTED)
    return true;
  return false;
}

public void remData(int i)
{
  data.remove(i);
}

public void remData(Serializable obj)
{
  data.remove(obj);
}

public void setData(Serializable obj, int i)
{
  if (obj == null)
    return;
  if (data == null)
    return;
  data.set(i, obj);
}

@SuppressWarnings("rawtypes")
public void setDataReference(List data)
{
  this.data = data;
}

protected final void setExtent(JExtent extent)
{
  this.extent = extent;
}

public final void setId(int id)
{
  this.id = id;
}

public void setSelected(boolean flag)
{
  if (flag)
  {
    if (status == READY)
      status = SELECTED;
  }
  else
  {
    if (status == SELECTED)
      status = READY;
  }
}

@Override
public String toString()
{
  return extent.toString();
}

public abstract void updateExtent();
}
