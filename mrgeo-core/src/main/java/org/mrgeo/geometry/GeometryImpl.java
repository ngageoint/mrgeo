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

import org.mrgeo.utils.GeometryUtils;
import org.mrgeo.utils.StringUtils;
import org.mrgeo.utils.tms.Bounds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public abstract class GeometryImpl implements WritableGeometry
{
Map<String, String> attributes = new HashMap<>();

Bounds bounds = null;

public static Class[] getClasses()
{
  return new Class[]{GeometryImpl.class, HashMap.class, Bounds.class};
}

@Override
public String toString()
{
  StringBuilder result = new StringBuilder();
  result.append(WktConverter.toWkt(this));
  result.append(" {");
  String sep = "";
  for (String key : getAllAttributesSorted().keySet())
  {
    result.append(sep);
    result.append(key);
    result.append(":\"");
    result.append(getAttribute(key));
    result.append("\"");
    sep = ", ";
  }
  result.append("}");
  return result.toString();
}

@Override
public WritableGeometry asWritable()
{
  return this;
}

@Override
public Map<String, String> getAllAttributes()
{
  return attributes;
}

@Override
public TreeMap<String, String> getAllAttributesSorted()
{
  return new TreeMap<>(attributes);
}

@Override
public void setAttribute(String key, String value)
{
  attributes.put(key, value);
}
@Override
public void setAttributes(Map<String, String> attrs)
{
  attributes = attrs;
}

@Override
public String getAttribute(String key)
{
  return attributes.get(key);
}

@Override
public boolean hasAttribute(String key)
{
  return attributes.containsKey(key);
}

@Override
public boolean hasAttribute(String key, String value)
{
  String attr = attributes.get(key);

  return attr != null && attr.equals(value);
}

@Override
public Geometry clip(Bounds bbox)
{
  return clip(GeometryUtils.toPoly(bbox));
}

@Override
public Geometry clip(Polygon geom)
{
  return GeometryUtils.clip(this, geom);
}

@Override
public void writeAttributes(DataOutputStream stream) throws IOException
{
  stream.writeInt(attributes.size());
  for (Map.Entry<String, String> attr: attributes.entrySet())
  {
    stream.writeUTF(attr.getKey());
    // attr can be larger than 64K, so we have to use the alternate write method
    StringUtils.write(attr.getValue(), stream);
  }
}

@Override
public void readAttributes(DataInputStream stream) throws IOException
{

  int attrs = stream.readInt();
  for (int i = 0; i < attrs; i++)
  {
    String key = stream.readUTF();
    String value = StringUtils.read(stream);

    attributes.put(key, value);
  }
}

}
