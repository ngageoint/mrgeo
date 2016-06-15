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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author jason.surratt
 * 
 */
public interface Geometry extends Serializable
{
  enum Type {
    POINT, LINESTRING, LINEARRING, POLYGON, COLLECTION
  }

  public WritableGeometry createWritableClone();
  public WritableGeometry asWritable();

  public Map<String, String> getAllAttributes();
  public TreeMap<String, String> getAllAttributesSorted();
  public String getAttribute(String key);
  public boolean hasAttribute(String key);
  public boolean hasAttribute(String key, String value);

  public Bounds getBounds();

  public boolean isValid();
  public boolean isEmpty();

  public com.vividsolutions.jts.geom.Geometry toJTS();

  public Type type();

  public Geometry clip(Bounds bbox);
  public Geometry clip(Polygon geom);
  
  public void write(DataOutputStream stream) throws IOException;
  public void writeAttributes(DataOutputStream stream) throws IOException;
}
