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

package org.mrgeo.geometry;


import org.mrgeo.utils.tms.Bounds;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author jason.surratt
 */
public interface Geometry extends Serializable
{
WritableGeometry createWritableClone();

WritableGeometry asWritable();

Map<String, String> getAllAttributes();

TreeMap<String, String> getAllAttributesSorted();

String getAttribute(String key);

boolean hasAttribute(String key);

boolean hasAttribute(String key, String value);

Bounds getBounds();

boolean isValid();

boolean isEmpty();

com.vividsolutions.jts.geom.Geometry toJTS();

Type type();

Geometry clip(Bounds bbox);

Geometry clip(Polygon geom);

void write(DataOutputStream stream) throws IOException;

void writeAttributes(DataOutputStream stream) throws IOException;

enum Type
{
  POINT, LINESTRING, LINEARRING, POLYGON, COLLECTION
}
}
