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

package org.mrgeo.data.vector;


import org.mrgeo.utils.tms.Bounds;

import java.util.ArrayList;
import java.util.List;

public class VectorMetadata
{
private List<String> attributes = new ArrayList<String>();
private Bounds bounds;

public VectorMetadata()
{
}

public Bounds getBounds()
{
  return bounds;
}

public void setBounds(final Bounds bounds)
{
  this.bounds = bounds;
}

/**
 * Attributes should not include the attribute that stores geometry.
 *
 * @param attribute
 */
public void addAttribute(String attribute)
{
  if (!hasAttribute(attribute))
  {
    attributes.add(attribute);
  }
}

public boolean hasAttribute(final String fieldName)
{
  return attributes.contains(fieldName);
}

public String[] getAttributes()
{
  String[] result = new String[attributes.size()];
  return attributes.toArray(result);
}
}
