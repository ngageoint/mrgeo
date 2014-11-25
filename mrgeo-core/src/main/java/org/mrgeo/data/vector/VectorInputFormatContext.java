/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.data.vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Set;

public class VectorInputFormatContext
{
  private static final String className = VectorInputFormatContext.class.getSimpleName();
  private static final String INPUTS = className + ".inputs";

  // TODO: Might need to include properties for spatial filtering
  // here - like a geometry collection. We could also add a flag for
  // inclusive/exclusive. Or we could make this extensive to support
  // lots of different spatial filtering, like intersects, colinear,
  // touches, disjoint, overlaps, contains, etc... Searching wikipedia
  // for "Spatial query" gives a pretty good list.
  //
  // TODO: Also should consider properties for attribute filtering.

  private Set<String> inputs;

  public VectorInputFormatContext(final Set<String> inputs)
  {
    this.inputs = inputs;
  }

  protected VectorInputFormatContext()
  {
  }

  public Set<String> getInputs()
  {
    return inputs;
  }

  public void save(final Configuration conf)
  {
    conf.set(INPUTS, StringUtils.join(inputs, ","));
  }

  public static VectorInputFormatContext load(final Configuration conf)
  {
    VectorInputFormatContext context = new VectorInputFormatContext();
    context.inputs = new HashSet<String>();
    String strInputs = conf.get(INPUTS);
    if (strInputs != null)
    {
      String[] confInputs = strInputs.split(",");
      for (String confInput : confInputs)
      {
        context.inputs.add(confInput);
      }
    }
    return context;
  }
}
