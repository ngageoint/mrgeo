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

package org.mrgeo.geometry.splitter;

import java.util.Map;

public class GeometrySplitterFactory
{
  public static final String OUTPUT_NAME_PREFIX = ".outputName.";

  /**
   * Create a new geometry splitter instance based on the splitterName passed
   * in and the splitterProperties (for configuring it). If outputNames is
   * null, then the splitter output names are created (using generated UUIDs
   * if uuiOutputNames is true or some other unique name that each type of
   * splitter determines for itself). If outputNames is non-null, it is used
   * as the output names for the splitter.
   * 
   * @param splitterName
   * @param splitterProperties
   * @param uuidOutputNames
   * @param outputNames
   * @return
   */
  public static GeometrySplitter createGeometrySplitter(final String splitterName,
      final Map<String, String> splitterProperties, final boolean uuidOutputNames,
      final String[] outputNames)
  {
    GeometrySplitter splitter = null;
    if (splitterName.equals("timeSpan"))
    {
      splitter = new TimeSpanGeometrySplitter();
    }

    if (splitter != null)
    {
      splitter.initialize(splitterProperties, uuidOutputNames, outputNames);
      return splitter;
    }
    else
    {
      throw new IllegalArgumentException("Invalid geometry splitter name: " + splitterName);
    }
  }
}
