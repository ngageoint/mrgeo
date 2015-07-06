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

import org.mrgeo.geometry.Geometry;

import java.util.List;
import java.util.Map;

/**
 * This interface is used when we want to split Geometry objects
 * into categories. A specific use case is when we want to split
 * Geometry objects across multiple output directories based on
 * some criteria within the Geometry itself.
 * 
 * Implementors of this interface will be configured with enough
 * information to perform a split on any given Geometry and return
 * a list of categories to split it into.
 */
public interface GeometrySplitter
{
  public void initialize(Map<String, String> splitterProperties, final boolean uuidOutputNames,
      final String[] outputNames);
  public String[] getAllSplits();
  public List<String> splitGeometry(final Geometry geometry);
}
