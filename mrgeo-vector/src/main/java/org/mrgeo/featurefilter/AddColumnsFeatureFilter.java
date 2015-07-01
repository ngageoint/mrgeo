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

package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;

import java.util.Collection;
import java.util.List;

public class AddColumnsFeatureFilter extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;
  Collection<String> columns;




  /**
   * Use this constructor if the new columns are all the same type and their
   * ordering is not important.
   * 
   * @param columns
   */
  public AddColumnsFeatureFilter(Collection<String> columns)
  {
    this.columns = columns;
  }

  @Override
  public Geometry filterInPlace(Geometry input)
  {

    WritableGeometry result = input.createWritableClone();

    for (String attr: columns)
    {
      result.setAttribute(attr, null);
    }

    return result;
  }
}
