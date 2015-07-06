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

import java.util.ArrayList;

public class FeatureFilterChain extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;
  ArrayList<FeatureFilter> filterList = new ArrayList<FeatureFilter>();

  public FeatureFilterChain(FeatureFilter... filters)
  {
    for (FeatureFilter f : filters)
    {
      filterList.add(f);
    }
  }
  
  public void add(FeatureFilter filter)
  {
    filterList.add(filter);
  }

  @Override
  public Geometry filterInPlace(Geometry g)
  {
    Geometry result = g;
    
    for (FeatureFilter f : filterList)
    {
      if (result != null)
      {
        result = f.filterInPlace(result);
      }
    }

    return result;
  }
}
