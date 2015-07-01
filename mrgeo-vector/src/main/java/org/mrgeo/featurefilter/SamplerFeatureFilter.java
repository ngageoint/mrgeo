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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplerFeatureFilter extends BaseFeatureFilter
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(SamplerFeatureFilter.class);

  private static final long serialVersionUID = 1L;
  String _uidColumn;
  double _samplePortion;
  
  /**
   * 
   * @param uidColumn - Column that contains a unique identifier
   * @param samplePortion - Percentage of values to keep -- 0 to 1.
   */
  public SamplerFeatureFilter(String uidColumn, double samplePortion)
  {
    _uidColumn = uidColumn;
    _samplePortion = samplePortion;
  }

  @Override
  public Geometry filterInPlace(Geometry g)
  {
    Geometry result = null;
    double h = Math.abs(g.getAttribute(_uidColumn).hashCode()) % 1000 / (1000.0);
    if (h < _samplePortion)
    {
      result = g;
    }
    return result;
  }
}
