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

package org.mrgeo.geometry;

import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class WellKnownProjections
{
  public final static String WGS84;

  static
  {
    CoordinateReferenceSystem crs;
    try
    {
      crs = CRS.decode("EPSG:4326");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException("Do you have all the right JARs?", e);
    }
    WGS84 = crs.toWKT();
  }
}
