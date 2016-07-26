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

package org.mrgeo.services.utils;

import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General utilities for dealing with WMS requests
 */
public class RequestUtils
{
  private static final Logger log = LoggerFactory.getLogger(RequestUtils.class);

  {
    GDALUtils.register();
  }

  /**
   * Parses a geographic bounds from a request parameter value
   *
   * @param param
   *          request parameter value
   * @return geographic bounds
   */
  public static Bounds boundsFromParam(final String param)
  {
    if (param == null)
    {
      throw new IllegalArgumentException("Bounding box must be specified.");
    }
    log.debug("incoming bounds request: " + param);
    final String[] bBoxValues = param.split(",");
    if (bBoxValues.length != 4)
    {
      throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
    }
    return new Bounds(Double.valueOf(bBoxValues[0]), Double.valueOf(bBoxValues[1]),
            Double.valueOf(bBoxValues[2]), Double.valueOf(bBoxValues[3]));
  }
  /**
   * Reprojects a bounds to Geographic
   *
   * @param bounds
   *            the projected input bounds
   * @param epsg
   *            the epsg string of the projected bounds crs
   * @return geographic bounds
   */
  public static Bounds reprojectBounds(final Bounds bounds, final String epsg)
  {
    if (epsg != null && !(epsg.equalsIgnoreCase("EPSG:4326")))
    {

      SpatialReference src = new SpatialReference(GDALUtils.EPSG4326());
      SpatialReference dst = new SpatialReference();
      String[] code = epsg.split(":");
      dst.ImportFromEPSG(Integer.parseInt(code[1]));

      CoordinateTransformation tx = new CoordinateTransformation(src, dst);

      double[] c1;
      double[] c2;
      double[] c3;
      double[] c4;

      c1 = tx.TransformPoint(bounds.w, bounds.s);
      c2 = tx.TransformPoint(bounds.w, bounds.n);
      c3 = tx.TransformPoint(bounds.e, bounds.s);
      c4 = tx.TransformPoint(bounds.e, bounds.n);

      return new Bounds(Math.min(Math.min(c1[0], c2[0]), Math.min(c3[0], c4[0])),
              Math.min(Math.min(c1[1], c2[1]), Math.min(c3[1], c4[1])),
              Math.max(Math.max(c1[0], c2[0]), Math.max(c3[0], c4[0])),
              Math.max(Math.max(c1[1], c2[1]), Math.max(c3[1], c4[1])));
    }
    else
    {
      return bounds.clone();
    }
  }
}