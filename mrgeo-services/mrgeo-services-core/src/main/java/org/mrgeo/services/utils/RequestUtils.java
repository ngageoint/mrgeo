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

package org.mrgeo.services.utils;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.mrgeo.utils.Bounds;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.vividsolutions.jts.geom.Envelope;

/**
 * General utilities for dealing with WMS requests
 */
public class RequestUtils
{
  private static final Logger log = LoggerFactory.getLogger(RequestUtils.class);

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
     * @throws FactoryException
     * @throws TransformException
     * @throws NoSuchAuthorityCodeException
     */
  public static Bounds reprojectBounds(final Bounds bounds, final String epsg) throws NoSuchAuthorityCodeException, TransformException, FactoryException
  {
      Bounds output = bounds.clone();
      //If SRS requires reprojection, adjust the input bounds here to Geographic EPSG:4326
      if (epsg != null && !(epsg.equalsIgnoreCase("EPSG:4326"))) {
          Envelope prjBounds = JTS.toGeographic(bounds.toEnvelope(), CRS.decode(epsg, true));
          output = new Bounds(prjBounds.getMinX(), prjBounds.getMinY(), prjBounds.getMaxX(), prjBounds.getMaxY());
      }
      return output;
  }
}