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
package org.mrgeo.geometryfilter;

import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.mrgeo.geometry.PointFilter;
import org.mrgeo.geometry.WritablePoint;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class Reprojector implements PointFilter
{
  private static final long serialVersionUID = 1L;

  /**
   * Creates a Reprojector from a specified code to another code. The codes take
   * a form similar to "EPSG:4326".
   * 
   * @param codeSrc
   * @param codeDest
   * @return
   */
  public static Reprojector createFromCode(String codeSrc, String codeDest)
  {
    try
    {
      CoordinateReferenceSystem sourceCrs = CRS.decode(codeSrc);
      CoordinateReferenceSystem destCrs = CRS.decode(codeDest);

      return new Reprojector(sourceCrs, destCrs);
    }
    catch (NoSuchAuthorityCodeException e)
    {
      throw new IllegalArgumentException("Error decoding projection - authority code not found", e);
    }
    catch (FactoryException e)
    {
      e.printStackTrace();
      throw new RuntimeException("Do you have all the necessary jars?", e);
    }
  }

  /**
   * Creates a Reprojector from a source and destination WKT projection. to
   * "EPSG:4326".
   * 
   * @param codeSrc
   * @param codeDest
   * @return
   */
  public static Reprojector createFromWkt(String wktSrc, String wktDest)
  {
    try
    {
      CoordinateReferenceSystem sourceCrs = CRS.parseWKT(wktSrc);
      CoordinateReferenceSystem destCrs = CRS.parseWKT(wktDest);

      return new Reprojector(sourceCrs, destCrs);
    }
    catch (FactoryException e)
    {
      e.printStackTrace();
      throw new RuntimeException("Do you have all the necessary jars?", e);
    }
  }

  MathTransform mathTransform;

  private boolean swapInputXy = false;

  private boolean swapOutputXy = false;

  private Reprojector(CoordinateReferenceSystem sourceCrs, CoordinateReferenceSystem destCrs)
      throws FactoryException
  {
    // if the projection is in lat/lng then GeoTools tries to be fancy and
    // stores the values
    // as y, x. Swap them back to rational ordering.
    if (sourceCrs.getCoordinateSystem().getAxis(0).getAbbreviation().compareTo("φ") == 0)
    {
      swapInputXy = true;
    }
    if (destCrs.getCoordinateSystem().getAxis(0).getAbbreviation().compareTo("φ") == 0)
    {
      swapOutputXy = true;
    }
    // create a lenient transform. This means that if the
    // "Bursa-Wolf parameters" are not
    // available a less accurate transform will be performed.
    mathTransform = CRS.findMathTransform(sourceCrs, destCrs, true);
  }

  @Override
  public void filter(WritablePoint p)
  {
    try
    {
      transform(p);
    }
    catch (TransformException e)
    {
      throw new IllegalArgumentException("Error tranforming", e);
    }
  }

  public void transform(WritablePoint p) throws TransformException
  {
    double x;
    double y;
    if (swapInputXy)
    {
      y = p.getX();
      x = p.getY();
    }
    else
    {
      x = p.getX();
      y = p.getY();
    }
    Coordinate source = new Coordinate(x, y);
    Coordinate dest = new Coordinate();

    JTS.transform(source, dest, mathTransform);

    if (swapOutputXy)
    {
      p.setX(dest.y);
      p.setY(dest.x);
    }
    else
    {
      p.setX(dest.x);
      p.setY(dest.y);
    }
  }
}
