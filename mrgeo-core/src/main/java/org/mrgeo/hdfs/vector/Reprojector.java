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

package org.mrgeo.hdfs.vector;

import org.gdal.gdal.gdal;
import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.gdal.osr.osr;
import org.mrgeo.geometry.PointFilter;
import org.mrgeo.geometry.WritablePoint;

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
    SpatialReference sourceSrs = new SpatialReference();
    int sourceEpsg = parseEpsgCode(codeSrc);
    sourceSrs.ImportFromEPSG(sourceEpsg);
    SpatialReference destSrs = new SpatialReference();
    int destEpsg = parseEpsgCode(codeDest);
    destSrs.ImportFromEPSG(destEpsg);

    return new Reprojector(sourceSrs, destSrs);
  }

  /**
   * Creates a Reprojector from a source and destination WKT projection. to
   * "EPSG:4326".
   * 
   * @param wktSrc
   * @param wktDest
   * @return
   */
  public static Reprojector createFromWkt(String wktSrc, String wktDest)
  {
    SpatialReference sourceSrs = new SpatialReference();
    sourceSrs.ImportFromWkt(wktSrc);
    sourceSrs.MorphFromESRI();
    SpatialReference destSrs = new SpatialReference();
    destSrs.ImportFromWkt(wktDest);
    destSrs.MorphFromESRI();

    return new Reprojector(sourceSrs, destSrs);
  }

  private static int parseEpsgCode(String epsg)
  {
    String prefix = "epsg:";
    int index = epsg.toLowerCase().indexOf(prefix);
    if (index >= 0) {
      try
      {
        return Integer.parseInt(epsg.substring(index + prefix.length()));
      }
      catch(NumberFormatException nfe)
      {
      }
    }
    throw new IllegalArgumentException("Invalid EPSG code: " + epsg);
  }

  CoordinateTransformation coordinateTransformation;

  private Reprojector(SpatialReference sourceSrs, SpatialReference destSrs)
  {
    coordinateTransformation = osr.CreateCoordinateTransformation(sourceSrs, destSrs);
    if (coordinateTransformation == null)
    {
      throw new IllegalArgumentException("Cannot perform transformation: " + gdal.GetLastErrorMsg());
    }
  }

  @Override
  public void filter(WritablePoint p)
  {
    transform(p);
  }

  private void transform(WritablePoint p)
  {
    double[] transformed = coordinateTransformation.TransformPoint(p.getX(), p.getY());
    p.setX(transformed[0]);
    p.setY(transformed[1]);
  }
}
