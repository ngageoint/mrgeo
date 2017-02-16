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

package org.mrgeo.geometry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@SuppressFBWarnings(value = "DESERIALIZATION_GADGET", justification = "verified read/writeObject")
public class Reprojector implements PointFilter
{
private static final long serialVersionUID = 1L;
transient CoordinateTransformation coordinateTransformation;
private String srcSRSWKT = null;
private String dstSRSWKT = null;

private Reprojector(SpatialReference sourceSrs, SpatialReference destSrs)
{
  coordinateTransformation = new CoordinateTransformation(sourceSrs, destSrs);

  srcSRSWKT = sourceSrs.ExportToWkt();
  dstSRSWKT = sourceSrs.ExportToWkt();
}

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
  if (index >= 0)
  {
    try
    {
      return Integer.parseInt(epsg.substring(index + prefix.length()));
    }
    catch (NumberFormatException nfe)
    {
    }
  }
  throw new IllegalArgumentException("Invalid EPSG code: " + epsg);
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

private void writeObject(ObjectOutputStream out) throws IOException
{
  out.writeUTF(srcSRSWKT);
  out.writeUTF(dstSRSWKT);
}

private void readObject(ObjectInputStream in) throws IOException
{
  srcSRSWKT = in.readUTF();
  dstSRSWKT = in.readUTF();

  SpatialReference srcSRS = new SpatialReference();
  srcSRS.ImportFromWkt(srcSRSWKT);
  srcSRS.MorphFromESRI();
  SpatialReference dstSRS = new SpatialReference();
  dstSRS.ImportFromWkt(dstSRSWKT);
  dstSRS.MorphFromESRI();

  coordinateTransformation = new CoordinateTransformation(srcSRS, dstSRS);
}


}
