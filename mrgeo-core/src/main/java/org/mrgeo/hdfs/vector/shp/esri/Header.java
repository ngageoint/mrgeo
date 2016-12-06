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

package org.mrgeo.hdfs.vector.shp.esri;

import org.mrgeo.hdfs.vector.shp.SeekableDataInput;
import org.mrgeo.hdfs.vector.shp.esri.geom.JExtent;
import org.mrgeo.hdfs.vector.shp.util.Convert;

import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * TODO Looks like there is a bit of redundant and unnecessary code in here.
 * Could use some refactoring.
 */
public class Header
{
protected JExtent extent;
// shape types
protected int fileCode; // should be 9994
protected int fileLength; // # in 2 byte words
protected double mmax; // 0.0 if unused
protected double mmin; // 0.0 if unused
protected int shapeType;
protected int version; // should be 1000
protected double zmax; // 0.0 if unused
protected double zmin; // 0.0 if unused

/**
 * Creates new Header
 */
protected Header()
{
}

public JExtent getExtentOnFile(SeekableDataInput in) throws IOException
{
  byte[] header = new byte[32];
  long current = in.getPos();
  in.seek(36);
  in.readFully(header, 0, 32);
  in.seek(current);
  // evaluate
  double Xmin = Convert.getLEDouble(header, 0);
  double Ymin = Convert.getLEDouble(header, 8);
  double Xmax = Convert.getLEDouble(header, 16);
  double Ymax = Convert.getLEDouble(header, 24);
  // return
  return new JExtent(Xmin, Ymin, Xmax, Ymax);
}

public void setExtentOnFile(RandomAccessFile os, JExtent extent) throws IOException
{
  byte[] header = new byte[32];
  // evaluate
  Convert.setLEDouble(header, 0, extent.getMinX());
  Convert.setLEDouble(header, 8, extent.getMinY());
  Convert.setLEDouble(header, 16, extent.getMaxX());
  Convert.setLEDouble(header, 24, extent.getMaxY());
  // write
  long current = os.getFilePointer();
  os.seek(36);
  os.write(header, 0, 32);
  os.seek(current);
}

@Override
public String toString()
{
  String s = "";
  s = s + "fileCode: " + fileCode + "\n";
  s = s + "fileLength: " + fileLength + "\n";
  s = s + "version: " + version + "\n";
  s = s + "shapeType: " + shapeType + "\n";
  s = s + "extent: " + extent + "\n";
  s = s + "z min,max: " + zmin + "," + zmax + "\n";
  s = s + "m min,max: " + mmin + "," + mmax + "\n";
  return s;
}

protected void load(SeekableDataInput is) throws IOException, FormatException
{
  byte[] header = new byte[100];
  is.readFully(header, 0, 100);
  // core data
  fileCode = Convert.getInteger(header, 0);
  if (fileCode != 9994)
  {
    throw new FormatException("Invalid Header File Code!");
  }
  fileLength = Convert.getInteger(header, 24); // in 16-bit words
  version = Convert.getLEInteger(header, 28);
  if (version != 1000)
  {
    throw new FormatException("Invalid Header Version!");
  }
  shapeType = Convert.getLEInteger(header, 32);
  // header bounding box
  double Xmin = Convert.getLEDouble(header, 36);
  double Ymin = Convert.getLEDouble(header, 44);
  double Xmax = Convert.getLEDouble(header, 52);
  double Ymax = Convert.getLEDouble(header, 60);
  zmin = Convert.getLEDouble(header, 68);
  zmax = Convert.getLEDouble(header, 76);
  mmin = Convert.getLEDouble(header, 84);
  mmax = Convert.getLEDouble(header, 92);
  extent = new JExtent(Xmin, Ymin, Xmax, Ymax);
}

protected void save(RandomAccessFile os) throws IOException
{
  byte[] header = new byte[100];
  // core data
  Convert.setInteger(header, 0, 9994);
  Convert.setInteger(header, 24, fileLength);
  Convert.setLEInteger(header, 28, 1000);
  Convert.setLEInteger(header, 32, shapeType);
  // check header
  if (extent == null)
  {
    extent = new JExtent();
  }
  // header bounding box
  Convert.setLEDouble(header, 36, extent.getMinX());
  Convert.setLEDouble(header, 44, extent.getMinY());
  Convert.setLEDouble(header, 52, extent.getMaxX());
  Convert.setLEDouble(header, 60, extent.getMaxY());
  // write
  os.write(header, 0, 100);
}
}
