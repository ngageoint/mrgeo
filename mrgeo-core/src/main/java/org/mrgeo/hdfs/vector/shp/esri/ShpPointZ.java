/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.vector.shp.esri;

import org.mrgeo.hdfs.vector.shp.esri.geom.JPointZ;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;
import org.mrgeo.hdfs.vector.shp.util.Convert;

import java.io.IOException;


public class ShpPointZ extends ShpShape
{

/**
 * Creates new ShpPoint
 */
public ShpPointZ(int initialSize)
{
  p = new JPointZ[initialSize];
}

@Override
public void addShape(JShape obj) throws FormatException
{
  if (obj instanceof JPointZ)
  {
    super.addShape(obj);
  }
}


@Override
public void load(int i, byte[] record)
{
  double px = Convert.getLEDouble(record, 4);
  double py = Convert.getLEDouble(record, 12);
  double z = Convert.getLEDouble(record, 20);
  double m = Convert.getLEDouble(record, 28);
  p[i] = new JPointZ(px, py, z, m);
  p[i].setId(i);
}

@Override
public void resizeCache(int size)
{
  p = new JPointZ[size];
}

@Override
public byte[] save(int i)
{
  JPointZ pt = (JPointZ)p[i];

  byte[] record = new byte[p[i].getRecordLength()];
  Convert.setLEInteger(record, 0, JShape.POINTZ);
  Convert.setLEDouble(record, 4, pt.getX());
  Convert.setLEDouble(record, 12, pt.getY());
  Convert.setLEDouble(record, 20, pt.getZ());
  Convert.setLEDouble(record, 28, pt.getM());
  return record;
}

}
