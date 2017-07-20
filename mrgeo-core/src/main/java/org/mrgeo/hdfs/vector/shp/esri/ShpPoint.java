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

import org.mrgeo.hdfs.vector.shp.esri.geom.JPoint;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;
import org.mrgeo.hdfs.vector.shp.util.Convert;


public class ShpPoint extends ShpShape
{

/**
 * Creates new ShpPoint
 */
public ShpPoint(int initialSize)
{
  p = new JPoint[initialSize];
}

@Override
public void addShape(JShape obj) throws FormatException
{
  if (obj instanceof JPoint)
  {
    super.addShape(obj);
  }
}

@Override
public void load(int i, byte[] record)
{
  Convert.getLEInteger(record, 0); // int shapeType =
  double px = Convert.getLEDouble(record, 4);
  double py = Convert.getLEDouble(record, 12);
  p[i] = new JPoint(px, py);
  p[i].setId(i);
}


@Override
public byte[] save(int i)
{
  JPoint pt = (JPoint)p[i];

  byte[] record = new byte[pt.getRecordLength()];
  Convert.setLEInteger(record, 0, JShape.POINT);
  Convert.setLEDouble(record, 4, pt.getX());
  Convert.setLEDouble(record, 12, pt.getY());
  return record;
}

}
