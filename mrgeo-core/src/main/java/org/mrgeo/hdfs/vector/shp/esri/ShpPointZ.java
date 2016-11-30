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

import org.mrgeo.hdfs.vector.shp.esri.geom.JPointZ;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;
import org.mrgeo.hdfs.vector.shp.util.Convert;

import java.io.IOException;


public class ShpPointZ implements ShpData
{
  protected JPointZ[] p;
  private ESRILayer parent = null;

  /** Creates new ShpPoint */
  public ShpPointZ(int initialSize)
  {
    p = new JPointZ[initialSize];
  }

  @Override
  public void addShape(JShape obj) throws FormatException
  {
    if (obj instanceof JPointZ)
    {
      JPointZ[] temp = new JPointZ[p.length + 1];
      System.arraycopy(p, 0, temp, 0, p.length);
      temp[p.length] = (JPointZ) obj;
      p = temp;
    }
  }

  @Override
  public int getCount()
  {
    return parent.index.recordCount;
  }

  @Override
  @SuppressWarnings("squid:S00112") // I didn't write this code, so I'm not sure why it throws the RuntimeException.  Keeping it
  public JShape getShape(int i) throws IOException
  {
    try
    {
      if (i < parent.index.getCachePos()
          || i > (parent.index.getCachePos() + parent.index.getCurrentCacheSize() - 1))
      {
        // save if necessary
        if (parent.index.modData)
          parent.save();
        if (parent.table.isModified())
          parent.table.save();
        // load
        parent.index.loadData(i);
        parent.shape.loadData(i);
        // set data references
        for (int j = 0; j < p.length; j++)
        {
          JShape obj = p[j];
          obj.setDataReference(parent.table.getRow(j + parent.index.getCachePos()));
        }
      }
      return p[i - parent.index.getCachePos()];
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
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
    byte[] record = new byte[p[i].getRecordLength()];
    Convert.setLEInteger(record, 0, JShape.POINTZ);
    Convert.setLEDouble(record, 4, p[i].getX());
    Convert.setLEDouble(record, 12, p[i].getY());
    Convert.setLEDouble(record, 20, p[i].getZ());
    Convert.setLEDouble(record, 28, p[i].getM());
    return record;
  }

  @Override
  public void setParent(ESRILayer parent)
  {
    this.parent = parent;
  }
}
