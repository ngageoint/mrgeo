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

package org.mrgeo.data.shp.esri;

import org.mrgeo.data.shp.esri.geom.JPoint;
import org.mrgeo.data.shp.esri.geom.JShape;
import org.mrgeo.data.shp.util.Convert;

import java.io.IOException;


public class ShpPoint extends java.lang.Object implements ShpData
{
  protected JPoint[] p;
  private ESRILayer parent;

  /** Creates new ShpPoint */
  public ShpPoint(int initialSize)
  {
    p = new JPoint[initialSize];
  }

  @Override
  public void addShape(JShape obj) throws FormatException
  {
    JPoint[] temp = new JPoint[p.length + 1];
    System.arraycopy(p, 0, temp, 0, p.length);
    temp[p.length] = (JPoint) obj;
    p = temp;
  }

  @Override
  public int getCount()
  {
    return parent.index.recordCount;
  }

  @Override
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
    Convert.getLEInteger(record, 0); // int shapeType =
    double px = Convert.getLEDouble(record, 4);
    double py = Convert.getLEDouble(record, 12);
    p[i] = new JPoint(px, py);
    p[i].setId(i);
  }

  @Override
  public void resizeCache(int size)
  {
    p = new JPoint[size];
  }

  @Override
  public byte[] save(int i)
  {
    byte[] record = new byte[p[i].getRecordLength()];
    Convert.setLEInteger(record, 0, JShape.POINT);
    Convert.setLEDouble(record, 4, p[i].getX());
    Convert.setLEDouble(record, 12, p[i].getY());
    return record;
  }

  @Override
  public void setParent(ESRILayer parent)
  {
    this.parent = parent;
  }
}
