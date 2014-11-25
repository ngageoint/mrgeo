/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.esri;

import org.mrgeo.data.shp.esri.geom.JPointZ;
import org.mrgeo.data.shp.esri.geom.JShape;
import org.mrgeo.data.shp.util.Convert;

import java.io.IOException;


public class ShpPointZ extends java.lang.Object implements ShpData
{
  protected JPointZ[] p;
  private ESRILayer parent;

  /** Creates new ShpPoint */
  public ShpPointZ(int initialSize)
  {
    p = new JPointZ[initialSize];
  }

  @Override
  public void addShape(JShape obj) throws FormatException
  {
    JPointZ[] temp = new JPointZ[p.length + 1];
    System.arraycopy(p, 0, temp, 0, p.length);
    temp[p.length] = (JPointZ) obj;
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
