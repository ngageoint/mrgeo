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

import org.mrgeo.hdfs.vector.shp.esri.geom.Coord;
import org.mrgeo.hdfs.vector.shp.esri.geom.JPolyLine;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;
import org.mrgeo.hdfs.vector.shp.util.Convert;

import java.io.IOException;


public class ShpPolyLine implements ShpData
{
  protected JPolyLine[] p;
  private ESRILayer parent = null;

  /** Creates new ShpLine */
  public ShpPolyLine(int initialSize)
  {
    p = new JPolyLine[initialSize];
  }

  @Override
  public void addShape(JShape obj) throws FormatException
  {
    if (obj instanceof JPolyLine)
    {
      JPolyLine[] temp = new JPolyLine[p.length + 1];
      System.arraycopy(p, 0, temp, 0, p.length);
      temp[p.length] = (JPolyLine) obj;
      p = temp;
    }
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
    int totParts = Convert.getLEInteger(record, 36);
    int totPoints = Convert.getLEInteger(record, 40);
    int[] startingPair = new int[totParts + 1];
    startingPair[totParts] = totPoints;
    for (int j = 0; j < totParts; j++)
    {
      startingPair[j] = Convert.getLEInteger(record, 44 + (j * 4));
    }
    p[i] = new JPolyLine();
    p[i].setId(i);
    for (int part = 0; part < totParts; part++)
    {
      for (int j = startingPair[part]; j < startingPair[part + 1]; j++)
      {
        double px = Convert.getLEDouble(record, 44 + (totParts * 4) + (j * 16));
        double py = Convert.getLEDouble(record, 44 + (totParts * 4) + (j * 16) + 8);
        if (j == startingPair[part])
        {
          // first point in part sequence
          p[i].add(new Coord(px, py), JPolyLine.NEW_PART);
        }
        else
        {
//          if (j == startingPair[part + 1] - 1)
//          {
//            // path not closed as in the case in ShpPolygon
//            p[i].add(new Coord(px, py));
//          }
//          else
          {
            p[i].add(new Coord(px, py));
          }
        }
      }
    }
    // check line
    p[i].check(true);
  }

  @Override
  public void resizeCache(int size)
  {
    p = new JPolyLine[size];
  }

  @Override
  public byte[] save(int i)
  {
    byte[] record = new byte[p[i].getRecordLength()];
    Convert.setLEInteger(record, 0, JShape.POLYLINE);
    Convert.setLEDouble(record, 4, p[i].getExtent().getMinX());
    Convert.setLEDouble(record, 12, p[i].getExtent().getMinY());
    Convert.setLEDouble(record, 20, p[i].getExtent().getMaxX());
    Convert.setLEDouble(record, 28, p[i].getExtent().getMaxY());
    int totParts = p[i].getPartCount();
    int totPoints = p[i].getPointCount();
    Convert.setLEInteger(record, 36, totParts);
    Convert.setLEInteger(record, 40, totPoints);
    for (int j = 0; j < totParts; j++)
    {
      Convert.setLEInteger(record, 44 + (j * 4), p[i].getPart(j));
    }
    for (int j = 0; j < totPoints; j++)
    {
      Coord c = p[i].getPoint(j);
      Convert.setLEDouble(record, 44 + (totParts * 4) + (j * 16), c.x);
      Convert.setLEDouble(record, 44 + (totParts * 4) + (j * 16) + 8, c.y);

    }
    return record;
  }

  @Override
  public void setParent(ESRILayer parent)
  {
    this.parent = parent;
  }
}
