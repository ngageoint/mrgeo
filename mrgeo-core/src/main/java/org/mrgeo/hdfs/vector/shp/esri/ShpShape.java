package org.mrgeo.hdfs.vector.shp.esri;

import org.mrgeo.hdfs.vector.shp.esri.geom.JPoint;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;

import java.io.IOException;

public class ShpShape implements ShpData
{
protected JShape[] p;
private ESRILayer parent;

@Override
public void addShape(JShape obj) throws FormatException
{
  JShape[] temp = p;

  resizeCache(p.length + 1);

  System.arraycopy(temp, 0, p, 0, temp.length);
  p[p.length - 1] = obj;
}

@Override
public int getCount()
{
  if (parent != null && parent.index != null)
  {
    return parent.index.recordCount;
  }
  return 0;
}

@Override
public JShape getShape(int i) throws IOException
{
  try
  {
    if (parent != null && parent.index != null)
    {

      if (i < parent.index.getCachePos()
          || i > (parent.index.getCachePos() + parent.index.getCurrentCacheSize() - 1))
      {
        if (parent.table.isModified())
        {
          parent.table.save();
        }

        // load
        parent.index.loadData(i);
        parent.shape.loadData(i);

        // set data references
        for (int j = 0; j < p.length; j++)
        {
          p[j].setDataReference(parent.table.getRow(j + parent.index.getCachePos()));
        }
      }
      return p[i - parent.index.getCachePos()];
    }
  }
  catch (FormatException e)
  {
    throw new RuntimeException(e);
  }

  throw new RuntimeException("Unknown Error");
}

@Override
public void load(int i, byte[] record)
{

}

@Override
public void resizeCache(int size)
{

}

@Override
public byte[] save(int i)
{
  return new byte[0];
}

@Override
public void setParent(ESRILayer parent)
{
  this.parent = parent;
}

}
