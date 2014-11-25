/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.esri.geom;

public class JNull extends JShape
{
  @SuppressWarnings("hiding")
  static final long serialVersionUID = 1L;

  /** Creates new JNull */
  public JNull()
  {
    super(NULL);
  }

  @Override
  public byte check(boolean clean)
  {
    return READY;
  }

  @Override
  public void debug()
  {
  }

  @Override
  public JExtent getExtent()
  {
    return null;
  }

  @Override
  public int getRecordLength()
  {
    return 0;
  }

  @Override
  public boolean intersects(JExtent other)
  {
    return false;
  }

  @Override
  public String toString()
  {
    return extent.toString();
  }

  @Override
  public void updateExtent()
  {
    extent = null;
  }
}
