/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.esri;

import org.mrgeo.data.shp.esri.geom.JShape;

import java.io.IOException;


public interface ShpData
{
  public void addShape(JShape obj) throws FormatException;

  public int getCount();

  public JShape getShape(int i) throws IOException;

  public void load(int i, byte[] record);

  public void resizeCache(int size);

  public byte[] save(int i);

  public void setParent(ESRILayer parent);
}
