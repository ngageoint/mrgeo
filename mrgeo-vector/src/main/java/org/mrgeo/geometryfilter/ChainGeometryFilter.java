/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;

import java.util.LinkedList;

public class ChainGeometryFilter extends GeometryFilter
{
  private static final long serialVersionUID = 1L;
  private LinkedList<GeometryFilter> filters = new LinkedList<GeometryFilter>();
  
  public ChainGeometryFilter()
  {
  }
  
  public void addFilter(GeometryFilter gf)
  {
    filters.add(gf);
  }
  
  @Override
  public WritableGeometry filterInPlace(WritableGeometry g)
  {
    for (GeometryFilter gf : filters)
    {
      g = gf.filterInPlace(g);
    }
    return g;
  }
}
