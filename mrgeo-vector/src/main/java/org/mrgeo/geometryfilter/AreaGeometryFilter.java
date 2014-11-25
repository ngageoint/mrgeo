/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;

public class AreaGeometryFilter extends GeometryFilter
{
  private static final long serialVersionUID = 1L;
  String attributeName = "area";
  
  public AreaGeometryFilter()
  {
  }
  
  public AreaGeometryFilter(String attributeName)
  {
    this.attributeName = attributeName;
  }
  
  @Override
  public WritableGeometry filterInPlace(WritableGeometry g)
  {
    double area = JtsConverter.convertToJts(g).getArea();
    g.setAttribute(attributeName, Double.toString(area));
    return g;
  }
}
