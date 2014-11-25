/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;

public class InverseGeometryFilter extends GeometryFilter
{
  private static final long serialVersionUID = 1L;
  String inputAttribute, outputAttribute;
  
  public InverseGeometryFilter(String inputAttribute, String outputAttribute)
  {
    this.inputAttribute = inputAttribute;
    this.outputAttribute = outputAttribute;
  }
  
  @Override
  public WritableGeometry filterInPlace(WritableGeometry g)
  {
    double v = Double.parseDouble(g.getAttribute(inputAttribute));
    g.setAttribute(outputAttribute, Double.toString(1 / v));
    return g;
  }
}
