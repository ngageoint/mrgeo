/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;

public class MultiplyGeometryFilter extends GeometryFilter
{
  private static final long serialVersionUID = 1L;
  String inputAttribute1, inputAttribute2 = null, outputAttribute;
  double multiplier;

  public MultiplyGeometryFilter(String inputAttribute1, String inputAttribute2, 
      String outputAttribute)
  {
    this.inputAttribute1 = inputAttribute1;
    this.inputAttribute2 = inputAttribute2;
    this.outputAttribute = outputAttribute;
  }

  public MultiplyGeometryFilter(String inputAttribute, String outputAttribute, double multiplier)
  {
    this.inputAttribute1 = inputAttribute;
    this.outputAttribute = outputAttribute;
    this.multiplier = multiplier;
  }

  @Override
  public WritableGeometry filterInPlace(WritableGeometry g)
  {
    String sv1 = g.getAttribute(inputAttribute1);
    double v1 = sv1 == null ? 0.0 : Double.parseDouble(sv1);
    double v2;
    if (inputAttribute2 == null)
    {
      v2 = multiplier;
    }
    else
    {
      String sv2 = g.getAttribute(inputAttribute2);
      v2 = sv2 == null ? 0.0 : Double.parseDouble(sv2);
    }
    g.setAttribute(outputAttribute, Double.toString(v1 * v2));
    return g;
  }
}
