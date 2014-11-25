/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;

public class IncludeGeometryFilter extends GeometryFilter
{
  private static final long serialVersionUID = 1L;
  String includeAttribute, value;
  
  public IncludeGeometryFilter(String includeAttribute, String value)
  {
    this.includeAttribute = includeAttribute;
    this.value = value;
  }
  
  @Override
  public WritableGeometry filterInPlace(WritableGeometry g)
  {
    WritableGeometry result = null;
    String v = g.getAttribute(includeAttribute);
    if (v != null && v.equals(value))
    {
      result = g;
    }
    return result;
  }
}
