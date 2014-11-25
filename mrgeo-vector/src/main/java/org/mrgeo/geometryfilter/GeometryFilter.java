/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;

import java.io.Serializable;

public abstract class GeometryFilter implements Serializable
{
  private static final long serialVersionUID = 1L;

  public WritableGeometry filter(Geometry g)
  {
    return filterInPlace(g.createWritableClone());
  }
  
  /**
   * Filters the geometry in place if possible and returns the result.
   * @param g
   */
  public abstract WritableGeometry filterInPlace(WritableGeometry g);
}
