/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data;

import org.mrgeo.geometry.WritableGeometry;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author jason.surratt
 * 
 */
@Deprecated
public interface GeometryInputStream extends Iterator<WritableGeometry>
{
  /**
   * Returns the projection as WKT.
   */
  public String getProjection();

  @Override
  public boolean hasNext();

  /**
   * The returned WritableGeometry is own by the caller.
   * 
   * @return
   */
  @Override
  public WritableGeometry next();
  
  public void close() throws IOException;
}
