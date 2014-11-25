/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;

import java.io.Serializable;

/**
 * When serialized only the information necessary to read the data is saved.
 * E.g. If you're reading from a database, the connection info and query are
 * stored, not the actual data.
 * 
 * If applicable, it is advised that you implement HdfsResource as well.
 * 
 * @author jason.surratt
 * 
 */
@Deprecated
public interface GeometryCollection extends Iterable<WritableGeometry>, Serializable
{

  /**
   * The specified index starts at zero and increases up to size() - 1. This is
   * not associated with feature IDs.
   * 
   * @param index
   * @return
   */
  WritableGeometry get(int index);

  /**
   * Returns the projection of the geometry collection as WKT.
   * 
   * @return
   */
  String getProjection();

  int size();

  void close();
}
