/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.data.GeometryInputStream;

import java.io.IOException;


public class ReprojectedGeometryInputStream implements GeometryInputStream
{

  String newProjection;
  Reprojector reprojector;
  GeometryInputStream srcStream = null;

  public ReprojectedGeometryInputStream(GeometryInputStream src, String newProjection)
  {
    this.newProjection = newProjection;
    srcStream = src;
    reprojector = Reprojector.createFromWkt(src.getProjection(), newProjection);
  }

  @Override
  public String getProjection()
  {
    return newProjection;
  }

  @Override
  public boolean hasNext()
  {
    return srcStream.hasNext();
  }

  @Override
  public WritableGeometry next()
  {
    WritableGeometry g = srcStream.next();
    if (g != null)
    {
      g.filter(reprojector);
    }
    return g;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    if (srcStream != null)
    {
      srcStream.close();
      srcStream = null;
    }
  }

}
