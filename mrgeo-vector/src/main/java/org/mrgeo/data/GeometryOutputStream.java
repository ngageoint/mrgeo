/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data;

import org.mrgeo.geometry.Geometry;

import java.io.IOException;

/**
 * @author jason.surratt
 * 
 */
@Deprecated
public interface GeometryOutputStream
{
  public abstract void close() throws IOException;

  public abstract void flush() throws IOException;

  public abstract void write(Geometry g) throws IOException;
}
