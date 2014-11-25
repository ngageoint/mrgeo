/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometry;

import org.mrgeo.data.GeometryOutputStream;

import java.io.IOException;
import java.util.Iterator;

@Deprecated
public class GeometryWriter
{
  public static void write(Iterator<? extends Geometry> gis, GeometryOutputStream gos)
      throws IOException
  {
    while (gis.hasNext())
    {
      gos.write(gis.next());
    }
  }
}
