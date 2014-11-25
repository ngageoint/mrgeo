/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Bounds;

import java.util.Iterator;


/**
 * @author jason.surratt
 * 
 */
public class BoundsCalculator
{
  public static Bounds calculateBounds(Iterator<? extends Geometry> it)
  {
    Bounds result = new Bounds();

    while (it.hasNext())
    {
      result.expand(it.next().getBounds());
    }

    return result;
  }
}
