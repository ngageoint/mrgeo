/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.util.Vector;

/**
 */
@SuppressWarnings("unchecked")
public class TerrainRuggednessIndexOpImage extends Window3x3OpImage
{
  public static TerrainRuggednessIndexOpImage create(RenderedImage src, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new TerrainRuggednessIndexOpImage(sources, hints);
  }

  @SuppressWarnings("rawtypes")
  private TerrainRuggednessIndexOpImage(Vector sources, RenderingHints hints)
  {
    super(sources, hints);
  }

  // TODO Break this into multiple functions
  @Override
  final protected double computeWindow(Raster r, int x, int y)
  {
    double result = 0.0;
    double eo = r.getSampleDouble(x, y, 0);
    if (isNull(eo))
    {
      result = getNull();
    }
    else
    {
      double sum = 0.0;
      int weight = 0;
      for (int i = 0; i < 8; i++)
      {
        double v = r.getSampleDouble(x + ndx[i], y + ndx[i], 0);
        if (isNull(v) == false)
        {
          sum += Math.abs(eo - v);
          weight++;
        }
      }
      if (weight != 0)
      {
        result = sum / weight;
      }
    }
    return result;
  }
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
