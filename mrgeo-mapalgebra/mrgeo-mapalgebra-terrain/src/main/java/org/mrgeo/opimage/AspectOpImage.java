/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import javax.media.jai.FloatDoubleColorModel;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * Calculates the aspect of a given region. Aspects are output as degrees where
 * from 0-360 where zero is north and values increase cockwise. In flat regions
 * an aspect of -1 is given. The output is a single channel 32bit float.
 * 
 * Input is expected to be three channels that represent the normal values. If
 * any of the normal's three channels are null, the resulting value will be 
 * null.
 * 
 * 1. Horn, B. K. P. (1981). Hill Shading and the Reflectance Map, Proceedings
 * of the IEEE, 69(1):14-47.
 * http://scholar.google.com/scholar?cluster=13504326307708658108&hl=en
 * 
 */
@SuppressWarnings("unchecked")
public class AspectOpImage extends MrGeoOpImage
{
  RenderedImage src;

  public static AspectOpImage create(RenderedImage src, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new AspectOpImage(sources, hints);
  }

  @SuppressWarnings("rawtypes")
  private AspectOpImage(Vector sources, RenderingHints hints)
  {
    super(sources, hints);
    src = (RenderedImage) sources.get(0);

    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false,
        false, Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), 
        src.getSampleModel().getHeight());
  }

  // TODO Break this into multiple functions
  @Override
  final protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    PlanarImage source = sources[0];

    Raster normals = source.getData(destRect);
    
    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        double vx = normals.getSampleFloat(x, y, 0);
        double vy = normals.getSampleFloat(x, y, 1);
        double aspect;
        if (isNull(vx))
        {
          aspect = getNull();
        }
        else if (vx == 0.0 && vy == 0.0)
        {
          aspect = -1;
        }
        else
        {
          double theta = Math.atan2(vy, vx);
          double v = (-theta + 3 * Math.PI / 2) % (2 * Math.PI);
          aspect = 360 - v * (360 / (2 * Math.PI));
        }
        dest.setSample(x, y, 0, aspect);
      }
    }
  }
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
