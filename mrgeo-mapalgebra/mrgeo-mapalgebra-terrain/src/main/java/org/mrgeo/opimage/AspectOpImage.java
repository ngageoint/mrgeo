/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
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
  private static final double DEG_2_RAD = 0.0174532925;

  RenderedImage src;
  String units;

  public static AspectOpImage create(RenderedImage src, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new AspectOpImage(sources, hints);
  }

  public static AspectOpImage create(RenderedImage src, String units, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new AspectOpImage(sources, units, hints);
  }

  @SuppressWarnings("rawtypes")
  private AspectOpImage(Vector sources, RenderingHints hints)
  {
    this(sources, "deg", hints);
  }
  @SuppressWar
  nings("rawtypes")
  private AspectOpImage(Vector sources, String units, RenderingHints hints)
  {
    super(sources, hints);
    src = (RenderedImage) sources.get(0);
    this.units = units;

    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false,
        false, Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), 
        src.getSampleModel().getHeight());
  }

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

          if (units.equalsIgnoreCase("rad"))
          {
            aspect *= DEG_2_RAD;
          }
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
