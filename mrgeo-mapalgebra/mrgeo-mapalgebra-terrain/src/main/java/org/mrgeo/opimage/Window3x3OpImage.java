/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
import javax.media.jai.RenderedOp;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * Calculates the normal to a given pixel. This normal is calculated using
 * Horn's formula [1]. The normal is then reported as a two dimensional layer.
 * The first layer is the vertical component in the x dimension and the second
 * layer is the vertical component in the y dimension.
 * 
 * For performance reasons, this class is not re-entrant. It could easily be
 * made re-entrant.
 * 
 * 1. Horn, B. K. P. (1981). Hill Shading and the Reflectance Map, Proceedings
 * of the IEEE, 69(1):14-47.
 * http://scholar.google.com/scholar?cluster=13504326307708658108&hl=en
 * 
 */
@SuppressWarnings("unchecked")
abstract public class Window3x3OpImage extends MrGeoOpImage
{
  RenderedImage src;

  RenderedOp bordered;

  int lastY = -Integer.MAX_VALUE;

  static protected final int[] pdx = { -1, 0, 1, -1, 0, 1, -1,  0,  1 };
  static protected final int[] pdy = {  1, 1, 1,  0, 0, 0, -1, -1, -1 };

  static protected final int[] ndx = { -1, 0, 1, -1, 1, -1,  0,  1 };
  static protected final int[] ndy = {  1, 1, 1,  0, 0, -1, -1, -1 };

  double dx;
  double dy;

  @SuppressWarnings("unused")
  static private final int np = 0, zp = 1, pp = 2, nz = 3, zz = 4, pz = 5, nn = 6, zn = 7, pn = 8;

  @SuppressWarnings("rawtypes")
  protected Window3x3OpImage(Vector sources, RenderingHints hints)
  {
    super(sources, hints);
    src = (RenderedImage) sources.get(0);

    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false,
        false, Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), src
        .getSampleModel().getHeight());
  }

  // TODO Break this into multiple functions
  @Override
  final protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    PlanarImage source = sources[0];

    Rectangle necessaryBounds = (Rectangle) destRect.clone();
    necessaryBounds.grow(1, 1);

    Raster r = source.getData(necessaryBounds);

    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        dest.setSample(x, y, 0, computeWindow(r, x, y));
      }
    }
  }
  
  abstract protected double computeWindow(Raster r, int x, int y); 
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
