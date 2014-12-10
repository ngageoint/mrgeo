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

import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
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
public class TopographicPositionIndexOpImage extends Window3x3OpImage
{
  public static TopographicPositionIndexOpImage create(RenderedImage src, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new TopographicPositionIndexOpImage(sources, hints);
  }

  @SuppressWarnings("rawtypes")
  private TopographicPositionIndexOpImage(Vector sources, RenderingHints hints)
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
          sum += v;
          weight++;
        }
      }
      if (weight != 0)
      {
        result = eo - (sum / weight);
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
