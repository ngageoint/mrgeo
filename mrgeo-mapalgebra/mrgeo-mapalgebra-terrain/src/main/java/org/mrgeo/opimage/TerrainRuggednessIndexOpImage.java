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
