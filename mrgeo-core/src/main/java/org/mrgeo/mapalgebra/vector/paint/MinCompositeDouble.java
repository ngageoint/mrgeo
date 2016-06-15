


/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.mapalgebra.vector.paint;

import org.mrgeo.utils.FloatUtils;

import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

public class MinCompositeDouble extends WeightedComposite
{

  public MinCompositeDouble()
  {
  }

  public MinCompositeDouble(double weight)
  {
    super(weight);
  }

  public MinCompositeDouble(double weight, double nodata)
  {
    super(weight, nodata);
  }


  private class MinCompositeDoubleContext implements CompositeContext
  {
    public MinCompositeDoubleContext()
    {
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.awt.CompositeContext#compose(java.awt.image.Raster,
     * java.awt.image.Raster, java.awt.image.WritableRaster)
     */
    @Override
    public void compose(Raster src, Raster dstIn, WritableRaster dstOut)
    {
      int minX = dstOut.getMinX();
      int minY = dstOut.getMinY();
      int maxX = minX + dstOut.getWidth();
      int maxY = minY + dstOut.getHeight();

      //log.debug("minX,minY,maxX,maxY: " + minX + "," + minY + "," + maxX + "," + maxY);
      for (int y = minY; y < maxY; y++)
      {
        for (int x = minX; x < maxX; x++)
        {
          double d = dstIn.getSampleDouble(x, y, 0);
          double s = src.getSampleDouble(x, y, 0) * weight;

          double sample;

          if (isNodataNaN)
          {
            if (Double.isNaN(d))
            {
              sample = s;            
            }
            else if (Double.isNaN(s))
            {
              sample = d;
            }
            else if (s < d)
            {
              sample = s;
            }
            else
            {
              sample = d;
            }
          }
          else
          {
            if (FloatUtils.isEqual(d, nodata))
            {
              sample = s;           
            }
            else if (FloatUtils.isEqual(s, nodata))
            {
              sample = d;
            }
            else if (s < d)
            {
              sample = s;
            }
            else
            {
              sample = d;
            }
          }

          dstOut.setSample(x, y, 0, sample);
        }
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.awt.CompositeContext#dispose()
     */
    @Override
    public void dispose()
    {

    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.awt.Composite#createContext(java.awt.image.ColorModel,
   * java.awt.image.ColorModel, java.awt.RenderingHints)
   */
  @Override
  public CompositeContext createContext(ColorModel srcColorModel, ColorModel dstColorModel,
      RenderingHints hints)
  {
    return new MinCompositeDoubleContext();
  }

}
