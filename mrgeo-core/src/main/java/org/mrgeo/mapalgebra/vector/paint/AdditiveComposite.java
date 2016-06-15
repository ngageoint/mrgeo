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

import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

/**
 * @author jason.surratt
 * 
 */
public class AdditiveComposite implements Composite
{
  private static class AdditiveCompositeContext implements CompositeContext
  {

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

      for (int y = minY; y < maxY; y++)
      {
        for (int x = minX; x < maxX; x++)
        {
          dstOut.setSample(x, y, 0, src.getSample(x, y, 0) + dstIn.getSample(x, y, 0));
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
    return new AdditiveCompositeContext();
  }
}
