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

package org.mrgeo.rasterops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.*;
import java.util.Vector;


@SuppressWarnings("unchecked")
public final class ColorScaleOpImage extends OpImage
{
  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(ColorScaleOpImage.class);
  RenderedImage src;
  ColorScale colorScale;
  
  public static ColorScaleOpImage create(RenderedImage src, ColorScale colorScale)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    return new ColorScaleOpImage(sources, colorScale);
  }

  @SuppressWarnings("rawtypes")
  private ColorScaleOpImage(Vector sources, ColorScale colorScale)
  {
    super(sources, null, null, false);

    src = (RenderedImage) sources.get(0);
    
    this.colorScale = colorScale;

    colorModel = new BufferedImage(1, 1, BufferedImage.TYPE_4BYTE_ABGR).getColorModel();
    sampleModel = 
      colorModel.createCompatibleSampleModel(
        src.getSampleModel().getWidth(), 
        src.getSampleModel().getHeight());
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r = src.getData(destRect);
    
    int maxY = destRect.y + destRect.height;
    int maxX = destRect.x + destRect.width;
    
    // looping through like this is extremely inefficient.
    for (int y = destRect.y; y < maxY; y++)
    {
      for (int x = destRect.x; x < maxX; x++)
      {
        final double v = r.getSampleDouble(x, y, 0);
        dest.setPixel(x, y, colorScale.lookup(v));
      }
    }
  }

  /* (non-Javadoc)
   * @see javax.media.jai.PlanarImage#getSampleModel()
   */
  @Override
  public SampleModel getSampleModel()
  {
    return sampleModel;
  }
  
  @Override
  public Rectangle mapDestRect(Rectangle destRect, int sourceIndex)
  {
    return destRect;
  }

  @Override
  public Rectangle mapSourceRect(Rectangle sourceRect, int sourceIndex)
  {
    return sourceRect;
  }
  
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }
}
