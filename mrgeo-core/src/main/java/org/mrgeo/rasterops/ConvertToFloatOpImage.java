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

import javax.media.jai.FloatDoubleColorModel;
import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;


@SuppressWarnings("unchecked")
public final class ConvertToFloatOpImage extends OpImage
{
  private RenderedImage src;
  private double noDataValue;
  private boolean isNoDataNan;
  private float outputNoData = Float.NaN;
  
  public static ConvertToFloatOpImage create(RenderedImage src)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    return new ConvertToFloatOpImage(sources);
  }

  @SuppressWarnings("rawtypes")
  private ConvertToFloatOpImage(Vector sources)
  {
    super(sources, null, null, false);

    src = (RenderedImage) sources.get(0);
    noDataValue = OpImageUtils.getNoData(src, Double.NaN);
    isNoDataNan = Double.isNaN(noDataValue);
    OpImageUtils.setNoData(this, outputNoData);
    
    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false,
        false, Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), src
        .getSampleModel().getHeight());
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r = src.getData(destRect);

    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        final double v = r.getSampleDouble(x, y, 0);
        if (OpImageUtils.isNoData(v, noDataValue, isNoDataNan))
        {
          dest.setSample(x, y, 0, outputNoData);
        }
        else
        {
          dest.setSample(x, y, 0, (float)v);
        }
      }
    }
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
