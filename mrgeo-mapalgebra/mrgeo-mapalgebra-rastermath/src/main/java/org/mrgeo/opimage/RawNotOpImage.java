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

import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.raster.RasterUtils;

import javax.media.jai.ImageLayout;
import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("unchecked")
public final class RawNotOpImage extends OpImage implements Serializable
{
  private static final long serialVersionUID = 1L;
  private static final double EPSILON = 0.0001;

  private double noDataValue;
  private boolean isNoDataValueNan;
  private int outputNoData;

  public static RawNotOpImage create(RenderedImage src1)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src1);
    ImageLayout layout = RasterUtils.createImageLayout(src1, DataBuffer.TYPE_BYTE, 1);
    int outNoData = 255;
    return new RawNotOpImage(sources, layout, outNoData);
  }

  @SuppressWarnings("rawtypes")
  private RawNotOpImage(Vector sources, ImageLayout layout, int outNoData)
  {
    super(sources, layout, null, false);

    RenderedImage src = (RenderedImage) sources.get(0);
    noDataValue = OpImageUtils.getNoData(src, Double.NaN);
    isNoDataValueNan = Double.isNaN(noDataValue);
    outputNoData = outNoData;
    OpImageUtils.setNoData(this, outputNoData);
  }

  //TODO: make this work for multiband images
  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r1 = sources[0].getData(destRect);

    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        double v = r1.getSampleDouble(x, y, 0);
        if (OpImageUtils.isNoData(v, noDataValue, isNoDataValueNan))
        {
          dest.setSample(x, y, 0, outputNoData);
        }
        else
        {
          // If the input value is close to 0, then output a 0, otherwise output a 1
          final int outputValue = (v >= -EPSILON && v <= EPSILON) ? 0 : 1;
          dest.setSample(x, y, 0, outputValue);
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
    return String.format("RawNotOpImage");
  }
}
