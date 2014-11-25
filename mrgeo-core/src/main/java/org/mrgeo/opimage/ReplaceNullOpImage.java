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

import org.mrgeo.rasterops.OpImageUtils;

import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

@SuppressWarnings("unchecked")
public final class ReplaceNullOpImage extends OpImage
{
  double newValue;
  // if this is set to true, then newValue becomes the new null value.
  private double oldNoDataValue;
  private boolean isOldNoDataNaN = false;

  public static ReplaceNullOpImage create(RenderedImage src, double newValue, boolean newNull,
      RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    return new ReplaceNullOpImage(sources, newValue, newNull, hints);
  }

  @SuppressWarnings("rawtypes")
  private ReplaceNullOpImage(Vector sources, double newValue, boolean newNull, RenderingHints hints)
  {
    super(sources, null, null, false);

    this.newValue = newValue;
    RenderedImage src = (RenderedImage) sources.get(0);
    oldNoDataValue = OpImageUtils.getNoData(src, Double.NaN);
    isOldNoDataNaN = Double.isNaN(oldNoDataValue);
    if (newNull)
    {
      OpImageUtils.setNoData(this, newValue);
    }

    colorModel = src.getColorModel();
    sampleModel = src.getSampleModel();
  }

  private boolean isNoData(double[] v)
  {
    // The following condition was held over from MrGeoOpImage.isNull().
    // I don't know why we need the special treatment for 4-band data...
    if (v.length == 4)
    {
      if (v[3] < 0.5)
      {
        return true;
      }
      else if (v[0] < 0.5 && v[1] < 0.5 && v[2] < 0.5)
      {
        return true;
      }
      else
      {
        return false;
      }
    }
    return OpImageUtils.isNoData(v[0], oldNoDataValue, isOldNoDataNaN);
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r1 = sources[0].getData(destRect);

    double[] pixel = new double[r1.getNumBands()];
    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        r1.getPixel(x, y, pixel);

        if (isNoData(pixel))
        {
          for (int i = 0; i < pixel.length; i++)
          {
            pixel[i] = newValue;
          }
        }
        dest.setPixel(x, y, pixel);
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
