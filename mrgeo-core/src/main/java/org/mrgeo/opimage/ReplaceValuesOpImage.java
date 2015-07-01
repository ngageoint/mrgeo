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

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.*;
import java.util.Vector;


@SuppressWarnings("unchecked")
public final class ReplaceValuesOpImage extends MrGeoOpImage
{
  double newValue;
  double min, max;
  boolean _floatAndEquals = false;

  public static ReplaceValuesOpImage create(RenderedImage src, double newValue, boolean newNull,
      double min, double max, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    return new ReplaceValuesOpImage(sources, newValue, newNull, min, max, hints);
  }

  public static ReplaceValuesOpImage create(RenderedImage src, double newValue, boolean newNull,
      double oldValue, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    return new ReplaceValuesOpImage(sources, newValue, newNull, oldValue, Double.NaN, hints);
  }

  @SuppressWarnings("rawtypes")
  private ReplaceValuesOpImage(Vector sources, double newValue, boolean newNull, double min,
      double max, RenderingHints hints)
  {
    super(sources, hints);

    this.newValue = newValue;
    this.min = min;
    this.max = max;
    RenderedImage src = (RenderedImage) sources.get(0);

    colorModel = src.getColorModel();
    sampleModel = src.getSampleModel();
    if (newNull)
    {
      OpImageUtils.setNoData(this, newValue);
    }

    // this is a common case when reading geotiffs and merits a special
    // optimization.
    if (sampleModel.getDataType() == DataBuffer.TYPE_FLOAT && Double.isNaN(max))
    {
      _floatAndEquals = true;
    }
  }

  @Override
  protected final void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r1 = sources[0].getData(destRect);

    if (_floatAndEquals)
    {
      float minf = (float)min;
      float newValuef = (float)newValue;
      
      DataBufferFloat dbf = (DataBufferFloat) r1.getDataBuffer();
      float[][] data = dbf.getBankData();
      for (int bank = 0; bank < data.length; bank++)
      {
        float[] b = data[bank];
        for (int i = 0; i < b.length; i++)
        {
          if (b[i] == minf)
          {
            b[i] = newValuef;
          }
        }
      }
    }
    else
    {
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double v = r1.getSampleDouble(x, y, 0);

          if (v > min && v < max)
          {
            v = newValue;
          }
          dest.setSample(x, y, 0, v);
        }
      }
    }
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
