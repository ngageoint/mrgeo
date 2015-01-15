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
import org.mrgeo.data.raster.RasterUtils;

import javax.media.jai.ImageLayout;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("unchecked")
public class LogWithBaseOpImage extends MrGeoOpImage
{
  private int baseNum = 0;
  private double outputNoData = Double.NaN;
  
  public static LogWithBaseOpImage create(RenderedImage src, int base, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    ImageLayout layout = RasterUtils.createImageLayout(src, DataBuffer.TYPE_FLOAT, 1);
    return new LogWithBaseOpImage(sources, layout, base, hints);
  }
  
  @SuppressWarnings("rawtypes")
  private LogWithBaseOpImage(Vector sources, ImageLayout layout, int base, RenderingHints hints)
  {
    super(sources, hints);
    baseNum = base;
    // We use a single NoData value for all the bands of output
    OpImageUtils.setNoData(this, outputNoData);
  }

  @Override
  final protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    PlanarImage source = sources[0];
    Raster raster = source.getData(destRect);
    int bands = raster.getNumBands();

    for (int band = 0; band < bands; band++)
    {
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double s = raster.getSampleDouble(x, y, band);
          // It is assumed that the same NoData value is used for all the
          // input bands.
          if (isNull(s))
          {
            dest.setSample(x, y, band, outputNoData);
          }
          else
          {
            double r = Math.log(s)/ Math.log(baseNum);
            dest.setSample(x, y, band, r);
          }
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
