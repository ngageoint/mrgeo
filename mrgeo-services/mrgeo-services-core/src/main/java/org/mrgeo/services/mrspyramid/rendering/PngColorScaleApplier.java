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

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.ColorScale.Scaling;
import org.mrgeo.data.raster.RasterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

/**
 * Applies a color scale to a PNG image
 */
public class PngColorScaleApplier extends ColorScaleApplier
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(PngColorScaleApplier.class);

  /*
   * (non-Javadoc)
   * 
   * @see org.mrgeo.services.wms.ColorScaleApplier#renderImage(java.awt.image.RenderedImage,
   * org.mrgeo.rasterops.ColorScale, double[], double)
   */
  @Override
  public Raster applyColorScale(final Raster raster, ColorScale colorScale, final double[] extrema,
    final double[] defaultValues) throws Exception
    {

    if (raster.getNumBands() == 3 || raster.getNumBands() == 4)
    {
      // no work to do...
      if (raster.getTransferType() == DataBuffer.TYPE_BYTE)
      {
        return raster;
      }

      final WritableRaster colored;
      if (raster.getNumBands() == 4)
      {
        colored = RasterUtils.createAGBRRaster(raster.getWidth(), raster.getHeight());
      }
      else
      {
        colored = RasterUtils.createGBRRaster(raster.getWidth(), raster.getHeight());
      }

      final int w = raster.getMinX() + raster.getWidth();
      final int h = raster.getMinY() + raster.getHeight();

      for (int y = raster.getMinY(); y < h; y++)
      {
        for (int x = raster.getMinX(); x < w; x++)
        {
          for (int b = 0; b < raster.getNumBands(); b++)
          {
            final double s = raster.getSampleDouble(x, y, b);
            colored.setSample(x, y, b, s);
          }
        }
      }
      return colored;
    }

    if (colorScale == null)
    {
      if (raster.getNumBands() == 1)
      {
        colorScale = ColorScale.createDefaultGrayScale();
      }
      else
      {
        colorScale = ColorScale.createDefault();
      }
    }

    // if we don't have min/max make the color scale modulo with
    if (extrema == null)
    {
      colorScale.setScaling(Scaling.Modulo);
      colorScale.setScaleRange(0.0, 10.0);
    }
    else if (colorScale.getScaling() == Scaling.MinMax)
    {
      colorScale.setScaleRange(extrema[0], extrema[1]);
    }

    colorScale.setTransparent(defaultValues[0]);

    final WritableRaster colored = RasterUtils.createAGBRRaster(raster.getWidth(), raster.getHeight());
    apply(raster, colored, colorScale);

    return colored;
    }

  @Override
  public String[] getMimeTypes()
  {
    return new String[] { "image/png" };
  }

  @Override
  public String[] getWmsFormats()
  {
    return new String[] { "png" };
  }

}
