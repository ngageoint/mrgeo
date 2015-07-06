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

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.rasterops.ColorScale;

import java.awt.image.*;

/**
 * Interface to implement for classes who apply color scales to images requested by the WMS;
 * Implementing class names must begin with the image format name they handle (e.g. "Png") and end
 * with "ColorScaleApplier". This class must stay in the same namespace as ImageHandlerFactory.
 */
public abstract class ColorScaleApplier
{
  protected static void apply(final Raster source, final WritableRaster dest,
    final ColorScale colorScale)
  {
    final int w = source.getMinX() + source.getWidth();
    final int h = source.getMinY() + source.getHeight();

    final DataBufferByte buffer = (DataBufferByte) (dest.getDataBuffer());

    final int scanlineStride;
    final int pixelStride;
    final int[] bandOffsets;

    final SampleModel model = dest.getSampleModel();
    if (model instanceof ComponentSampleModel)
    {
      final ComponentSampleModel cm = (ComponentSampleModel) model;
      scanlineStride = cm.getScanlineStride();
      pixelStride = cm.getPixelStride();
      bandOffsets = cm.getBandOffsets();
    }
    else
    {
      scanlineStride = w;
      pixelStride = 1;
      bandOffsets = new int[source.getNumBands()];
      for (int i = 0; i < source.getNumBands(); i++)
      {
        bandOffsets[i] = i;
      }
    }

    final byte[][] data = buffer.getBankData();
    final int banks = data.length;

    for (int y = source.getMinY(); y < h; y++)
    {
      final int lineOffset = y * scanlineStride;
      for (int x = source.getMinX(); x < w; x++)
      {
        final double v = source.getSampleDouble(x, y, 0);
        final int[] color = colorScale.lookup(v);

        if (banks == 1)
        {
          final int pixelOffset = lineOffset + x * pixelStride;

          for (int b = 0; b < dest.getNumBands(); b++)
          {
            data[0][pixelOffset + bandOffsets[b]] = (byte) color[b];
          }
        }
        else
        {
          // what to do here?
        }
      }
    }
  }

  /**
   * Applies a color scale to a rendered image
   * 
   * @param image
   *          the image to apply the color scale to
   * @param colorScale
   *          the color scale to apply to the image
   * @param extrema
   *          minimum/maximum values in the image raster data
   * @param defaultValue
   *          default value in the image raster data
   * @param isTransparent
   *          set to true if the image being passed in is transparent; false otherwise
   * @return a rendered image with a color scale applied
   * @throws Exception
   * @todo don't necessarily like "isTransparent", but it will have to do for now
   */
  public abstract Raster applyColorScale(Raster raster, ColorScale colorScale, double[] extrema,
    double[] defaultValue) throws Exception;

  /**
   * Returns the mime type for the color scale applier
   * 
   * @return a mime type string
   */
  abstract String[] getMimeTypes();

  abstract String[] getWmsFormats();
}
