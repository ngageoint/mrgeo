/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.colorscale.applier;

import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.data.raster.MrGeoRaster;

/**
 * Interface to implement for classes who apply color scales to images requested by the WMS;
 * Implementing class names must begin with the image format name they handle (e.g. "Png") and end
 * with "ColorScaleApplier". This class must stay in the same namespace as ImageHandlerFactory.
 */
public abstract class ColorScaleApplier
{
/**
 * Applies a color scale to a rendered image
 */
public abstract MrGeoRaster applyColorScale(MrGeoRaster raster, ColorScale colorScale, double[] extrema,
    double[] defaultValue, double[][] quantiles) throws ColorScale.ColorScaleException;

/**
 * Returns the mime type for the color scale applier
 *
 * @return a mime type string
 */
public abstract String[] getMimeTypes();

public abstract String[] getWmsFormats();

public abstract int getBytesPerPixelPerBand();

public abstract int getBands(final int sourceBands);

protected void apply(final MrGeoRaster source, final MrGeoRaster dest, ColorScale colorScale)
{
  if (source.bands() == dest.bands() && (source.bands() == 3 || source.bands() == 4))
  {
    dest.copyFrom(0, 0, source.width(), source.height(), source, 0, 0);
    return;
  }

  if (colorScale == null)
  {
    colorScale = ColorScale.createDefaultGrayScale();
  }

  int[] color = new int[4];

  for (int y = 0; y < dest.height(); y++)
  {
    for (int x = 0; x < dest.width(); x++)
    {
      colorScale.lookup(source.getPixelDouble(x, y, 0), color);

      for (int b = 0; b < color.length; b++)
      {
        if (dest.bands() > b)
        {
          dest.setPixel(x, y, b, color[b]);
        }
      }
    }
  }
}

void setupExtrema(ColorScale colorScale, double[] extrema, double defaultValue, double[] quantiles)
{
// if we don't have min/max make the color scale modulo with
  if (extrema == null)
  {
    colorScale.setScaling(ColorScale.Scaling.Modulo);
    colorScale.setScaleRange(0.0, 10.0);
  }
  else if (colorScale.getScaling() == ColorScale.Scaling.MinMax)
  {
    colorScale.setScaleRange(extrema[0], extrema[1]);
  }
  else if (colorScale.getScaling() == ColorScale.Scaling.Quantile) {
    colorScale.setScaleRangeWithQuantiles(extrema[0], extrema[1], quantiles);
  }

  colorScale.setTransparent(defaultValue);
}
}
