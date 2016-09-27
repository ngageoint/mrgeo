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

package org.mrgeo.colorscale.applier;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdalconst.gdalconstConstants;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.data.raster.MrGeoRaster;

/**
 * Interface to implement for classes who apply color scales to images requested by the WMS;
 * Implementing class names must begin with the image format name they handle (e.g. "Png") and end
 * with "ColorScaleApplier". This class must stay in the same namespace as ImageHandlerFactory.
 */
public abstract class ColorScaleApplier
{
protected void apply(final MrGeoRaster source, final Dataset dest, ColorScale colorScale)
{
  if (source.bands() == 3 || source.bands() == 4)
  {

    byte px[] = new byte[1];

    for (int b = 0; b < 3; b++)
    {
      Band band = dest.GetRasterBand(b + 1);

      for (int y = 0; y < source.height(); y++)
      {
        for (int x = 0; x < source.width(); x++)
        {
          px[0] = source.getPixelByte(x, y, b);
          band.WriteRaster(x, y, 1, 1, gdalconstConstants.GDT_Byte, px);
        }
      }
    }
    return;
  }

  if (colorScale == null)
  {
    if (source.bands() == 1)
    {
      colorScale = ColorScale.createDefaultGrayScale();
    }
    else
    {
      colorScale = ColorScale.createDefault();
    }
  }

  int[] px_row = new int[source.width() * 4];

  for (int y = 0; y < source.height(); y++)
  {
    for (int x = 0; x < source.width(); x++)
    {
      colorScale.lookup(source.getPixelDouble(x, y, 0), px_row, x * 4);
    }

    dest.WriteRaster(0, y, source.width(), 1, source.width(), 1, gdalconstConstants.GDT_Int32,
        px_row, null);
  }
}

void setupExtrema(ColorScale colorScale, double[] extrema, double defaultValue)
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

  colorScale.setTransparent(defaultValue);
}


/**
 * Applies a color scale to a rendered image
 *
 */
public abstract MrGeoRaster applyColorScale(MrGeoRaster raster, ColorScale colorScale, double[] extrema,
    double[] defaultValue) throws Exception;

/**
 * Returns the mime type for the color scale applier
 *
 * @return a mime type string
 */
public abstract String[] getMimeTypes();

public abstract String[] getWmsFormats();
}
