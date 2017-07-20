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
import org.mrgeo.colorscale.ColorScale.ColorScaleException;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.MrGeoRaster.MrGeoRasterException;
import org.mrgeo.data.raster.RasterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;

/**
 * Applies a color scale to a PNG image
 */
public class JpegColorScaleApplier extends ColorScaleApplier
{
@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(JpegColorScaleApplier.class);

@Override
public MrGeoRaster applyColorScale(MrGeoRaster raster, ColorScale colorScale, double[] extrema,
    double[] defaultValues, double[][] quantiles) throws ColorScaleException
{
  try
  {
    MrGeoRaster colored = MrGeoRaster.createEmptyRaster(raster.width(), raster.height(), getBands(raster.bands()), DataBuffer.TYPE_BYTE);
    colored.fill(RasterUtils.getDefaultNoDataForType(DataBuffer.TYPE_BYTE));

    setupExtrema(colorScale, extrema, defaultValues[0],
            (quantiles == null || quantiles.length == 0) ? null : quantiles[0]);
    apply(raster, colored, colorScale);
    return colored;
  }
  catch (MrGeoRasterException e)
  {
    throw new ColorScaleException(e);
  }

}

@Override
public int getBytesPerPixelPerBand()
{
  return 1;
}

@Override
public int getBands(int sourceBands)
{
  return 3;
}

@Override
public String[] getMimeTypes()
{
  return new String[]{"image/jpeg", "image/jpg"};
}

@Override
public String[] getWmsFormats()
{
  return new String[]{"jpeg", "jpg"};
}

}
