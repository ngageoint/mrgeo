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

import org.gdal.gdal.Dataset;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.utils.GDALUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;

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
 * ColorScale, double[], double)
 */
@Override
public MrGeoRaster applyColorScale(final MrGeoRaster raster, ColorScale colorScale, final double[] extrema,
    final double[] defaultValues) throws Exception
{
  Dataset colored = GDALUtils.createEmptyMemoryRaster(raster.width(), raster.height(),
      raster.bands() == 3 ? 3 : 4, GDALUtils.toGDALDataType(DataBuffer.TYPE_BYTE), defaultValues);

  setupExtrema(colorScale, extrema, defaultValues[0]);
  apply(raster, colored, colorScale);
  return MrGeoRaster.fromDataset(colored);
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
