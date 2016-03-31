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

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.utils.GDALJavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Writes TIF images to an HTTP response
 */
public class TiffImageResponseWriter extends ImageResponseWriterAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(TiffImageResponseWriter.class);

  /*
   * (non-Javadoc)
   * 
   * @see org.mrgeo.services.wms.ImageResponseWriter#getMimeType()
   */
  @Override
  public String[] getMimeTypes()
  {
    return new String[] { "image/tiff", "image/tif" };
  }

  @Override
  public String getResponseMimeType()
  {
    return "image/tiff";
  }

  @Override
  public String[] getWmsFormats()
  {
    return new String[] { "tiff", "tif" };
  }


  @Override
  public void writeToStream(Raster raster, double[] defaults, ByteArrayOutputStream byteStream) throws IOException
  {
    GDALJavaUtils.saveRaster(raster, byteStream, "Tiff");
    //ImageIO.write(RasterUtils.makeBufferedImage(raster), "TIFF", byteStream);
    byteStream.close();
  }
}
