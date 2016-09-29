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

import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.utils.GDALJavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Writes JPEG images to an HTTP response
 */
public class JpegImageResponseWriter extends ImageResponseWriterAbstract
{
  private static final Logger log = LoggerFactory.getLogger(JpegImageResponseWriter.class);

  /*
   * (non-Javadoc)
   * 
   * @see org.mrgeo.services.wms.ImageResponseWriter#getMimeType()
   */
  @Override
  public String[] getMimeTypes()
  {
    return new String[] { "image/jpeg", "image/jpg" };
  }

  @Override
  public String getResponseMimeType()
  {
    return "image/jpeg";
  }

  @Override
  public String[] getWmsFormats()
  {
    return new String[] { "jpeg", "jpg" };
  }

  @Override
  public void writeToStream(final MrGeoRaster raster, double[] defaults, final ByteArrayOutputStream byteStream)
    throws IOException
  {
    GDALJavaUtils.saveRaster(raster.toDataset(null, defaults), byteStream, "jpeg");
    byteStream.close();
  }
}
