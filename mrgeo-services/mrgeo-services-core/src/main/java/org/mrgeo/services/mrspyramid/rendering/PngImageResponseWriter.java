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

import ar.com.hjg.pngj.*;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.utils.GDALJavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Writes PNG images to an HTTP response
 */
public class PngImageResponseWriter extends ImageResponseWriterAbstract
{
  private static final Logger log = LoggerFactory.getLogger(PngImageResponseWriter.class);

  /*
   * (non-Javadoc)
   * 
   * @see org.mrgeo.services.wms.ImageResponseWriter#getMimeType()
   */
  @Override
  public String[] getMimeTypes()
  {
    return new String[] { "image/png" };
  }

  @Override
  public String getResponseMimeType()
  {
    return "image/png";
  }

  @Override
  public String[] getWmsFormats()
  {
    return new String[] { "png" };
  }

  @Override
  public void writeToStream(final MrGeoRaster raster, final double[] defaults,
    final ByteArrayOutputStream byteStream) throws IOException
  {
    GDALJavaUtils.saveRaster(raster.toDataset(null, defaults), byteStream, "png");
    byteStream.close();

//    // final long start = System.currentTimeMillis();
//
//    final boolean alpha = raster.bands() == 4;
//
//    final int bands = raster.bands();
//
//    final ImageInfo imi = new ImageInfo(raster.width(), raster.height(), 8, true);
//    final ImageLineInt line = new ImageLineInt(imi);
//
//    final PngWriter writer = new PngWriter(byteStream, imi);
//    writer.setFilterType(FilterType.FILTER_NONE); // no filtering
//    writer.setCompLevel(6);
//
//    final int r = 0;
//    final int g = 1;
//    final int b = 2;
//    final int a = alpha ? 3 : 0;
//
//    int rnodata;
//    int gnodata;
//    int bnodata;
//
//    byte red;
//    byte green;
//    byte blue;
//
//    if (!alpha)
//    {
//      rnodata = defaults != null && defaults.length > 0 ? (int) defaults[0] : Integer.MAX_VALUE;
//      gnodata = defaults != null && defaults.length > 1 ? (int) defaults[1] : Integer.MAX_VALUE;
//      bnodata = defaults != null && defaults.length > 2 ? (int) defaults[2] : Integer.MAX_VALUE;
//    }
//    else
//    {
//      rnodata = 0;
//      gnodata = 0;
//      bnodata = 0;
//    }
//
//    byte[] px = new byte[bands];
//    for (int y = 0, j = 0; y < raster.height(); y++)
//    {
//      for (int x = 0; x < raster.width(); x++, j += bands)
//      {
//
//        for (int band = 0; band < bands; band++)
//        {
//          px[b] = raster.getPixelByte(x, y, b);
//        }
//
//        if (alpha)
//        {
//          ImageLineHelper.setPixelRGBA8(line, x, px[0], px[1], px[2], px[3]);
//        }
//        else
//        {
//          red = (byte)px[0];
//          green = (byte)px[1];
//          blue =  (byte)px[2];
//
//          ImageLineHelper.setPixelRGBA8(line, x, red, green, blue, (red == rnodata &&
//              green == gnodata && blue == bnodata) ? 0 : 255);
//        }
//      }
//      writer.writeRow(line);
//    }
//    writer.end();
//
//    byteStream.close();
  }
}
