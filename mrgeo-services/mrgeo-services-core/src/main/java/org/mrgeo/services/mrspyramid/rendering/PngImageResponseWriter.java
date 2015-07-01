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

import ar.com.hjg.pngj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
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
  public void writeToStream(final Raster raster, final double[] defaults,
    final ByteArrayOutputStream byteStream) throws IOException
  {
    // final long start = System.currentTimeMillis();

    final int[] bandOffsets;

    final SampleModel model = raster.getSampleModel();
    if (model instanceof ComponentSampleModel)
    {
      final ComponentSampleModel cm = (ComponentSampleModel) model;
      bandOffsets = cm.getBandOffsets();
    }
    else
    {
      bandOffsets = new int[raster.getNumBands()];
      for (int i = 0; i < raster.getNumBands(); i++)
      {
        bandOffsets[i] = i;
      }
    }

    final boolean alpha = raster.getNumBands() == 4;

    final byte[] bytes = ((DataBufferByte) raster.getDataBuffer()).getData();

    final int bands = raster.getNumBands();
    final int scanlinestride = raster.getWidth() * bands;

    // final ImageInfo imi = new ImageInfo(raster.getWidth(), raster.getHeight(), 8, alpha);
    final ImageInfo imi = new ImageInfo(raster.getWidth(), raster.getHeight(), 8, true);

    final ImageLineInt line = new ImageLineInt(imi);

    final PngWriter writer = new PngWriter(byteStream, imi);
    writer.setFilterType(FilterType.FILTER_NONE); // no filtering
    writer.setCompLevel(6);

    final int r = bandOffsets[0];
    final int g = bandOffsets[1];
    final int b = bandOffsets[2];
    final int a = alpha ? bandOffsets[3] : 0;

    int rnodata;
    int gnodata;
    int bnodata;

    int red;
    int green;
    int blue;

    if (!alpha)
    {
      rnodata = defaults != null && defaults.length > 0 ? (int) defaults[0] : Integer.MAX_VALUE;
      gnodata = defaults != null && defaults.length > 1 ? (int) defaults[1] : Integer.MAX_VALUE;
      bnodata = defaults != null && defaults.length > 2 ? (int) defaults[2] : Integer.MAX_VALUE;
    }
    else
    {
      rnodata = 0;
      gnodata = 0;
      bnodata = 0;
    }

    for (int row = 0, rowoffset = 0; row < imi.rows; row++, rowoffset += scanlinestride)
    {
      for (int col = 0, j = rowoffset; col < imi.cols; col++, j += bands)
      {
        if (alpha)
        {
          ImageLineHelper.setPixelRGBA8(line, col, bytes[j + r], bytes[j + g], bytes[j + b],
            bytes[j + a]);
        }
        else
        {
          // ImageLineHelper.setPixelRGB8(line, col, bytes[j + r], bytes[j + g], bytes[j + b]);
          red = bytes[j + r];
          green = bytes[j + g];
          blue = bytes[j + b];

          ImageLineHelper.setPixelRGBA8(line, col, red, green, blue, (red == rnodata &&
            green == gnodata && blue == bnodata) ? 0 : 255);
        }
      }
      writer.writeRow(line);
    }
    writer.end();

    // ImageIO.write(RasterUtils.makeBufferedImage(raster), "PNG", byteStream);

    byteStream.close();
  }

}
