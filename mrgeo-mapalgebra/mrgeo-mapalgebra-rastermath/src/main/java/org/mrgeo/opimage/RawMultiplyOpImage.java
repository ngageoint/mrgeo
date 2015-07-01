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

package org.mrgeo.opimage;

import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

@SuppressWarnings("unchecked")
public final class RawMultiplyOpImage extends OpImage
{
  public enum BandTreatment {
    /**
     * The bands of each image are multiplied up to the smallest number of bands
     * in one of the images.
     */
    ONE_FOR_ONE,
    /**
     * The first band of the first image is multiplied against all the bands of
     * the second image.
     */
    ONE_BY_RGB
  }

  private BandTreatment bandTreatment;
  int numBands;

  public static RawMultiplyOpImage create(RenderedImage src1, RenderedImage src2,
      BandTreatment bandTreatment)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src1);
    sources.add(src2);
    return new RawMultiplyOpImage(sources, bandTreatment);
  }

  @SuppressWarnings("rawtypes")
  private RawMultiplyOpImage(Vector sources, BandTreatment bandTreatment)
  {
    super(sources, null, null, false);

    this.bandTreatment = bandTreatment;

    RenderedImage src1 = (RenderedImage) sources.get(0);
    RenderedImage src2 = (RenderedImage) sources.get(1);

    switch (bandTreatment)
    {
    case ONE_FOR_ONE:
      numBands = Math.min(src1.getSampleModel().getNumBands(), src2.getSampleModel().getNumBands());
      break;
    case ONE_BY_RGB:
      if (src2.getSampleModel().getNumBands() < 3 || src2.getSampleModel().getNumBands() > 4)
      {
        throw new IllegalArgumentException(
            "With ONE_BY_RGB the second source must have 3 or 4 bands.");
      }
      numBands = src2.getSampleModel().getNumBands();
      break;
    }

    colorModel = PlanarImage.getDefaultColorModel(src2.getSampleModel().getDataType(), numBands);
    sampleModel = src2.getSampleModel();
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r1 = sources[0].getData(destRect);
    final Raster r2 = sources[1].getData(destRect);

    switch (bandTreatment)
    {
    case ONE_FOR_ONE:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          for (int b = 0; b < numBands; b++)
          {
            final double v = r1.getSampleDouble(x, y, b) * r2.getSampleDouble(x, y, b);
            dest.setSample(x, y, b, v);
          }
        }
      }
      break;
    case ONE_BY_RGB:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          // only multiply the RGB channels.
          for (int b = 0; b < 3; b++)
          {
            double v = r1.getSampleDouble(x, y, 0) * r2.getSampleDouble(x, y, b);
            dest.setSample(x, y, b, v);
          }
          // copy transparent directly if applicable.
          for (int b = 3; b < numBands; b++)
          {
            double v = r2.getSampleDouble(x, y, b);
            dest.setSample(x, y, b, v);
          }
        }
      }
      break;
    }
  }

  @Override
  public Rectangle mapDestRect(Rectangle destRect, int sourceIndex)
  {
    return destRect;
  }

  @Override
  public Rectangle mapSourceRect(Rectangle sourceRect, int sourceIndex)
  {
    return sourceRect;
  }
  
  @Override
  public String toString()
  {
    return String.format("RawMultiplyOpImage Band treatment: %s", bandTreatment.toString());
  }
}
