/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.core;

import javax.media.jai.ImageLayout;
import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;
import javax.media.jai.operator.ExtremaDescriptor;
import javax.media.jai.operator.RescaleDescriptor;
import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.io.IOException;

/**
 * 
 */
public class ScalarToGrayDescriptor
{
  public static double[] calculateExtrema(RenderedOp image)
  {
    double[] result = { -1, -1 };
    Object extrema = image.getProperty("extrema");
    // if it has already been calculated, use it.
    if (!(extrema instanceof double[][]))
    {
      final Integer ONE = new Integer(1);
      image = ExtremaDescriptor.create(image, // The source image.
          null, // The region of the image to scan. Default to all.
          ONE, // The horizontal sampling rate. Default to 1.
          ONE, // The vertical sampling rate. Default to 1.
          null, // Whether to store extrema locations. Default to false.
          ONE, // Maximum number of run length codes to store.
          null);
      extrema = image.getProperty("extrema");
    }
    double[][] v = (double[][]) extrema;
    result[0] = v[0][0];
    result[1] = v[1][0];

    return result;
  }

  /**
   * Creates a rendering op that returns an RGB version of a grey scale image
   * 
   * @param input
   * @param bounds
   * @param width
   * @param height
   * @return
   * @throws IOException
   */
  public static RenderedOp create(RenderedImage scalar, double[] extrema) throws IOException
  {
    final double[] scale = new double[1];
    final double[] offset = new double[1];
    scale[0] = 255 / (extrema[1] - extrema[0]);
    offset[0] = extrema[0] * scale[0];
    RenderingHints hints = (RenderingHints) JAI.getDefaultInstance().getRenderingHints().clone();
    if (scalar instanceof RenderedOp)
    {
      hints = ((RenderedOp) scalar).getRenderingHints();
    }
    // Creates the new color model.
    final ColorModel oldCm = scalar.getColorModel();
    final ColorModel newCm = new ComponentColorModel(oldCm.getColorSpace(), oldCm.hasAlpha(), // If
        // true,
        // supports
        // transparency.
        oldCm.isAlphaPremultiplied(), // If true, alpha is premultiplied.
        oldCm.getTransparency(), // What alpha values can be represented.
        DataBuffer.TYPE_BYTE); // Type of primitive array used to represent
    // pixel.
    // Creating the final image layout which should allow us to change color
    // model.
    ImageLayout layout;
    final Object candidate = hints.get(JAI.KEY_IMAGE_LAYOUT);
    if (candidate instanceof ImageLayout)
    {
      layout = (ImageLayout) candidate;
    }
    else
    {
      layout = new ImageLayout();
    }

    layout.setColorModel(newCm);
    layout.setSampleModel(newCm.createCompatibleSampleModel(scalar.getWidth(), scalar.getHeight()));
    hints.put(JAI.KEY_IMAGE_LAYOUT, layout);

    RenderedOp rescaled = RescaleDescriptor.create(scalar, // The source image.
        scale, // The per-band constants to multiply by.
        offset, // The per-band offsets to be added.
        hints); // The rendering hints.

    return rescaled;
  }
}
