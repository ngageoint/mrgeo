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

package org.mrgeo.opimage;

import org.mrgeo.rasterops.OpImageUtils;

import javax.media.jai.FloatDoubleColorModel;
import javax.media.jai.ImageLayout;
import javax.media.jai.PlanarImage;
import javax.media.jai.SourcelessOpImage;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ConstantOpImage extends SourcelessOpImage implements Serializable
{
  private static final long serialVersionUID = 1L;
  private double value;
  // We'll change this dynamically to convince PlanarImage that we accept
  // anything.
  private Rectangle bounds;
  private double[] _defaultValue = new double[1];
  
  public static ConstantOpImage create(double value, int tilesize)
  {
    FloatDoubleColorModel colorModel = new FloatDoubleColorModel(
        ColorSpace.getInstance(ColorSpace.CS_GRAY), false, false, 
        Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    
    SampleModel sampleModel = colorModel.createCompatibleSampleModel(tilesize, tilesize);

    ImageLayout layout = new ImageLayout(0, 0, tilesize, tilesize, 0, 0, tilesize, tilesize, sampleModel, colorModel);
    return new ConstantOpImage(layout, null, sampleModel, value);
  }

  @SuppressWarnings("rawtypes")
  public ConstantOpImage(ImageLayout layout, Map configuration, SampleModel sampleModel,
      double value)
  {
    super(layout, configuration, sampleModel, 0, 0, sampleModel.getWidth(), sampleModel.getHeight());

    this.value = value;
    _defaultValue[0] = value;

    bounds = super.getBounds();
    OpImageUtils.setNoData(this, Double.NaN);
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    double[] d = { value };
    for (int x = (int) destRect.getMinX(); x < destRect.getMinX() + destRect.getWidth(); x++)
    {
      for (int y = (int)destRect.getMinY(); y < destRect.getMinY() + destRect.getHeight(); y++)
      {
        dest.setPixel(x, y, d);
      }
    }
  }

  @Override
  public Rectangle getBounds()
  {
    return bounds;
  }

//  @Override
//  public Raster getData(Rectangle region)
//  {
//    WritableRaster wr;
//    
//    if (region == null)
//    {
//      wr = getColorModel().createCompatibleWritableRaster(bounds.width, bounds.height);
//      wr = wr.createWritableTranslatedChild(bounds.x, bounds.y);
//    }
//    else
//    {
//      wr = getColorModel().createCompatibleWritableRaster(region.width, region.height);
//      wr = wr.createWritableTranslatedChild(region.x, region.y);
//    }
//
//    computeRect(new PlanarImage[0], wr, region);
//
//    return wr;
//  }

  @Override
  public String toString()
  {
    return String.format("ConstantOpImage value: %f", value);
  }
}
