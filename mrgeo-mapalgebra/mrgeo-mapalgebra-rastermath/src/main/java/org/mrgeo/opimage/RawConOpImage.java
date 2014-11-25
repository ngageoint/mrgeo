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
import org.mrgeo.data.raster.RasterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.ImageLayout;
import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("unchecked")
public final class RawConOpImage extends OpImage implements Serializable
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(RawConOpImage.class);

  private static final long serialVersionUID = 1L;
  private static final double EPSILON = 1e-8;

  private double[] sourceNoData;
  private boolean[] isSourceNoDataNan;
  private double outputNoData;

  @SuppressWarnings("rawtypes")
  public static RawConOpImage create(Vector sources)
  {
    int tileWidth = -1;
    int tileHeight = -1;
    int width = -1;
    int height = -1;

    if (sources.size() < 3 || sources.size() % 2 != 1)
    {
      throw new IllegalArgumentException(
          "There must be at least 3 sources and an odd number of sources.");
    }

    // use the largest tile size and width/height to define the width/height &
    // tile size for this image.
    for (Object o : sources)
    {
      RenderedImage ri = (RenderedImage) o;
      tileWidth = Math.max(ri.getTileWidth(), tileWidth);
      tileHeight = Math.max(ri.getTileHeight(), tileHeight);
      width = Math.max(width, ri.getWidth());
      height = Math.max(height, ri.getHeight());
    }

    Vector v = new Vector();

    for (int i = 0; i < sources.size(); i++)
    {
      RenderedImage src = (RenderedImage) sources.get(i);
      //      if (src.getWidth() == 1 && src.getHeight() == 1)
      //      {
      //        v.add(ConstantExtenderOpImage.create(src, width, height));
      //      }
      //      else if (src.getWidth() < width || src.getHeight() < height)
      //      {
      //        v.add(ConstantExtenderOpImage.create(src, width, height, Double.NaN));
      //      }
      //      else
      {
        v.add(src);
      }
    }

    RenderedImage[] sourcesArray = new RenderedImage[v.size()];
    for (int ii=0; ii < v.size(); ii++)
    {
      sourcesArray[ii] = (RenderedImage)v.get(ii);
    }
    RenderedImage riModel = RasterUtils.getMostSpecificSource(sourcesArray);
    ImageLayout layout = RasterUtils.createImageLayout(riModel, riModel.getSampleModel().getDataType(), 1);
    double outputNoData = OpImageUtils.getNoData(riModel, Double.NaN);
    return new RawConOpImage(v, layout, outputNoData);
  }

  @SuppressWarnings("rawtypes")
  private RawConOpImage(Vector sources, ImageLayout layout, double outNoData)
  {
    super(sources, layout, null, false);
    sourceNoData = new double[sources.size()];
    isSourceNoDataNan = new boolean[sourceNoData.length];
    for (int i = 0; i < sources.size(); i++)
    {
      RenderedImage src = (RenderedImage) sources.get(0);
      sourceNoData[i] = OpImageUtils.getNoData(src, Double.NaN);
      isSourceNoDataNan[i] = Double.isNaN(this.sourceNoData[i]);
    }
    this.outputNoData = outNoData;
    OpImageUtils.setNoData(this, outNoData);
  }

  private boolean isNoData(double value, int sourceIndex)
  {
    if (isSourceNoDataNan[sourceIndex])
    {
      return Double.isNaN(value);
    }
    return ((value >= sourceNoData[sourceIndex] - EPSILON) &&
        (value <= sourceNoData[sourceIndex] + EPSILON));
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    Raster[] r = new Raster[sources.length];
    // we will do lazy loading of the source rasters. We may not need them all
    // and some may be expensive to calculate.
    for (int i = 0; i < sources.length; i++)
    {
      r[i] = null;
    }

    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        boolean done = false;
        for (int i = 0; i < sources.length - 1; i += 2)
        {
          if (r[i] == null)
          {
            r[i] = sources[i].getData(destRect);
          }
          
          final double v = r[i].getSampleDouble(x, y, 0);
          // If the condition layer has a NODATA in it, then set the
          // pixel value of the output to the outputNoDataValue.
          if (isNoData(v, i))
          {
            dest.setSample(x, y, 0, outputNoData);
            done = true;
            break;
          }
          if (Math.abs(v) > 0.0001)
          {
            if (r[i + 1] == null)
            {
              r[i + 1] = sources[i + 1].getData(destRect);
            }
            dest.setSample(x, y, 0, r[i + 1].getSampleDouble(x, y, 0));
            done = true;
            break;
          }
        }
        if (done == false)
        {
          int index = sources.length - 1;
          if (r[index] == null)
          {
            r[index] = sources[index].getData(destRect);
          }
          dest.setSample(x, y, 0, r[index].getSampleDouble(x, y, 0));
        }
      }
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
    return String.format("RawConOpImage");
  }
}
