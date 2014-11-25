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

import javax.media.jai.ImageLayout;
import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.Serializable;
import java.util.Vector;

/**
 * This class contains very simple unary operators. Much easier than creating a
 * bunch of individual classes.
 */
@SuppressWarnings("unchecked")
public final class RawUnaryOpImage extends OpImage implements Serializable
{
  private static final long serialVersionUID = 1L;
  private static final double EPSILON = 1e-8;
  public enum Operator {
    Negative, Abs, Sine, Cosine, Tangent, IsNull
  }

  Operator _op;
  private double noDataValue;
  private boolean isNoDataNan;
  private double outputNoDataValue;

  static final double _trueThreshold = 0.0001;

  public static RawUnaryOpImage create(RenderedImage src1, String op)
  {
    Operator o;
    if (op.equals("UMinus") || op.equals("-"))
    {
      o = Operator.Negative;
    }
    else if (op.equals("abs"))
    {
      o = Operator.Abs;
    }
    else if (op.equals("sin"))
    {
      o = Operator.Sine;
    }
    else if (op.equals("cos"))
    {
      o = Operator.Cosine;
    }
    else if (op.equals("tan"))
    {
      o = Operator.Tangent;
    }
    else if (op.equals("isNull"))
    {
      o = Operator.IsNull;
    }
    else
    {
      throw new IllegalArgumentException("The operator didn't match any known type.");
    }
    return create(src1, o);
  }

  public static RawUnaryOpImage create(RenderedImage src1, Operator op)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src1);
    ImageLayout layout = null;
    double outNoData = Double.NaN;
    if (op == Operator.IsNull)
    {
      // Since the result of this operation is boolean, we use byte output
      // to save space.
      layout = RasterUtils.createImageLayout(src1, DataBuffer.TYPE_BYTE, 1);
      outNoData = 255;
    }
    else if (op == Operator.Negative || op == Operator.Abs)
    {
      // For these operators, the output should be the same type as the input, and
      // the output NoData value should be the same.
      layout = RasterUtils.createImageLayout(src1, src1.getSampleModel().getDataType(), 1);
      outNoData = OpImageUtils.getNoData(src1, Double.NaN);
    }
    else
    {
      // The output for sin, cos and tan will always be double. And because of the
      // nature of the these computations, we will use NaN for the output NoData value.
      layout = RasterUtils.createImageLayout(src1, DataBuffer.TYPE_DOUBLE, 1);
      outNoData = Double.NaN;
    }
    return new RawUnaryOpImage(sources, op, layout, outNoData);
  }

  @SuppressWarnings("rawtypes")
  private RawUnaryOpImage(Vector sources, Operator op, ImageLayout layout, double outputNoData)
  {
    super(sources, layout, null, false);

    _op = op;
    RenderedImage src = (RenderedImage) sources.get(0);
    noDataValue = OpImageUtils.getNoData(src, Double.NaN);
    isNoDataNan = Double.isNaN(this.noDataValue);
    outputNoDataValue = outputNoData;
    OpImageUtils.setNoData(this, outputNoData);
  }

  private boolean isNoData(double value)
  {
    if (isNoDataNan)
    {
      return Double.isNaN(value);
    }
    return ((value >= noDataValue - EPSILON) && (value <= noDataValue + EPSILON));
  }

  // TODO: make this work for multiband images
  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r1 = sources[0].getData(destRect);

    switch (_op)
    {
    case Negative:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v = r1.getSampleDouble(x, y, 0);
          if (isNoData(v))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            dest.setSample(x, y, 0, -v);
          }
        }
      }
      break;
    case Abs:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v = r1.getSampleDouble(x, y, 0);
          if (isNoData(v))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            dest.setSample(x, y, 0, Math.abs(v));
          }
        }
      }
      break;
    case Sine:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v = r1.getSampleDouble(x, y, 0);
          if (isNoData(v))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            dest.setSample(x, y, 0, Math.sin(v));
          }
        }
      }
      break;
    case Cosine:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v = r1.getSampleDouble(x, y, 0);
          if (isNoData(v))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            dest.setSample(x, y, 0, Math.cos(v));
          }
        }
      }
      break;
    case Tangent:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v = r1.getSampleDouble(x, y, 0);
          if (isNoData(v))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            dest.setSample(x, y, 0, Math.tan(v));
          }
        }
      }
      break;
    case IsNull:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v = r1.getSampleDouble(x, y, 0);
          dest.setSample(x, y, 0, isNoData(v) ?
                  1 : 0);
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
    return String.format("RawCosOpImage");
  }
}
