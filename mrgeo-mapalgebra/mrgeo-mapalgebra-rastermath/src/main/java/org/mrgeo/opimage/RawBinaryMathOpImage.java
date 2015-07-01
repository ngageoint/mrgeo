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

import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.raster.RasterUtils;

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
public final class RawBinaryMathOpImage extends OpImage implements Serializable
{
  private static final long serialVersionUID = 1L;
  //RenderedImage _ce1, _ce2;

  public enum Operator {
    Pow, Multiply, Divide, Add, Subtract
  }

  Operator _op;
  private double noDataValue1;
  private boolean isNoData1Nan;
  private double noDataValue2;
  private boolean isNoData2Nan;
  private double outputNoDataValue;

  public static Operator convertToOperator(String op)
  {
    Operator o;
    if (op.equals("pow") || op.equals("^"))
    {
      o = Operator.Pow;
    }
    else if (op.equals("*"))
    {
      o = Operator.Multiply;
    }
    else if (op.equals("/"))
    {
      o = Operator.Divide;
    }
    else if (op.equals("+"))
    {
      o = Operator.Add;
    }
    else if (op.equals("-"))
    {
      o = Operator.Subtract;
    }
    else
    {
      throw new IllegalArgumentException("The operator didn't match any known type.");
    }

    return o;
  }


  public static RawBinaryMathOpImage create(RenderedImage src1, RenderedImage src2,
      String op)
  {
    return create(src1, src2, convertToOperator(op));
  }

  public static RawBinaryMathOpImage create(RenderedImage src1, RenderedImage src2,
      Operator op)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src1);
    sources.add(src2);

    RenderedImage[] sourceArray = new RenderedImage[] { src1, src2 };
    RenderedImage modelSource = RasterUtils.getMostSpecificSource(sourceArray);
    ImageLayout layout = null;
    if (modelSource != null && modelSource.getSampleModel() != null)
    {
      layout = RasterUtils.createImageLayout(modelSource, modelSource.getSampleModel().getDataType(), 1);
      // TODO: If there are two "int" type inputs, and neither of them has a nodata value specified,
      // using Double.NaN below is not the right answer. The default nodata value passed below
      // as the second argument should really be set differently based on the data type of the
      // modelSource.
      double outNoData = OpImageUtils.getNoData(modelSource, Double.NaN);
      return new RawBinaryMathOpImage(sources, layout, op, outNoData);
    }
      throw new IllegalArgumentException("Unable to to create a RawBinaryMathOpImage. Neither source " +
          "could be used to determine the image layout of the output");
  }

  @SuppressWarnings("rawtypes")
  private RawBinaryMathOpImage(Vector sources, ImageLayout layout,
      Operator op, double outputNoData)
  {
    super(sources, layout, null, false);

    _op = op;

    RenderedImage src1 = (RenderedImage) sources.get(0);
    this.noDataValue1 = OpImageUtils.getNoData(src1, Double.NaN);
    RenderedImage src2 = (RenderedImage) sources.get(1);
    this.noDataValue2 = OpImageUtils.getNoData(src2, Double.NaN);

//    if (src1.getWidth() == 1 && src1.getHeight() == 1)
//    {
//      _ce1 = ConstantExtenderOpImage.create(src1, getWidth(), getHeight());
//    }
//    else
//    {
//      _ce1 = ConstantExtenderOpImage.create(src1, getWidth(), getHeight(), Double.NaN);
//    }
//
//    if (src2.getWidth() == 1 && src2.getHeight() == 1)
//    {
//      _ce2 = ConstantExtenderOpImage.create(src2, getWidth(), getHeight());
//    }
//    else
//    {
//      _ce2 = ConstantExtenderOpImage.create(src2, getWidth(), getHeight(), Double.NaN);
//    }
    
    this.isNoData1Nan = Double.isNaN(this.noDataValue1);
    this.isNoData2Nan = Double.isNaN(this.noDataValue2);
    this.outputNoDataValue = outputNoData;
    OpImageUtils.setNoData(this, outputNoData);
  }

  private boolean isNoData1(double value)
  {
    return OpImageUtils.isNoData(value, this.noDataValue1, this.isNoData1Nan);
  }

  private boolean isNoData2(double value)
  {
    return OpImageUtils.isNoData(value, this.noDataValue2, this.isNoData2Nan);
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
//    final Raster r1 = _ce1.getData(destRect);
//    final Raster r2 = _ce2.getData(destRect);
    
    final Raster r1 = sources[0].getData(destRect);
    final Raster r2 = sources[1].getData(destRect);

    switch (_op)
    {
    case Pow:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double v1 = r1.getSampleDouble(x, y, 0);
          if (isNoData1(v1))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            double v2 = r2.getSampleDouble(x, y, 0);
            if (isNoData2(v2))
            {
              dest.setSample(x, y, 0, outputNoDataValue);
            }
            else
            {
              final double v = Math.pow(v1, v2);
              dest.setSample(x, y, 0, v);
            }
          }
        }
      }
      break;
    case Multiply:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double v1 = r1.getSampleDouble(x, y, 0);
          if (isNoData1(v1))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            double v2 = r2.getSampleDouble(x, y, 0);
            if (isNoData2(v2))
            {
              dest.setSample(x, y, 0, outputNoDataValue);
            }
            else
            {
              final double v = v1 * v2;
              dest.setSample(x, y, 0, v);
            }
          }
        }
      }
      break;
    case Divide:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double v1 = r1.getSampleDouble(x, y, 0);
          if (isNoData1(v1))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            double v2 = r2.getSampleDouble(x, y, 0);
            if (isNoData2(v2))
            {
              dest.setSample(x, y, 0, outputNoDataValue);
            }
            else
            {
              final double v = v1 / v2;
              dest.setSample(x, y, 0, v);
            }
          }
        }
      }
      break;
    case Add:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double v1 = r1.getSampleDouble(x, y, 0);
          if (isNoData1(v1))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            double v2 = r2.getSampleDouble(x, y, 0);
            if (isNoData2(v2))
            {
              dest.setSample(x, y, 0, outputNoDataValue);
            }
            else
            {
              final double v = v1 + v2;
              dest.setSample(x, y, 0, v);
            }
          }
        }
      }
      break;
    case Subtract:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          double v1 = r1.getSampleDouble(x, y, 0);
          if (isNoData1(v1))
          {
            dest.setSample(x, y, 0, outputNoDataValue);
          }
          else
          {
            double v2 = r2.getSampleDouble(x, y, 0);
            if (isNoData2(v2))
            {
              dest.setSample(x, y, 0, outputNoDataValue);
            }
            else
            {
              final double v = v1 - v2;
              dest.setSample(x, y, 0, v);
            }
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
    return String.format("RawBinaryMathOpImage");
  }
}
