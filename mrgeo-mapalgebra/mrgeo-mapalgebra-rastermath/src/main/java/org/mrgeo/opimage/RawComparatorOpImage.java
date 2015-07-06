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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

@SuppressWarnings("unchecked")
public final class RawComparatorOpImage extends OpImage implements Serializable
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(RawComparatorOpImage.class);

  private static final long serialVersionUID = 1L;
  private static final double EPSILON = 1e-8;

  public enum Operator {
    LessThan, GreaterThan, Equals, LessThanEquals, GreaterThanEquals, NotEqual, And, Or, Xor
  }

  Operator _op;
  private double noDataValue1;
  private double noDataValue2;
  private boolean isNoData1Nan;
  private boolean isNoData2Nan;
  private int outputNoData = 255;

  static final double _trueThreshold = 0.0001;

  public static RawComparatorOpImage create(RenderedImage src1, RenderedImage src2, String op)
  {
    Operator o;
    if (op.equals("<") || op.equals("lt"))
    {
      o = Operator.LessThan;
    }
    else if (op.equals(">") || op.equals("gt"))
    {
      o = Operator.GreaterThan;
    }
    else if (op.equals("==") || op.equals("eq"))
    {
      o = Operator.Equals;
    }
    else if (op.equals("<=") || op.equals("le"))
    {
      o = Operator.LessThanEquals;
    }
    else if (op.equals(">=") || op.equals("ge"))
    {
      o = Operator.GreaterThanEquals;
    }
    else if (op.equals("!=") || op.equals("^=") || op.equals("<>") || op.equals("ne"))
    {
      o = Operator.NotEqual;
    }
    else if (op.equals("&&") || op.equals("&") || op.equals("and"))
    {
      o = Operator.And;
    }
    else if (op.equals("||") || op.equals("|") || op.equals("or"))
    {
      o = Operator.Or;
    }
    else if (op.equals("!") || op.equals("xor"))
    {
      o = Operator.Xor;
    }
    else
    {
      throw new IllegalArgumentException("The operator didn't match any known type. (" + op + ")");
    }
    return create(src1, src2, o);
  }


  public static RawComparatorOpImage create(RenderedImage src1, RenderedImage src2, Operator op)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src1);
    sources.add(src2);
    
    ImageLayout layout = RasterUtils.createImageLayout(src1, DataBuffer.TYPE_BYTE, 1);
    return new RawComparatorOpImage(sources, layout, op);
  }

  @SuppressWarnings("rawtypes")
  private RawComparatorOpImage(Vector sources, ImageLayout layout, Operator op)
  {
    super(sources, layout, null, false);

    _op = op;
    RenderedImage src1 = (RenderedImage)sources.elementAt(0);
    RenderedImage src2 = (RenderedImage)sources.elementAt(1);
    noDataValue1 = OpImageUtils.getNoData(src1, Double.NaN);
    noDataValue2 = OpImageUtils.getNoData(src2, Double.NaN);
    isNoData1Nan = Double.isNaN(this.noDataValue1);
    isNoData2Nan = Double.isNaN(this.noDataValue2);
    outputNoData = 255;
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

  // TODO: make this work for multiband images
  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    final Raster r1 = sources[0].getData(destRect);
    final Raster r2 = sources[1].getData(destRect);

    // I've broken out the for loops for performance reasons. Yes, there are a
    // lot of other things that could be done, but this helps -- maybe.
    switch (_op)
    {
    case LessThan:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (v1 < v2) ? 1 : 0;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case GreaterThan:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (v1 > v2) ? 1 : 0;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case Equals:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (v1 == v2) ? 1 : 0;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case LessThanEquals:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);

          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (v1 <= v2) ? 1 : 0;
          }

          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case GreaterThanEquals:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (v1 >= v2) ? 1 : 0;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case NotEqual:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (v1 == v2) ? 0 : 1;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case And:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (valueEquals(v1, 0.0) || valueEquals(v2, 0.0)) ? 0 : 1;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case Or:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            v = (!valueEquals(v1, 0.0) || !valueEquals(v2, 0.0)) ? 1 : 0;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    case Xor:
      for (int y = destRect.y; y < destRect.y + destRect.height; y++)
      {
        for (int x = destRect.x; x < destRect.x + destRect.width; x++)
        {
          final double v;
          final double v1 = r1.getSampleDouble(x, y, 0);
          final double v2 = r2.getSampleDouble(x, y, 0);
          if (isNoData1(v1) || isNoData2(v2))
          {
            v = outputNoData;
          }
          else
          {
            boolean v1False = valueEquals(v1, 0.0);
            boolean v2False = valueEquals(v2, 0.0);
            v = (v1False == v2False) ? 0 : 1;
          }
          dest.setSample(x, y, 0, v);
        }
      }
      break;
    }
  }

  private static boolean valueEquals(double v1, double v2)
  {
    return (((v1 - EPSILON) <= v2) && (v2 <= (v1 + EPSILON)));
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
    return String.format("RawComparatorOpImage");
  }
}
