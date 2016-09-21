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

package org.mrgeo.data.raster;

import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.utils.FloatUtils;

import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.IOException;
import java.nio.*;
import java.util.Arrays;

/**
 *
 */
public class RasterUtils
{

public final static int BYTES_BYTES = 1;
public final static int FLOAT_BYTES = Float.SIZE / Byte.SIZE;
public final static int DOUBLE_BYTES = Double.SIZE / Byte.SIZE;
public final static int LONG_BYTES = Long.SIZE / Byte.SIZE;
public final static int INT_BYTES = Integer.SIZE / Byte.SIZE;
public final static int SHORT_BYTES = Short.SIZE / Byte.SIZE;
public final static int USHORT_BYTES = Character.SIZE / Byte.SIZE;


public static ColorModel createColorModel(final Raster raster)
{
  SampleModel sm = raster.getSampleModel();

  int bands = raster.getNumBands();
  int type = raster.getTransferType();

  ColorSpace cs = bands < 3 ?
      ColorSpace.getInstance(ColorSpace.CS_GRAY) :
      ColorSpace.getInstance(ColorSpace.CS_sRGB);

  boolean alpha = bands == 2 || bands == 4;

  if (sm instanceof ComponentSampleModel)
  {
    return new ComponentColorModel(cs, alpha, false,
        alpha ? Transparency.TRANSLUCENT : Transparency.OPAQUE, type);
  }

  // TODO:  Any more types needed?
  return null;
}

public static boolean isFloatingPoint(int dataType)
{
  switch (dataType) {
    case DataBuffer.TYPE_BYTE:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
    case DataBuffer.TYPE_INT:
      return false;

    case DataBuffer.TYPE_FLOAT:
    case DataBuffer.TYPE_DOUBLE:
      return true;
  }
  throw new IllegalArgumentException("Invalid raster data type: " + dataType);
}

public static boolean isFloatingPoint(MrsPyramid pyramid) throws IOException
{
  return isFloatingPoint(pyramid.getMetadata().getTileType());
}

  /**
 * Scan the list of sources for the one that uses the
 * largest data type and return it.
 */
public static RenderedImage getMostSpecificSource(RenderedImage[] sources)
{
  int useIndex = -1;
  int largestDataType = -1;
  for (int ii = 0; ii < sources.length; ii++)
  {
    RenderedImage ri = sources[ii];
    if (ri != null)
    {
      SampleModel sm = ri.getSampleModel();
      if (sm != null)
      {
        boolean replaceLargest = false;
        int dataType = sm.getDataType();
        if (largestDataType >= 0)
        {
          // See if the current data type is larger
          if (dataType != largestDataType)
          {
            // If either source has undefined type, let's choose the one that takes more storage bits
            if (dataType == DataBuffer.TYPE_UNDEFINED || largestDataType == DataBuffer.TYPE_UNDEFINED)
            {
              if (DataBuffer.getDataTypeSize(dataType) > DataBuffer.getDataTypeSize(largestDataType))
              {
                replaceLargest = true;
              }
            }
            else
            {
              // When choosing the largest data type, unfortunately we can't just use a numeric
              // comparison of the dataType values because the value of USHORT is less than SHORT.
              // And it's probably better
              switch (largestDataType)
              {
              case DataBuffer.TYPE_BYTE:
                replaceLargest = true;
                break;
              case DataBuffer.TYPE_SHORT:
                if (dataType == DataBuffer.TYPE_USHORT ||
                    dataType == DataBuffer.TYPE_INT ||
                    dataType == DataBuffer.TYPE_FLOAT ||
                    dataType == DataBuffer.TYPE_DOUBLE)
                {
                  replaceLargest = true;
                }
                break;
              case DataBuffer.TYPE_USHORT:
                if (dataType == DataBuffer.TYPE_INT ||
                    dataType == DataBuffer.TYPE_FLOAT ||
                    dataType == DataBuffer.TYPE_DOUBLE)
                {
                  replaceLargest = true;
                }
                break;
              case DataBuffer.TYPE_INT:
                if (dataType == DataBuffer.TYPE_FLOAT ||
                    dataType == DataBuffer.TYPE_DOUBLE)
                {
                  replaceLargest = true;
                }
                break;
              case DataBuffer.TYPE_FLOAT:
                if (dataType == DataBuffer.TYPE_DOUBLE)
                {
                  replaceLargest = true;
                }
                break;
              }
            }
          }
        }
        else
        {
          replaceLargest = true;
        }

        if (replaceLargest)
        {
          useIndex = ii;
          largestDataType = dataType;
        }
      }
    }
  }
  if (useIndex >= 0)
  {
    return sources[useIndex];
  }
  return null;
}


public static WritableRaster createCompatibleEmptyRaster(final Raster raster, final int width, final int height,
    final double[] nodata)
{
  final WritableRaster newraster = raster.createCompatibleWritableRaster(width, height);
  fillWithNodata(newraster, nodata);
  return newraster;
}

public static WritableRaster createCompatibleEmptyRaster(final Raster raster, final int width, final int height,
    final double nodata)
{
  final WritableRaster newraster = raster.createCompatibleWritableRaster(width, height);
  fillWithNodata(newraster, nodata);
  return newraster;
}

public static WritableRaster createCompatibleEmptyRaster(final Raster raster, final double nodata)
{
  final WritableRaster newraster = raster.createCompatibleWritableRaster();
  fillWithNodata(newraster, nodata);
  return newraster;
}

public static WritableRaster createEmptyRaster(final int width, final int height,
    final int bands, final int datatype,
    final double nodata)
{
  final WritableRaster raster = createEmptyRaster(width, height, bands, datatype);
  fillWithNodata(raster, nodata);
  return raster;
}
public static WritableRaster createEmptyRaster(final int width, final int height,
    final int bands, final int datatype,
    final double[] nodatas)
{
  final WritableRaster raster = createEmptyRaster(width, height, bands, datatype);
  fillWithNodata(raster, nodatas);
  return raster;
}

public static WritableRaster createEmptyRaster(final int width, final int height,
    final int bands, final int datatype)
{
  // we'll force the empty raster to be a banded model, for simplicity of the code.
  final SampleModel model = new BandedSampleModel(datatype, width, height, bands);
  return Raster.createWritableRaster(model, null);
}

public static WritableRaster createGBRRaster(final int width, final int height)
{
  return Raster.createInterleavedRaster(DataBuffer.TYPE_BYTE, width, height,
      width * 3, 3, new int[]{2, 1, 0}, null);
}

public static WritableRaster createAGBRRaster(final int width, final int height)
{
  return Raster.createInterleavedRaster(DataBuffer.TYPE_BYTE, width, height,
      width * 4, 4, new int[]{3, 2, 1, 0}, null);
}

public static WritableRaster createRGBARaster(final int width, final int height)
{
  return Raster.createInterleavedRaster(DataBuffer.TYPE_BYTE, width, height,
      width * 4, 4, new int[]{0, 1, 2, 3}, null);
}

public static WritableRaster createRGBRaster(final int width, final int height)
{
  return Raster.createInterleavedRaster(DataBuffer.TYPE_BYTE, width, height,
      width * 3, 3, new int[]{0, 1, 2}, null);
}

public static Raster crop(final Raster src, final long tx, final long ty, final long minTx, final long maxTy, final int tilesize)
{
  final int dtx = (int) (tx - minTx);
  final int dty = (int) (maxTy - ty);

  final int x = dtx * tilesize;
  final int y = dty * tilesize;

  final WritableRaster cropped = src.createCompatibleWritableRaster(tilesize, tilesize);
  cropped.setDataElements(0, 0, tilesize, tilesize, src.getDataElements(x, y, tilesize, tilesize, null));

  return cropped;
}

public static void fillWithNodata(final WritableRaster raster, final double nodata)
{
  final int elements = raster.getHeight() * raster.getWidth();

  final int type = raster.getTransferType();
  for (int b = 0; b < raster.getNumBands(); b++)
  {
    switch (type)
    {
    case DataBuffer.TYPE_BYTE:
    case DataBuffer.TYPE_INT:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      final int[] intsamples = new int[elements];
      final int inodata = (int) nodata;
      Arrays.fill(intsamples, inodata);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, intsamples);
      break;
    case DataBuffer.TYPE_FLOAT:
      final float[] floatsamples = new float[elements];

      final float fnodata = (float) nodata;
      Arrays.fill(floatsamples, fnodata);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, floatsamples);
      break;
    case DataBuffer.TYPE_DOUBLE:
      final double[] doublesamples = new double[elements];
      Arrays.fill(doublesamples, nodata);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, doublesamples);
      break;
    default:
      throw new RasterWritable.RasterWritableException(
          "Error trying to get fill pixels in the raster with nodata value. Bad raster data type");
    }
  }
}

public static void fillWithNodata(final WritableRaster raster, final double[] nodata)
{
  if (raster.getNumBands() != nodata.length)
  {
    throw new RasterWritable.RasterWritableException(
        "Error - cannot fill fill " + raster.getNumBands() +
            " band raster with nodata array containing " + nodata.length +
            " values");
  }
  final int elements = raster.getHeight() * raster.getWidth();

  final int type = raster.getTransferType();
  for (int b = 0; b < raster.getNumBands(); b++)
  {
    switch (type)
    {
    case DataBuffer.TYPE_BYTE:
    case DataBuffer.TYPE_INT:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      final int[] intsamples = new int[elements];
      final int inodata = (int)nodata[b];
      Arrays.fill(intsamples, inodata);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, intsamples);
      break;
    case DataBuffer.TYPE_FLOAT:
      final float[] floatsamples = new float[elements];

      final float fnodata = (float)nodata[b];
      Arrays.fill(floatsamples, fnodata);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, floatsamples);
      break;
    case DataBuffer.TYPE_DOUBLE:
      final double[] doublesamples = new double[elements];
      Arrays.fill(doublesamples, nodata[b]);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, doublesamples);
      break;
    default:
      throw new RasterWritable.RasterWritableException(
          "Error trying to get fill pixels in the raster with nodata value. Bad raster data type");
    }
  }
}

private static void copyPixel(final int x, final int y, final int b, final Raster src,
    final WritableRaster dst, Double nodata)
{

  switch (src.getTransferType())
  {
  case DataBuffer.TYPE_BYTE:
  {
    final byte p = (byte) src.getSample(x, y, b);
    if (p != nodata.byteValue())
    {
      dst.setSample(x, y, b, p);
    }
    break;
  }
  case DataBuffer.TYPE_FLOAT:
  {
    final float p = src.getSampleFloat(x, y, b);
    if (FloatUtils.isNotNodata(p, nodata.floatValue()))
    {
      dst.setSample(x, y, b, p);
    }

    break;
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    final double p = src.getSampleDouble(x, y, b);
    if (FloatUtils.isNotNodata(p, nodata.doubleValue()))
    {
      dst.setSample(x, y, b, p);
    }

    break;
  }
  case DataBuffer.TYPE_INT:
  {
    final int p = src.getSample(x, y, b);
    if (p != nodata.intValue())
    {
      dst.setSample(x, y, b, p);
    }

    break;
  }
  case DataBuffer.TYPE_SHORT:
  {
    final short p = (short) src.getSample(x, y, b);
    if (p != nodata.shortValue())
    {
      dst.setSample(x, y, b, p);
    }

    break;
  }
  case DataBuffer.TYPE_USHORT:
  {
    final int p = src.getSample(x, y, b);
    if (p != nodata.shortValue())
    {
      dst.setSample(x, y, b, p);
    }

    break;
  }

  }
}

public static void mosaicTile(final Raster src, final WritableRaster dst, final double[] nodatas)
{
  for (int y = 0; y < src.getHeight(); y++)
  {
    for (int x = 0; x < src.getWidth(); x++)
    {
      for (int b = 0; b < src.getNumBands(); b++)
      {
        copyPixel(x, y, b, src, dst, nodatas[b]);
      }
    }
  }
}

public static void mosaicTile(final Raster src, final WritableRaster dst, final Double[] nodatas)
{
  double[] dnodata = new double[nodatas.length];
  for (int i = 0; i < dnodata.length; i++)
  {
    dnodata[i] = nodatas[i].doubleValue();
  }
  mosaicTile(src, dst, dnodata);
}

public static BufferedImage makeBufferedImage(Raster raster)
{
  WritableRaster wr;
  if (raster instanceof WritableRaster)
  {
    wr = (WritableRaster) raster;
  }
  else
  {
    wr = RasterUtils.makeRasterWritable(raster);
  }

  ColorModel cm = RasterUtils.createColorModel(raster);
  return new BufferedImage(cm, wr, false, null);
}

public static WritableRaster makeRasterWritable(final Raster raster)
{
  if (raster instanceof WritableRaster)
  {
    return (WritableRaster)raster;
  }

  // create a writable raster using the sample model and actual data buffer from the source raster
  return Raster.createWritableRaster(raster.getSampleModel(),
      raster.getDataBuffer(), null);
}

// Scaling algorithm taken from: http://willperone.net/Code/codescaling.php and modified to use
// Rasters. It is an optimized Bresenham's algorithm.
// Interpolated algorithm was http://tech-algorithm.com/articles/bilinear-image-scaling/
// Also used was http://www.compuphase.com/graphic/scale.htm, explaining interpolated
// scaling
public static WritableRaster scaleRaster(final Raster src, final int dstWidth,
    final int dstHeight, final boolean interpolate, final Double nodata)
{
  final int type = src.getTransferType();

  double scaleW = (double) dstWidth / src.getWidth();
  double scaleH = (double) dstHeight / src.getHeight();

  Raster s = src;

  // bresenham's scalar really doesn't like being scaled more than 2x or 1/3x without the
  // possibility of artifacts. But it turns out you can scale, then scale, etc. and get
  // an answer without artifacts. Hence the loop here...
  while (true)
  {
    int dw;
    int dh;

    final double scale = Math.max(scaleW, scaleH);

    if (scale > 2.0)
    {
      dw = (int) (s.getWidth() * 2.0);
      dh = (int) (s.getHeight() * 2.0);

    }
    else if (scale < 0.50)
    {
      dw = (int) (s.getWidth() * 0.50);
      dh = (int) (s.getHeight() * 0.50);
    }
    else
    {
      dw = dstWidth;
      dh = dstHeight;
    }

    final WritableRaster dst = src.createCompatibleWritableRaster(dw, dh);

    switch (type)
    {
    case DataBuffer.TYPE_BYTE:
    case DataBuffer.TYPE_INT:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      if (interpolate)
      {
        scaleRasterInterpInt(s, dst, dw, dh, nodata.intValue());
      }
      else
      {
        scaleRasterNearestInt(s, dst, dw, dh);
      }
      break;
    case DataBuffer.TYPE_FLOAT:
      if (interpolate)
      {
        scaleRasterInterpFloat(s, dst, dw, dh, nodata.floatValue());
      }
      else
      {
        scaleRasterNearestFloat(s, dst, dw, dh);
      }
      break;
    case DataBuffer.TYPE_DOUBLE:
      if (interpolate)
      {
        scaleRasterInterpDouble(s, dst, dw, dh, nodata.doubleValue());
      }
      else
      {
        scaleRasterNearestDouble(s, dst, dw, dh);
      }
      break;
    default:
      throw new RasterWritable.RasterWritableException("Error trying to scale raster. Bad raster data type");
    }

    if (dst.getWidth() == dstWidth && dst.getHeight() == dstHeight)
    {
      return dst;
    }

    s = dst;
    scaleW = (double) dst.getWidth() / s.getWidth();
    scaleH = (double) dst.getHeight() / s.getHeight();
  }
}

public static WritableRaster scaleRasterInterp(final Raster src, final int dstWidth,
    final int dstHeight, final Double nodata)
{
  return scaleRaster(src, dstWidth, dstHeight, true, nodata);
}

public static WritableRaster scaleRasterNearest(final Raster src, final int dstWidth,
    final int dstHeight)
{
  return scaleRaster(src, dstWidth, dstHeight, false, null);
}

private static void scaleRasterInterpDouble(final Raster src, final WritableRaster dst,
    final int dstWidth, final int dstHeight, final double nodata)
{
  final int srcW = src.getWidth();
  final int srcH = src.getHeight();

  final int srcX = src.getMinX();
  final int srcY = src.getMinY();

  final double[] rawInput = new double[srcW * srcH];
  double A, B, C, D;
  double r1, r2;

  int x, y;
  final float x_ratio = (float) srcW / dstWidth;
  final float y_ratio = (float) srcH / dstHeight;
  float x_diff, y_diff;

  int ul, ur, ll, lr;

  for (int band = 0; band < src.getNumBands(); band++)
  {
    src.getSamples(srcX, srcY, srcW, srcH, band, rawInput);

    final double[] rawOutput = new double[dstWidth * dstHeight];

    int offset = 0;
    for (int i = 0; i < dstHeight; i++)
    {
      for (int j = 0; j < dstWidth; j++)
      {
        x = (int) (x_ratio * j);
        y = (int) (y_ratio * i);

        x_diff = (x_ratio * j) - x;
        y_diff = (y_ratio * i) - y;

        ul = y * srcW + x;

        // keep the indexes from going out of bounds.
        if (y < srcH - 1)
        {
          ll = ul + srcW;
        }
        else
        {
          ll = ul;
        }

        if (x < srcW - 1)
        {
          ur = ul + 1;
          lr = ll + 1;
        }
        else
        {
          ur = ul;
          lr = ll;
        }

        A = rawInput[ul];
        B = rawInput[ur];
        C = rawInput[ll];
        D = rawInput[lr];

        if (Double.compare(A, nodata) == 0)
        {
          r1 = B;
        }
        else if (Double.compare(B, nodata) == 0)
        {
          r1 = A;
        }
        else
        {
          r1 = A * (1 - x_diff) + B * (x_diff);
        }

        if (Double.compare(C, nodata) == 0)
        {
          r2 = D;
        }
        else if (Double.compare(D, nodata) == 0)
        {
          r2 = C;
        }
        else
        {
          r2 = C * (1 - x_diff) + D * (x_diff);
        }

        if (Double.compare(r1, nodata) == 0)
        {
          rawOutput[offset] = r2;
        }
        else if (Double.compare(r2, nodata) == 0)
        {
          rawOutput[offset] = r1;
        }
        else
        {
          rawOutput[offset] = r1 * (1 - y_diff) + r2 * y_diff;
        }
        offset++;

        // Y = A(1-w)(1-h) + B(w)(1-h) + C(h)(1-w) + Dwh
      }
    }
    dst.setSamples(0, 0, dstWidth, dstHeight, band, rawOutput);
  }
}

private static void scaleRasterInterpFloat(final Raster src, final WritableRaster dst,
    final int dstWidth, final int dstHeight, final float nodata)
{
  final int srcW = src.getWidth();
  final int srcH = src.getHeight();

  final int srcX = src.getMinX();
  final int srcY = src.getMinY();

  final float[] rawInput = new float[srcW * srcH];
  float A, B, C, D;
  float r1, r2;

  int x, y;
  final float x_ratio = (float) srcW / dstWidth;
  final float y_ratio = (float) srcH / dstHeight;
  float x_diff, y_diff;

  int ul, ur, ll, lr;

  for (int band = 0; band < src.getNumBands(); band++)
  {
    src.getSamples(srcX, srcY, srcW, srcH, band, rawInput);

    final float[] rawOutput = new float[dstWidth * dstHeight];

    int offset = 0;
    for (int i = 0; i < dstHeight; i++)
    {
      for (int j = 0; j < dstWidth; j++)
      {
        x = (int) (x_ratio * j);
        y = (int) (y_ratio * i);

        x_diff = (x_ratio * j) - x;
        y_diff = (y_ratio * i) - y;

        ul = y * srcW + x;

        // keep the indexes from going out of bounds.
        if (y < srcH - 1)
        {
          ll = ul + srcW;
        }
        else
        {
          ll = ul;
        }

        if (x < srcW - 1)
        {
          ur = ul + 1;
          lr = ll + 1;
        }
        else
        {
          ur = ul;
          lr = ll;
        }

        A = rawInput[ul];
        B = rawInput[ur];
        C = rawInput[ll];
        D = rawInput[lr];

        if (Float.compare(A, nodata) == 0)
        {
          r1 = B;
        }
        else if (Float.compare(B, nodata) == 0)
        {
          r1 = A;
        }
        else
        {
          r1 = A * (1 - x_diff) + B * (x_diff);
        }

        if (Float.compare(C, nodata) == 0)
        {
          r2 = D;
        }
        else if (Float.compare(D, nodata) == 0)
        {
          r2 = C;
        }
        else
        {
          r2 = C * (1 - x_diff) + D * (x_diff);
        }

        if (Float.compare(r1, nodata) == 0)
        {
          rawOutput[offset] = r2;
        }
        else if (Float.compare(r2, nodata) == 0)
        {
          rawOutput[offset] = r1;
        }
        else
        {
          rawOutput[offset] = r1 * (1 - y_diff) + r2 * y_diff;
        }
        offset++;

        // Y = A(1-w)(1-h) + B(w)(1-h) + C(h)(1-w) + Dwh
        // rawOutput[offset++] = (A * (1 - x_diff) * (1 - y_diff) + B * (x_diff) * (1 - y_diff) +
        // C *
        // (y_diff) * (1 - x_diff) + D * (x_diff * y_diff));
      }
    }
    dst.setSamples(0, 0, dstWidth, dstHeight, band, rawOutput);
  }
}

private static void scaleRasterInterpInt(final Raster src, final WritableRaster dst,
    final int dstWidth, final int dstHeight, final int nodata)
{
  final int srcW = src.getWidth();
  final int srcH = src.getHeight();

  final int srcX = src.getMinX();
  final int srcY = src.getMinY();

  final int[] rawInput = new int[srcW * srcH];
  int A, B, C, D;
  int r1, r2;

  int x, y;

  final double x_ratio = (double) srcW / dstWidth;
  final double y_ratio = (double) srcH / dstHeight;

  double x_diff, y_diff;

  int ul, ur, ll, lr;

  for (int band = 0; band < src.getNumBands(); band++)
  {
    src.getSamples(srcX, srcY, srcW, srcH, band, rawInput);

    final int[] rawOutput = new int[dstWidth * dstHeight];

    int offset = 0;
    for (int i = 0; i < dstHeight; i++)
    {
      for (int j = 0; j < dstWidth; j++)
      {
        x = (int) (x_ratio * j);
        y = (int) (y_ratio * i);

        x_diff = (x_ratio * j) - x;
        y_diff = (y_ratio * i) - y;

        ul = y * srcW + x;

        // keep the indexes from going out of bounds.
        if (y < srcH - 1)
        {
          ll = ul + srcW;
        }
        else
        {
          ll = ul;
        }

        if (x < srcW - 1)
        {
          ur = ul + 1;
          lr = ll + 1;
        }
        else
        {
          ur = ul;
          lr = ll;
        }

        A = rawInput[ul];
        B = rawInput[ur];
        C = rawInput[ll];
        D = rawInput[lr];

        // Y = A(1-w)(1-h) + B(w)(1-h) + C(h)(1-w) + Dwh

        if (A == nodata)
        {
          r1 = B;
        }
        else if (B == nodata)
        {
          r1 = A;
        }
        else
        {
          r1 = (int) (A * (1 - x_diff) + B * (x_diff));
        }

        if (C == nodata)
        {
          r2 = D;
        }
        else if (D == nodata)
        {
          r2 = C;
        }
        else
        {
          r2 = (int) (C * (1 - x_diff) + D * (x_diff));
        }

        if (r1 == nodata)
        {
          rawOutput[offset] = r2;
        }
        else if (r2 == nodata)
        {
          rawOutput[offset] = r1;
        }
        else
        {
          rawOutput[offset] = (int) (r1 * (1 - y_diff) + r2 * y_diff);
        }

        offset++;

      }
    }
    dst.setSamples(0, 0, dstWidth, dstHeight, band, rawOutput);
  }
}

private static void scaleRasterNearestDouble(final Raster src, final WritableRaster dst,
    final int dstWidth,
    final int dstHeight)
{
  final int srcW = src.getWidth();
  final int srcH = src.getHeight();

  final int srcX = src.getMinX();
  final int srcY = src.getMinY();

  // YD compensates for the x loop by subtracting the width back out
  final int YD = (srcH / dstHeight) * srcW - srcW;
  final int YR = srcH % dstHeight;
  final int XD = srcW / dstWidth;
  final int XR = srcW % dstWidth;

  final double[] rawInput = new double[srcW * srcH];
  for (int band = 0; band < src.getNumBands(); band++)
  {
    src.getSamples(srcX, srcY, srcW, srcH, band, rawInput);

    final double[] rawOutput = new double[dstWidth * dstHeight];

    int outOffset = 0;
    int inOffset = 0;

    for (int y = dstHeight, YE = 0; y > 0; y--)
    {
      for (int x = dstWidth, XE = 0; x > 0; x--)
      {
        rawOutput[outOffset++] = rawInput[inOffset];
        inOffset += XD;
        XE += XR;
        if (XE >= dstWidth)
        {
          XE -= dstWidth;
          inOffset++;
        }
      }
      inOffset += YD;
      YE += YR;
      if (YE >= dstHeight)
      {
        YE -= dstHeight;
        inOffset += srcW;
      }
    }

    dst.setSamples(0, 0, dstWidth, dstHeight, band, rawOutput);
  }
}

private static void scaleRasterNearestFloat(final Raster src, final WritableRaster dst,
    final int dstWidth,
    final int dstHeight)
{
  final int srcW = src.getWidth();
  final int srcH = src.getHeight();

  final int srcX = src.getMinX();
  final int srcY = src.getMinY();

  // YD compensates for the x loop by subtracting the width back out
  final int YD = (srcH / dstHeight) * srcW - srcW;
  final int YR = srcH % dstHeight;
  final int XD = srcW / dstWidth;
  final int XR = srcW % dstWidth;

  final float[] rawInput = new float[srcW * srcH];
  for (int band = 0; band < src.getNumBands(); band++)
  {
    src.getSamples(srcX, srcY, srcW, srcH, band, rawInput);

    final float[] rawOutput = new float[dstWidth * dstHeight];

    int outOffset = 0;
    int inOffset = 0;

    for (int y = dstHeight, YE = 0; y > 0; y--)
    {
      for (int x = dstWidth, XE = 0; x > 0; x--)
      {
        rawOutput[outOffset++] = rawInput[inOffset];
        inOffset += XD;
        XE += XR;
        if (XE >= dstWidth)
        {
          XE -= dstWidth;
          inOffset++;
        }
      }
      inOffset += YD;
      YE += YR;
      if (YE >= dstHeight)
      {
        YE -= dstHeight;
        inOffset += srcW;
      }
    }

    dst.setSamples(0, 0, dstWidth, dstHeight, band, rawOutput);
  }

}

private static void scaleRasterNearestInt(final Raster src, final WritableRaster dst,
    final int dstWidth,
    final int dstHeight)
{
  final int srcW = src.getWidth();
  final int srcH = src.getHeight();

  final int srcX = src.getMinX();
  final int srcY = src.getMinY();

  // YD compensates for the x loop by subtracting the width back out
  final int YD = (srcH / dstHeight) * srcW - srcW;
  final int YR = srcH % dstHeight;
  final int XD = srcW / dstWidth;
  final int XR = srcW % dstWidth;

  final int[] rawInput = new int[srcW * srcH];
  for (int band = 0; band < src.getNumBands(); band++)
  {
    src.getSamples(srcX, srcY, srcW, srcH, band, rawInput);

    final int[] rawOutput = new int[dstWidth * dstHeight];

    int outOffset = 0;
    int inOffset = 0;

    for (int y = dstHeight, YE = 0; y > 0; y--)
    {
      for (int x = dstWidth, XE = 0; x > 0; x--)
      {
        rawOutput[outOffset++] = rawInput[inOffset];
        inOffset += XD;
        XE += XR;
        if (XE >= dstWidth)
        {
          XE -= dstWidth;
          inOffset++;
        }
      }
      inOffset += YD;
      YE += YR;
      if (YE >= dstHeight)
      {
        YE -= dstHeight;
        inOffset += srcW;
      }
    }

    dst.setSamples(0, 0, dstWidth, dstHeight, band, rawOutput);
  }
}

public static void fillWithNodata(final WritableRaster raster, final MrsPyramidMetadata metadata)
{

  int elements = raster.getHeight() * raster.getWidth();
  for (int b = 0; b < raster.getNumBands(); b++)
  {
    switch (raster.getTransferType())
    {
    case DataBuffer.TYPE_BYTE:
    case DataBuffer.TYPE_INT:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      final int[] intsamples = new int[elements];
      Arrays.fill(intsamples, metadata.getDefaultValueInt(b));
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, intsamples);
      break;
    case DataBuffer.TYPE_FLOAT:
      final float[] floatsamples = new float[elements];
      Arrays.fill(floatsamples, metadata.getDefaultValueFloat(b));
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, floatsamples);
      break;
    case DataBuffer.TYPE_DOUBLE:
      final double[] doublesamples = new double[elements];
      Arrays.fill(doublesamples, metadata.getDefaultValueDouble(b));
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, doublesamples);
      break;
    default:
      throw new RasterWritable.RasterWritableException(
          "Error trying to get fill pixels in the raster with nodata value. Bad raster data type");
    }
  }
}

public static void decimate(final Raster parent, final WritableRaster child, final Aggregator aggregator,
    final MrsPyramidMetadata metadata)
{
  final int w = parent.getWidth();
  final int h = parent.getHeight();

  for (int y = 0; y < h; y += 2)
  {
    for (int x = 0; x < w; x += 2)
    {

      for (int b = 0; b < child.getNumBands(); b++)
      {
        switch (child.getTransferType())
        {
        case DataBuffer.TYPE_BYTE:
        case DataBuffer.TYPE_INT:
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          final int[] intsamples = new int[4];
          parent.getSamples(x, y, 2, 2, b, intsamples);
          int intSample = aggregator.aggregate(intsamples, metadata.getDefaultValueInt(b));
          //      ImageStats.updateStats(stats[b],intSample, metadata.getDefaultValue(b));
          child.setSample(x / 2, y / 2, b, intSample);
          break;
        case DataBuffer.TYPE_FLOAT:
          final float[] floatsamples = new float[4];
          parent.getSamples(x, y, 2, 2, b, floatsamples);
          float floatSample = aggregator.aggregate(floatsamples, metadata.getDefaultValueFloat(b));
          //      ImageStats.updateStats(stats[b], floatSample, metadata.getDefaultValue(b));
          child.setSample(x / 2, y / 2, b, floatSample);
          break;
        case DataBuffer.TYPE_DOUBLE:
          final double[] doublesamples = new double[4];
          parent.getSamples(x, y, 2, 2, b, doublesamples);
          double doubleSample = aggregator.aggregate(doublesamples, metadata.getDefaultValueDouble(b));
          //      ImageStats.updateStats(stats[b], doubleSample, metadata.getDefaultValue(b));
          child.setSample(x / 2, y / 2, b, doubleSample);
          break;
        default:
          throw new RasterWritable.RasterWritableException(
              "Error trying to get decimate pixels in the raster. Bad raster data type");
        }
      }
    }
  }
}

public static void decimate(final Raster parent, final WritableRaster child,
    final Aggregator aggregator, final double[] nodatas)
{
  decimate(parent, child, 0, 0, aggregator, nodatas);
}

public static void decimate(final Raster parent, final WritableRaster child, final int startX, final int startY, final Aggregator aggregator, final double[] nodatas)
{
  final int w = parent.getWidth();
  final int h = parent.getHeight();

  for (int y = 0; y < h; y += 2)
  {
    for (int x = 0; x < w; x += 2)
    {
      for (int b = 0; b < child.getNumBands(); b++)
      {
        switch (child.getTransferType())
        {
        case DataBuffer.TYPE_BYTE:
        case DataBuffer.TYPE_INT:
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          final int[] intsamples = new int[4];
          parent.getSamples(x, y, 2, 2, b, intsamples);
          int intSample = aggregator.aggregate(intsamples, (int)nodatas[b]);
          child.setSample(startX + (x / 2), startY + (y / 2), b, intSample);
          break;
        case DataBuffer.TYPE_FLOAT:
          final float[] floatsamples = new float[4];
          parent.getSamples(x, y, 2, 2, b, floatsamples);
          float floatSample = aggregator.aggregate(floatsamples, (float)nodatas[b]);
          child.setSample(startX + (x / 2), startY + (y / 2), b, floatSample);
          break;
        case DataBuffer.TYPE_DOUBLE:
          final double[] doublesamples = new double[4];
          parent.getSamples(x, y, 2, 2, b, doublesamples);
          double doubleSample = aggregator.aggregate(doublesamples, nodatas[b]);
          child.setSample(startX + (x / 2), startY + (y / 2), b, doubleSample);
          break;
        default:
          throw new RasterWritable.RasterWritableException(
              "Error trying to get decimate pixels in the raster. Bad raster data type");
        }
      }
    }
  }
}

public static int getElementSize(final int rasterDataType)
{
  int size = -1;
  switch (rasterDataType)
  {
  case DataBuffer.TYPE_BYTE:
    size = RasterUtils.BYTES_BYTES;
    break;
  case DataBuffer.TYPE_FLOAT:
    size = RasterUtils.FLOAT_BYTES;
    break;
  case DataBuffer.TYPE_DOUBLE:
    size = RasterUtils.DOUBLE_BYTES;
    break;
  case DataBuffer.TYPE_INT:
    size = RasterUtils.INT_BYTES;
    break;
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
    size = RasterUtils.SHORT_BYTES;
    break;
  default:
    throw new RasterWritable.RasterWritableException(
        "Error trying to get element size from raster. Bad raster data type");
  }
  return size;
}

public static double getDefaultNoDataForType(final int rasterDataType)
{
  switch (rasterDataType)
  {
    case DataBuffer.TYPE_BYTE:
      return 255;
    case DataBuffer.TYPE_FLOAT:
      return Float.NaN;
    case DataBuffer.TYPE_DOUBLE:
      return Double.NaN;
    case DataBuffer.TYPE_INT:
      return Integer.MIN_VALUE;
    case DataBuffer.TYPE_SHORT:
      return Short.MIN_VALUE;
    case DataBuffer.TYPE_USHORT:
      return 65536;  // no ushort constant
    default:
      throw new RasterWritable.RasterWritableException(
              "Error trying to get default nodata value from raster. Bad raster data type " + rasterDataType);
  }
}

public static int getElementSize(final Raster r)
{
  return getElementSize(r.getTransferType());
}

public static void addToRaster(WritableRaster r, WritableRaster mr)
{
  addToRaster(r, mr, 1.0);
}

/**
 * @param r
 * @param mr
 */
public static void addToRaster(WritableRaster r, WritableRaster mr, double weight)
{
  for (int y = 0; y < r.getHeight(); y++)
  {
    for (int x = 0; x < r.getWidth(); x++)
    {
      int rx = x + r.getMinX();
      int ry = y + r.getMinY();
      int mrv = mr.getSample(x, y, 0);
      double v = r.getSampleFloat(rx, ry, 0);
      if (Double.isNaN(v) && mrv > 0)
      {
        r.setSample(rx, ry, 0, mrv * weight);
      }
      else
      {
        r.setSample(rx, ry, 0, v + mrv * weight);
      }
      mr.setSample(x, y, 0, 0.0);
    }
  }
}

public static void setToRaster(WritableRaster r, Raster mr)
{
  for (int y = 0; y < r.getHeight(); y++)
  {
    for (int x = 0; x < r.getWidth(); x++)
    {
      int rx = x + r.getMinX();
      int ry = y + r.getMinY();
      int mrv = mr.getSample(x, y, 0);
      if (mrv > 0)
      {
        r.setSample(rx, ry, 0, 0);
      }
    }
  }
}

public static void applyToRaster(WritableRaster r, Raster mr, double v)
{
  for (int y = 0; y < r.getHeight(); y++)
  {
    for (int x = 0; x < r.getWidth(); x++)
    {
      int rx = x + r.getMinX();
      int ry = y + r.getMinY();
      int mrv = mr.getSample(x, y, 0);
      if (mrv > 0)
      {
        r.setSample(rx, ry, 0, v);
      }
    }
  }
}

public static byte[] deinterleave(byte[] data, int numbands)
{
  byte[] deinterlaced = new byte[data.length];

  ByteBuffer[] bands = new ByteBuffer[numbands];
  int bandoffset = data.length / numbands;

  for (int band = 0; band < numbands; band++)
  {
    bands[band] = ByteBuffer.wrap(deinterlaced, band * bandoffset, bandoffset);
  }

  for (int pixel = 0; pixel < data.length; pixel += numbands)
  {
    for (int band = 0; band < numbands; band++)
    {
      bands[band].put(data[pixel]);
    }
  }

  return deinterlaced;
}

public static short[] deinterleave(short[] data, int numbands)
{
  short[] deinterlaced = new short[data.length];

  ShortBuffer[] bands = new ShortBuffer[numbands];
  int bandoffset = data.length / numbands;

  for (int band = 0; band < numbands; band++)
  {
    bands[band] = ShortBuffer.wrap(deinterlaced, band * bandoffset, bandoffset);
  }

  for (int pixel = 0; pixel < data.length; pixel += numbands)
  {
    for (int band = 0; band < numbands; band++)
    {
      bands[band].put(data[pixel]);
    }
  }

  return deinterlaced;
}

public static int[] deinterleave(int[] data, int numbands)
{
  int[] deinterlaced = new int[data.length];

  IntBuffer[] bands = new IntBuffer[numbands];
  int bandoffset = data.length / numbands;

  for (int band = 0; band < numbands; band++)
  {
    bands[band] = IntBuffer.wrap(deinterlaced, band * bandoffset, bandoffset);
  }

  for (int pixel = 0; pixel < data.length; pixel += numbands)
  {
    for (int band = 0; band < numbands; band++)
    {
      bands[band].put(data[pixel]);
    }
  }

  return deinterlaced;
}

public static float[] deinterleave(float[] data, int numbands)
{
  float[] deinterlaced = new float[data.length];

  FloatBuffer[] bands = new FloatBuffer[numbands];
  int bandoffset = data.length / numbands;

  for (int band = 0; band < numbands; band++)
  {
    bands[band] = FloatBuffer.wrap(deinterlaced, band * bandoffset, bandoffset);
  }

  for (int pixel = 0; pixel < data.length; pixel += numbands)
  {
    for (int band = 0; band < numbands; band++)
    {
      bands[band].put(data[pixel]);
    }
  }

  return deinterlaced;
}

public static double[] deinterleave(double[] data, int numbands)
{
  double[] deinterlaced = new double[data.length];

  DoubleBuffer[] bands = new DoubleBuffer[numbands];
  int bandoffset = data.length / numbands;

  for (int band = 0; band < numbands; band++)
  {
    bands[band] = DoubleBuffer.wrap(deinterlaced, band * bandoffset, bandoffset);
  }

  for (int pixel = 0; pixel < data.length; pixel += numbands)
  {
    for (int band = 0; band < numbands; band++)
    {
      bands[band].put(data[pixel]);
    }
  }

  return deinterlaced;
}
}