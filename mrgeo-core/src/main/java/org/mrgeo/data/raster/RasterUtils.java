/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.raster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;

import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
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


public static class RasterSize
{
  private int widthInPixels;
  private int heightInPixels;
  private int zoom;

  public RasterSize(int widthInPixels, int heightInPixels, int zoom)
  {
    this.widthInPixels = widthInPixels;
    this.heightInPixels = heightInPixels;
    this.zoom = zoom;
  }

  public int getWidthInPixels() { return widthInPixels; }
  public int getHeightInPixels() { return heightInPixels; }
  public int getZoom() { return zoom; }
}

public static RasterSize getMaxPixelsForSize(int sizeInKb,
                                             Bounds bounds,
                                             int bytesPerPixelPerBand,
                                             int bands,
                                             int tilesize)
{
  long sizeInBytes = (long)(1024) * (long)sizeInKb;
  long totalPixels = sizeInBytes / ((long)bytesPerPixelPerBand * (long)bands);
  double wToH = bounds.width() / bounds.height();
  // Given:
  //   1. totalPixels = heightInPx * widthInPixels
  //   2. widthInPx = heightInPx * wToH
  // Conclusion:
  //   totalPixels = heightInPx * (heightInPx * wToH)
  //   => heightInPx = sqrt(totalPixels / wToH)
  int heightInPx = (int)Math.sqrt(totalPixels / wToH);
  int widthInPx = (int)(heightInPx * wToH);
  double resolution = bounds.height() / (double)heightInPx;
  int zoom = TMSUtils.zoomForPixelSize(resolution, tilesize);
  return new RasterSize(widthInPx, heightInPx, zoom);
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

public static BufferedImage makeBufferedImage(Raster raster)
{
  WritableRaster wr;
  if (raster instanceof WritableRaster)
  {
    wr = (WritableRaster) raster;
  }
  else
  {
    wr = Raster.createWritableRaster(raster.getSampleModel(),
        raster.getDataBuffer(), null);
  }

  ColorModel cm = RasterUtils.createColorModel(raster);
  return new BufferedImage(cm, wr, false, null);
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

private static void fillWithNodata(final WritableRaster raster, final double nodata)
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

private static void fillWithNodata(final WritableRaster raster, final double[] nodata)
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
      final int inodata = (int) nodata[b];
      Arrays.fill(intsamples, inodata);
      raster.setSamples(0, 0, raster.getWidth(), raster.getHeight(), b, intsamples);
      break;
    case DataBuffer.TYPE_FLOAT:
      final float[] floatsamples = new float[elements];

      final float fnodata = (float) nodata[b];
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

}