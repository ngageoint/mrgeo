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

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.data.raster.Interpolator.Bilinear;
import org.mrgeo.data.raster.Interpolator.Nearest;
import org.mrgeo.data.raster.RasterWritable.RasterWritableException;
import org.mrgeo.utils.ByteArrayUtils;
import org.mrgeo.utils.FloatUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.Pixel;
import org.mrgeo.utils.tms.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.Serializable;
import java.nio.*;

public abstract class MrGeoRaster implements Serializable
{
private static Logger log = LoggerFactory.getLogger(MrGeoRaster.class);
final static int HEADER_LEN = 12;
// data offset: byte (VERSION) + int (width) + int (height) + short (bands) + byte (datatype)
private final static int VERSION_OFFSET = 0;    // start
private final static int WIDTH_OFFSET = 1;      // byte (VERSION)
private final static int HEIGHT_OFFSET = 5;     // byte (VERSION) + int (width)
private final static int BANDS_OFFSET = 9;      // byte (VERSION) + int (width) + int (height)
private final static int DATATYPE_OFFSET = 11;  // byte (VERSION) + int (width) + int (height) + short (bands)
private final static byte VERSION = 0x03;  // MUST NOT BE 0!
final byte[] data;
private final int width;
private final int height;
private final int bands;
private final int datatype;
private final int dataoffset;
private final int bandoffset;

MrGeoRaster(int width, int height, int bands, int datatype, byte[] data, int dataoffset)
{
  this.width = width;
  this.height = height;
  this.bands = bands;
  this.datatype = datatype;
  this.data = data;
  this.dataoffset = dataoffset;

  bandoffset = width * height;
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype) throws MrGeoRasterException
{
  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
  {
    return MrGeoByteRaster.createEmptyRaster(width, height, bands);
  }
  case DataBuffer.TYPE_FLOAT:
  {
    return MrGeoFloatRaster.createEmptyRaster(width, height, bands);
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    return MrGeoDoubleRaster.createEmptyRaster(width, height, bands);
  }
  case DataBuffer.TYPE_INT:
  {
    return MrGeoIntRaster.createEmptyRaster(width, height, bands);
  }
  case DataBuffer.TYPE_SHORT:
  {
    return MrGeoShortRaster.createEmptyRaster(width, height, bands);
  }
  case DataBuffer.TYPE_USHORT:
  {
    return MrGeoUShortRaster.createEmptyRaster(width, height, bands);
  }
  default:
    throw new RasterWritableException("Error trying to read raster.  Bad raster data type");
  }
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype, double nodata)
    throws MrGeoRasterException
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);
  raster.fill(nodata);
  return raster;
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype, double[] nodatas)
    throws MrGeoRasterException
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);
  raster.fill(nodatas);
  return raster;
}

public static MrGeoRaster createRaster(int width, int height, int bands, int datatype, byte[] data, int dataOffset)
{
  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
  {
    return new MrGeoByteRaster(width, height, bands, data, dataOffset);
  }
  case DataBuffer.TYPE_FLOAT:
  {
    return new MrGeoFloatRaster(width, height, bands, data, dataOffset);
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    return new MrGeoDoubleRaster(width, height, bands, data, dataOffset);
  }
  case DataBuffer.TYPE_INT:
  {
    return new MrGeoIntRaster(width, height, bands, data, dataOffset);
  }
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
  {
    return new MrGeoShortRaster(width, height, bands, data, dataOffset);
  }
  default:
    throw new RasterWritableException("Error trying to read raster.  Bad raster data type");
  }
}

public static MrGeoRaster fromRaster(Raster raster) throws IOException
{
  MrGeoRaster mrgeo =
      createEmptyRaster(raster.getWidth(), raster.getHeight(), raster.getNumBands(), raster.getTransferType());

  for (int b = 0; b < raster.getNumBands(); b++)
  {
    for (int y = 0; y < raster.getHeight(); y++)
    {
      for (int x = 0; x < raster.getWidth(); x++)
      {
        switch (mrgeo.datatype())
        {
        case DataBuffer.TYPE_BYTE:
          mrgeo.setPixel(x, y, b, (byte) raster.getSample(x, y, b));
          break;
        case DataBuffer.TYPE_INT:
          mrgeo.setPixel(x, y, b, raster.getSample(x, y, b));
          break;
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          mrgeo.setPixel(x, y, b, (short) raster.getSample(x, y, b));
          break;
        case DataBuffer.TYPE_FLOAT:
          mrgeo.setPixel(x, y, b, raster.getSampleFloat(x, y, b));
          break;
        case DataBuffer.TYPE_DOUBLE:
          mrgeo.setPixel(x, y, b, raster.getSampleDouble(x, y, b));
          break;
        default:
          throw new RasterWritableException("Error trying to read raster.  Bad raster data type");
        }
      }
    }
  }

  return mrgeo;
}

public static MrGeoRaster fromDataset(Dataset dataset) throws MrGeoRasterException
{
  return fromDataset(dataset, 0, 0, dataset.GetRasterXSize(), dataset.GetRasterYSize());
}

public static MrGeoRaster fromDataset(Dataset dataset, int x, int y, int width,
    int height)
    throws MrGeoRasterException
{
  int gdaltype = dataset.GetRasterBand(1).getDataType();
  int bands = dataset.GetRasterCount();
  int datasize = gdal.GetDataTypeSize(gdaltype) / 8;

  MrGeoRaster raster = createEmptyRaster(width, height, bands, GDALUtils.toRasterDataBufferType(gdaltype));

  for (int b = 0; b < bands; b++)
  {
    Band band = dataset.GetRasterBand(b + 1); // gdal bands are 1's based
    byte[] data = new byte[datasize * width * height];

    int success = band.ReadRaster(x, y, width, height, width, height, gdaltype, data);


    if (success != gdalconstConstants.CE_None)
    {
      String lastErr = gdal.GetLastErrorMsg();
      log.warn("Failed reading raster. gdal error: " + success + ": " + lastErr + " (This may be a result of the threading problem in GDAL)");
    }
    //GDALUtils.swapBytes(data, gdaltype);

    System.arraycopy(data, 0, raster.data, raster.calculateByteOffset(0, 0, b), data.length);
  }

  return raster;
}

static int writeHeader(int width, int height, int bands, int datatype, byte[] data)
{
  ByteArrayUtils.setByte(VERSION, data, VERSION_OFFSET);
  ByteArrayUtils.setInt(width, data, WIDTH_OFFSET);
  ByteArrayUtils.setInt(height, data, HEIGHT_OFFSET);
  ByteArrayUtils.setShort((short) bands, data, BANDS_OFFSET);
  ByteArrayUtils.setByte((byte) datatype, data, DATATYPE_OFFSET);
  return HEADER_LEN;
}

static int[] readHeader(byte[] data)
{
  return new int[]{
      ByteArrayUtils.getByte(data, VERSION_OFFSET),
      ByteArrayUtils.getInt(data, WIDTH_OFFSET),
      ByteArrayUtils.getInt(data, HEIGHT_OFFSET),
      ByteArrayUtils.getShort(data, BANDS_OFFSET),
      ByteArrayUtils.getByte(data, DATATYPE_OFFSET),
      HEADER_LEN
  };
}

static MrGeoRaster createRaster(byte[] data)
{
  int[] header = readHeader(data);
  return createRaster(header[1], header[2], header[3], header[4], data, header[5]);
}

final public MrGeoRaster createCompatibleRaster(int width, int height) throws MrGeoRasterException
{
  return createEmptyRaster(width, height, bands, datatype);
}

final public MrGeoRaster createCompatibleEmptyRaster(int width, int height, double nodata) throws MrGeoRasterException
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);

  MrGeoRaster row = createEmptyRaster(width, 1, 1, datatype);
  for (int x = 0; x < width; x++)
  {
    row.setPixel(x, 0, 0, nodata);
  }

  int headerlen = raster.dataoffset;
  int len = row.data.length - headerlen;

  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y++)
    {
      int offset = raster.calculateByteOffset(0, y, b);

      System.arraycopy(row.data, headerlen, raster.data, offset, len);
    }
  }

  return raster;
}

final public MrGeoRaster createCompatibleEmptyRaster(int width, int height, double[] nodata) throws MrGeoRasterException
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);

  MrGeoRaster row;

  for (int b = 0; b < bands; b++)
  {
    row = createEmptyRaster(width, 1, 1, datatype);

    for (int x = 0; x < width; x++)
    {
      row.setPixel(x, 0, 0, nodata[b]);
    }

    int headerlen = raster.dataoffset();
    int len = row.data.length - headerlen;

    for (int y = 0; y < height; y++)
    {
      int offset = raster.calculateByteOffset(0, y, b);

      System.arraycopy(row.data, headerlen, raster.data, offset, len);
    }
  }

  return raster;
}

final public int width()
{
  return width;
}

final public int height()
{
  return height;
}

final public int bands()
{
  return bands;
}

final public int datatype()
{
  return datatype;
}

final public int dataoffset()
{
  return dataoffset;
}

final public int datasize()
{
  return data.length - dataoffset;
}

final public int datalength()
{
  return data.length;
}

final public MrGeoRaster clip(int x, int y, int width, int height) throws MrGeoRasterException
{
  MrGeoRaster clipraster = createEmptyRaster(width, height, bands, datatype);

  for (int b = 0; b < bands; b++)
  {
    for (int yy = 0; yy < height; yy++)
    {
      int[] offsets = calculateByteRangeOffset(x, yy + y, x + width, yy + y, b);
      int dstOffset = clipraster.calculateByteOffset(0, yy, b);

      System.arraycopy(data, offsets[0], clipraster.data, dstOffset, offsets[1] - offsets[0]);
    }
  }

  return clipraster;
}

final public MrGeoRaster clip(int x, int y, int width, int height, int band) throws MrGeoRasterException
{
  MrGeoRaster clipraster = createEmptyRaster(width, height, 1, datatype);

  for (int yy = 0; yy < height; yy++)
  {
    int[] offsets = calculateByteRangeOffset(x, yy + y, x + width, yy + y, band);
    int dstOffset = clipraster.calculateByteOffset(0, yy, 0);

    System.arraycopy(data, offsets[0], clipraster.data, dstOffset, offsets[1] - offsets[0]);
  }

  return clipraster;
}

final public void copyFrom(int srcx, int srcy, int width, int height, MrGeoRaster src, int dstx, int dsty)
{
  for (int b = 0; b < bands; b++)
  {
    for (int yy = 0; yy < height; yy++)
    {
      int[] srcoffcets = src.calculateByteRangeOffset(srcx, yy + srcy, srcx + width, yy + srcy, b);
      int dstOffset = calculateByteOffset(dstx, yy + dsty, b);

      System.arraycopy(src.data, srcoffcets[0], data, dstOffset, srcoffcets[1] - srcoffcets[0]);
    }
  }
}

final public void copyFrom(int srcx, int srcy, int srcBand, int width, int height, MrGeoRaster src,
    int dstx, int dsty, int dstBand)
{
  for (int yy = 0; yy < height; yy++)
  {
    int[] srcoffcets = src.calculateByteRangeOffset(srcx, yy + srcy, srcx + width, yy + srcy, srcBand);
    int dstOffset = calculateByteOffset(dstx, yy + dsty, dstBand);

    System.arraycopy(src.data, srcoffcets[0], data, dstOffset, srcoffcets[1] - srcoffcets[0]);
  }
}

final public void fill(double value) throws MrGeoRasterException
{
  MrGeoRaster row = createEmptyRaster(width, 1, 1, datatype);
  for (int x = 0; x < width; x++)
  {
    row.setPixel(x, 0, 0, value);
  }

  int len = row.data.length - dataoffset;

  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y++)
    {
      int offset = calculateByteOffset(0, y, b);

      System.arraycopy(row.data, dataoffset, data, offset, len);
    }
  }

}

final public void fill(double[] values) throws MrGeoRasterException
{
  MrGeoRaster row[] = new MrGeoRaster[bands];

  for (int b = 0; b < bands; b++)
  {
    row[b] = createEmptyRaster(width, 1, 1, datatype);

    for (int x = 0; x < width; x++)
    {
      row[b].setPixel(x, 0, 0, values[b]);
    }
  }

  int headerlen = dataoffset;
  int len = row[0].data.length - headerlen;

  for (int b = 0; b < bands; b++)
  {
    int offset = headerlen + (b * bandoffset * bytesPerPixel());
    for (int y = 0; y < height; y++)
    {
      System.arraycopy(row[b].data, headerlen, data, offset, len);

      offset += len;
    }
  }
}

final public void fill(int band, double value) throws MrGeoRasterException
{
  MrGeoRaster row = createEmptyRaster(width, 1, 1, datatype);
  for (int x = 0; x < width; x++)
  {
    row.setPixel(x, 0, 0, value);
  }

  int headerlen = dataoffset;
  int len = row.data.length - headerlen;

  int offset = headerlen + (band * bandoffset * bytesPerPixel());
  for (int y = 0; y < height; y++)
  {

    System.arraycopy(row.data, headerlen, data, offset, len);
    offset += len;
  }
}

// Scaling algorithm taken from: http://willperone.net/Code/codescaling.php and modified to use
// Rasters. It is an optimized Bresenham's algorithm.
// Interpolated algorithm was http://tech-algorithm.com/articles/bilinear-image-scaling/
// Also used was http://www.compuphase.com/graphic/scale.htm, explaining interpolated
// scaling
public MrGeoRaster scale(int dstWidth,
    int dstHeight, boolean interpolate, double[] nodatas) throws MrGeoRasterException
{

  MrGeoRaster src = this;

  double scaleW = (double) dstWidth / src.width;
  double scaleH = (double) dstHeight / src.height;

  while (true)
  {
    int dw;
    int dh;

    double scale = Math.max(scaleW, scaleH);

    // bresenham's scalar really doesn't like being scaled more than 2x or 1/2x without the
    // possibility of artifacts. But it turns out you can scale, then scale, etc. and get
    // an answer without artifacts. Hence the loop here...
    if (interpolate)
    {
      if (scale > 2.0)
      {
        dw = (int) (src.width * 2.0);
        dh = (int) (src.height * 2.0);

      }
      else if (scale < 0.50)
      {
        dw = (int) (src.width * 0.50);
        dh = (int) (src.height * 0.50);
      }
      else
      {
        dw = dstWidth;
        dh = dstHeight;
      }
    }
    else
    {
      dw = dstWidth;
      dh = dstHeight;
    }

    MrGeoRaster dst = createCompatibleRaster(dw, dh);

    switch (datatype)
    {
    case DataBuffer.TYPE_BYTE:
    case DataBuffer.TYPE_INT:
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      if (interpolate)
      {
        Bilinear.scaleInt(src, dst, nodatas);
      }
      else
      {
        Nearest.scaleInt(src, dst);
      }
      break;
    case DataBuffer.TYPE_FLOAT:
      if (interpolate)
      {
        Bilinear.scaleFloat(src, dst, nodatas);
      }
      else
      {
        Nearest.scaleFloat(src, dst);
      }
      break;
    case DataBuffer.TYPE_DOUBLE:
      if (interpolate)
      {
        Bilinear.scaleDouble(src, dst, nodatas);
      }
      else
      {
        Nearest.scaleDouble(src, dst);
      }
      break;
    default:
      throw new RasterWritableException("Error trying to scale raster. Bad raster data type");
    }

    if (dst.width == dstWidth && dst.height == dstHeight)
    {
      return dst;
    }

    src = dst;

    scaleW = (double) dstWidth / src.width;
    scaleH = (double) dstHeight / src.height;
  }
}

final public MrGeoRaster reduce(int xfactor, int yfactor, Aggregator aggregator, double[] nodatas)
    throws MrGeoRasterException
{
  MrGeoRaster child = createCompatibleRaster(width / xfactor, height / yfactor);

  int subsize = xfactor * yfactor;
  byte[] bytesamples = new byte[subsize];
  short[] shortsamples = new short[subsize];
  int[] intsamples = new int[subsize];
  float[] floatsamples = new float[subsize];
  double[] doublesamples = new double[subsize];

  int ndx;

  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y += yfactor)
    {
      for (int x = 0; x < width; x += xfactor)
      {
        ndx = 0;
        switch (datatype)
        {
        case DataBuffer.TYPE_BYTE:
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              bytesamples[ndx++] = getPixelByte(xx, yy, b);
            }
          }

          byte bytesample = aggregator.aggregate(bytesamples, (byte) nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, bytesample);
          break;


        case DataBuffer.TYPE_SHORT:
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              shortsamples[ndx++] = getPixelShort(xx, yy, b);
            }
          }

          short shortsample = aggregator.aggregate(shortsamples, (short) nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, shortsample);
          break;
        case DataBuffer.TYPE_USHORT:
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              shortsamples[ndx++] = getPixelShort(xx, yy, b);
            }
          }

          int ushortsample = aggregator.aggregate(shortsamples, (short) nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, ushortsample);
          break;
        case DataBuffer.TYPE_INT:
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              intsamples[ndx++] = getPixelInt(xx, yy, b);
            }
          }

          int intSample = aggregator.aggregate(intsamples, (int) nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, intSample);
          break;
        case DataBuffer.TYPE_FLOAT:
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              floatsamples[ndx++] = getPixelFloat(xx, yy, b);
            }
          }

          float floatsample = aggregator.aggregate(floatsamples, (float) nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, floatsample);
          break;
        case DataBuffer.TYPE_DOUBLE:
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              doublesamples[ndx++] = getPixelDouble(xx, yy, b);
            }
          }

          double doublesample = aggregator.aggregate(doublesamples, nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, doublesample);
          break;
        default:
          throw new RasterWritableException(
              "Error trying to get decimate pixels in the raster. Bad raster data type");
        }
      }
    }
  }

  return child;
}

final public void mosaic(MrGeoRaster other, double[] nodata)
{
  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y++)
    {
      for (int x = 0; x < width; x++)
      {
        switch (datatype)
        {
        case DataBuffer.TYPE_BYTE:
        {
          byte p = other.getPixelByte(x, y, b);
          if (getPixelByte(x, y, b) == (byte) nodata[b])
          {
            setPixel(x, y, b, p);
          }
          break;
        }
        case DataBuffer.TYPE_FLOAT:
        {
          float p = other.getPixelFloat(x, y, b);
          if (FloatUtils.isNotNodata(p, (float) nodata[b]))
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_DOUBLE:
        {
          double p = other.getPixelDouble(x, y, b);
          if (FloatUtils.isNotNodata(p, nodata[b]))
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_INT:
        {
          int p = other.getPixelInt(x, y, b);
          if (getPixelInt(x, y, b) == (int) nodata[b])
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_SHORT:
        {
          short p = other.getPixelShort(x, y, b);
          if (getPixelShort(x, y, b) == (short) nodata[b])
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_USHORT:
        {
          int p = other.getPixeUShort(x, y, b);
          if (getPixeUShort(x, y, b) == (short) nodata[b])
          {
            setPixel(x, y, b, p);
          }

          break;
        }

        }
      }
    }
  }
}

final public Dataset toDataset()
{
  return toDataset(null, null);
}

final public Dataset toDiskBasedDataset(Bounds bounds, double[] nodatas,
                                        int xoffset, int yoffset, int outWidth, int outHeight)
{
  int gdaltype = GDALUtils.toGDALDataType(datatype);
  Dataset ds = GDALUtils.createEmptyDiskBasedRaster(outWidth, outHeight, bands, gdaltype, nodatas);
  return toDataset(ds, gdaltype, bounds, xoffset, yoffset, outWidth, outHeight, nodatas);
}

final public Dataset toDataset(Bounds bounds, double[] nodatas)
{
  int gdaltype = GDALUtils.toGDALDataType(datatype);
  Dataset ds = GDALUtils.createEmptyMemoryRaster(width, height, bands, gdaltype, nodatas);
  return toDataset(ds, gdaltype, bounds, 0, 0, width, height, nodatas);
}

private Dataset toDataset(Dataset ds, int gdaltype, Bounds bounds,
                          int xoffset, int yoffset,
                          int outWidth, int outHeight,
                          double[] nodatas)
{
  double[] xform = new double[6];
  if (bounds != null)
  {

    xform[0] = bounds.w;
    xform[1] = bounds.width() / outWidth;
    xform[2] = 0;
    xform[3] = bounds.n;
    xform[4] = 0;
    xform[5] = -bounds.height() / outHeight;

    ds.SetProjection(GDALUtils.EPSG4326());
  }
  else
  {
    xform[0] = 0;
    xform[1] = outWidth;
    xform[2] = 0;
    xform[3] = 0;
    xform[4] = 0;
    xform[5] = -outHeight;
  }
  ds.SetGeoTransform(xform);

  byte[] rowdata = new byte[bytesPerPixel() * outWidth];

  for (int b = 0; b < bands; b++)
  {
    Band band = ds.GetRasterBand(b + 1); // gdal bands are 1's based
    if (nodatas != null)
    {
      if (b < nodatas.length)
      {
        band.SetNoDataValue(nodatas[b]);
      }
      else
      {
        band.SetNoDataValue(nodatas[nodatas.length - 1]);
      }
    }
    for (int y=0; y < outHeight; y++) {
      System.arraycopy(data, calculateByteOffset(xoffset, y + yoffset, b), rowdata, 0, rowdata.length);
      int success = band.WriteRaster(0, y, outWidth, 1, outWidth, 1, gdaltype, rowdata);
      if (success != gdalconstConstants.CE_None)
      {
        System.out.println("Failed writing raster. gdal error: " + success);
        break;
      }
    }
  }

  return ds;
}

public void copyToDataset(Dataset ds, int dsWidth, int dsHeight, Bounds fullBounds, Bounds bounds,
                          int tilesize, int zoomlevel, int gdaltype) throws IOException
{
  Pixel ulPixelTile = TMSUtils
          .latLonToPixelsUL(bounds.n, bounds.w, zoomlevel, tilesize);
  Pixel ulPixelDS = TMSUtils
          .latLonToPixelsUL(fullBounds.n, fullBounds.w, zoomlevel, tilesize);
  Pixel lrPixelTile = TMSUtils
          .latLonToPixelsUL(bounds.s, bounds.e, zoomlevel, tilesize);
  Pixel lrPixelDS = TMSUtils
          .latLonToPixelsUL(fullBounds.s, fullBounds.e, zoomlevel, tilesize);

  long leftPixel = Math.max(ulPixelDS.px, ulPixelTile.px);
  long rightPixel = Math.min(lrPixelDS.px, lrPixelTile.px);
  long topPixel = Math.max(ulPixelDS.py, ulPixelTile.py);
  long bottomPixel = Math.min(lrPixelDS.py, lrPixelTile.py);
  int xoffset = (ulPixelTile.px < leftPixel) ? (int) (leftPixel - ulPixelTile.px) : 0;
  int yoffset = (ulPixelTile.py < topPixel) ? (int) (topPixel - ulPixelTile.py) : 0;
  int xoffsetWrite = (int)(leftPixel - ulPixelDS.px);
  int yoffsetWrite = (int)(topPixel - ulPixelDS.py);
  int outWidth = (int) (rightPixel - leftPixel);
  int outHeight = (int) (bottomPixel - topPixel);

  byte[] rowdata = new byte[bytesPerPixel() * outWidth];
  for (int b = 0; b < ds.GetRasterCount(); b++) {
    for (int y = 0; y < outHeight; y++) {
      Band band = ds.GetRasterBand(b + 1); // gdal bands are 1's based
      System.arraycopy(data, calculateByteOffset(xoffset, y + yoffset, b), rowdata, 0, rowdata.length);
      int success = band.WriteRaster(xoffsetWrite, y + yoffsetWrite, outWidth, 1, outWidth, 1, gdaltype, rowdata);
      if (success != gdalconstConstants.CE_None) {
        throw new IOException("Failed writing raster. gdal error: " + success);
      }
    }
  }
}

final public Raster toRaster()
{
  WritableRaster raster = RasterUtils.createEmptyRaster(width, height, bands, datatype);

  ByteBuffer rasterBuffer = ByteBuffer.wrap(data);
  rasterBuffer.order(ByteOrder.LITTLE_ENDIAN);

  // skip over the header in the data buffer
  for (int i = 0; i < HEADER_LEN; i++)
  {
    rasterBuffer.get();
  }

  int databytes = data.length - HEADER_LEN;

  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
  {
    // we can't use the byte buffer explicitly because the header info is
    // still in it...
    byte[] bytedata = new byte[databytes];
    rasterBuffer.get(bytedata);

    raster.setDataElements(0, 0, width, height, bytedata);
    break;
  }
  case DataBuffer.TYPE_FLOAT:
  {
    FloatBuffer floatbuff = rasterBuffer.asFloatBuffer();
    float[] floatdata = new float[databytes / bytesPerPixel()];

    floatbuff.get(floatdata);

    raster.setDataElements(0, 0, width, height, floatdata);
    break;
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    DoubleBuffer doublebuff = rasterBuffer.asDoubleBuffer();
    double[] doubledata = new double[databytes / bytesPerPixel()];

    doublebuff.get(doubledata);

    raster.setDataElements(0, 0, width, height, doubledata);

    break;
  }
  case DataBuffer.TYPE_INT:
  {
    IntBuffer intbuff = rasterBuffer.asIntBuffer();
    int[] intdata = new int[databytes / bytesPerPixel()];

    intbuff.get(intdata);

    raster.setDataElements(0, 0, width, height, intdata);

    break;
  }
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
  {
    ShortBuffer shortbuff = rasterBuffer.asShortBuffer();
    short[] shortdata = new short[databytes / bytesPerPixel()];
    shortbuff.get(shortdata);
    raster.setDataElements(0, 0, width, height, shortdata);
    break;
  }
  default:
    throw new RasterWritableException("Error trying to read raster.  Bad raster data type");
  }

  return raster;
}

public abstract byte getPixelByte(int x, int y, int band);

public abstract short getPixelShort(int x, int y, int band);

public abstract short getPixeUShort(int x, int y, int band);

public abstract int getPixelInt(int x, int y, int band);

public abstract float getPixelFloat(int x, int y, int band);

public abstract double getPixelDouble(int x, int y, int band);

public abstract void setPixel(int x, int y, int band, byte pixel);

public abstract void setPixel(int x, int y, int band, short pixel);

public abstract void setPixel(int x, int y, int band, int pixel);

public abstract void setPixel(int x, int y, int band, float pixel);

public abstract void setPixel(int x, int y, int band, double pixel);

final byte[] data()
{
  return data;
}

final int calculateByteOffset(int x, int y, int band)
{
  return ((y * width + x) + band * bandoffset) * bytesPerPixel() + dataoffset;
}

final int[] calculateByteRangeOffset(int startx, int starty, int endx, int endy, int band)
{
  int bpp = bytesPerPixel();
  int bandoffset = band * this.bandoffset;

  return new int[]{
      ((starty * width + startx) + bandoffset) * bpp + dataoffset,
      ((endy * width + endx) + bandoffset) * bpp + dataoffset};
}

final int[] calculateByteRangeOffset(int startx, int starty, int startband,
    int endx, int endy, int endband)
{
  int bpp = bytesPerPixel();

  return new int[]{
      ((starty * width + startx) + (startband * bandoffset)) * bpp + dataoffset,
      ((endy * width + endx) + (endband * bandoffset)) * bpp + dataoffset};
}

public abstract int bytesPerPixel();

public static class MrGeoRasterException extends IOException
{
  private static final long serialVersionUID = 1L;

  private final Exception origException;

  MrGeoRasterException(String msg)
  {
    origException = new Exception(msg);
  }

  @Override
  public void printStackTrace()
  {
    origException.printStackTrace();
  }
}


}
