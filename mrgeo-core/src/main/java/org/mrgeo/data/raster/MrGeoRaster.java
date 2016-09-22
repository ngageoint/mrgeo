package org.mrgeo.data.raster;

import org.apache.commons.lang3.NotImplementedException;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.data.raster.Interpolator.Bilinear;
import org.mrgeo.data.raster.Interpolator.Nearest;
import org.mrgeo.utils.ByteArrayUtils;
import org.mrgeo.utils.FloatUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;

import java.awt.*;
import java.awt.image.*;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.*;

public abstract class MrGeoRaster
{
final static int HEADER_LEN = 12;       // data offset: byte (VERSION) + int (width) + int (height) + short (bands) + byte (datatype)
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

  this.bandoffset = width * height;
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype)
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
    throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
  }
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype, double nodata)
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);
  raster.fill(nodata);
  return raster;
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype, double[] nodatas)
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
    throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
  }
}

public static MrGeoRaster fromRaster(Raster raster) throws IOException
{
  MrGeoRaster mrgeo = MrGeoRaster.createEmptyRaster(raster.getWidth(), raster.getHeight(), raster.getNumBands(), raster.getTransferType());

  for (int b = 0; b < raster.getNumBands(); b++)
  {
    for (int y = 0; y < raster.getHeight(); y++)
    {
      for (int x = 0; x < raster.getWidth(); x++)
      {
        switch (mrgeo.datatype())
        {
        case DataBuffer.TYPE_BYTE:
          mrgeo.setPixel(x, y, b, (byte)raster.getSample(x, y, b));
          break;
        case DataBuffer.TYPE_INT:
          mrgeo.setPixel(x, y, b, raster.getSample(x, y, b));
          break;
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          mrgeo.setPixel(x, y, b, (short)raster.getSample(x, y, b));
          break;
        case DataBuffer.TYPE_FLOAT:
          mrgeo.setPixel(x, y, b, raster.getSampleFloat(x, y, b));
          break;
        case DataBuffer.TYPE_DOUBLE:
          mrgeo.setPixel(x, y, b, raster.getSampleDouble(x, y, b));
          break;
        default:
          throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
        }
      }
    }
  }

  return mrgeo;
}

public static MrGeoRaster fromDataset(Dataset dataset)
{
  return fromDataset(dataset, 0, 0, dataset.GetRasterXSize(), dataset.GetRasterYSize());
}

public static MrGeoRaster fromDataset(final Dataset dataset, final int x, final int y, final int width, final int height)
{
  int datatype = dataset.GetRasterBand(1).getDataType();
  int bands = dataset.GetRasterCount();
  int datasize = gdal.GetDataTypeSize(datatype) / 8;

  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, bands, GDALUtils.toRasterDataBufferType(datatype));

  for (int b = 0; b < bands; b++)
  {
    Band band = dataset.GetRasterBand(b + 1); // gdal bands are 1's based
    byte[] data = new byte[datasize * width * height];

    int success = band.ReadRaster(x, y, width, height, data);

    if (success != gdalconstConstants.CE_None)
    {
      System.out.println("Failed reading raster. gdal error: " + success);
    }
    GDALUtils.swapBytes(data, datatype);

    System.arraycopy(data, 0, raster.data, raster.calculateByteOffset(0, 0, b), data.length);
  }

  return raster;
}

static int writeHeader(final int width, final int height, final int bands, final int datatype, byte[] data)
{
  ByteArrayUtils.setByte(VERSION, data, VERSION_OFFSET);
  ByteArrayUtils.setInt(width, data, WIDTH_OFFSET);
  ByteArrayUtils.setInt(height, data, HEIGHT_OFFSET);
  ByteArrayUtils.setShort((short) bands, data, BANDS_OFFSET);
  ByteArrayUtils.setByte((byte) datatype, data, DATATYPE_OFFSET);
  return HEADER_LEN;
}

static int[] readHeader(byte[] data) {
  return new int[] {
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
  final int[] header = MrGeoRaster.readHeader(data);
  return createRaster(header[1], header[2], header[3], header[4], data, header[5]);
}

final public MrGeoRaster createCompatibleRaster(int width, int height)
{
  return createEmptyRaster(width, height, bands, datatype);
}

final public MrGeoRaster createCompatibleEmptyRaster(int width, int height, double nodata)
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);

  MrGeoRaster row = MrGeoRaster.createEmptyRaster(width, 1, 1, datatype);
  for (int x = 0; x < width; x++)
  {
    row.setPixel(x, 0, 0, nodata);
  }

  int headerlen = raster.bandoffset;
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

final public MrGeoRaster createCompatibleEmptyRaster(int width, int height, double[] nodata)
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);

  MrGeoRaster row;

  for (int b = 0; b < bands; b++)
  {
    row = MrGeoRaster.createEmptyRaster(width, 1, 1, datatype);

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

final public byte[] data()
{
  return data;
}

final public int dataoffset()
{
  return dataoffset;
}

final public int datasize()
{
  return data.length - dataoffset;
}

final public MrGeoRaster clip(int x, int y, int width, int height)
{
  MrGeoRaster clipraster = MrGeoRaster.createEmptyRaster(width, height, bands, datatype);

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

final public MrGeoRaster clip(int x, int y, int width, int height, int fromBand)
{
  MrGeoRaster clipraster = MrGeoRaster.createEmptyRaster(width, height, 1, datatype);

  int[] offsets = calculateByteRangeOffset(x, y, x + width, y + height, fromBand);
  int dstOffset = clipraster.calculateByteOffset(0, 0, 0);

  System.arraycopy(data, offsets[0], clipraster.data, dstOffset, offsets[1] - offsets[0]);

  return clipraster;
}

final public void copy(int dstx, int dsty, int width, int height, MrGeoRaster src, int srcx, int srcy)
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

final public void fill(final double value)
{
  MrGeoRaster row = MrGeoRaster.createEmptyRaster(width, 1, 1, datatype);
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

final public void fill(final double[] values)
{
  MrGeoRaster row[] = new MrGeoRaster[bands];

  for (int b = 0; b < bands; b++)
  {
    row[b] = MrGeoRaster.createEmptyRaster(width, 1, 1, datatype);

    for (int x = 0; x < width; x++)
    {
      row[b].setPixel(x, 0, 0, values[b]);
    }
  }

  int headerlen = dataoffset();
  int len = row[0].data.length - headerlen;

  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y++)
    {
      int offset = calculateByteOffset(0, y, b);

      System.arraycopy(row[b].data, headerlen, data, offset, len);
    }
  }
}

final public void fill(final int band, final double value)
{
  MrGeoRaster row = MrGeoRaster.createEmptyRaster(width, 1, 1, datatype);
  for (int x = 0; x < width; x++)
  {
    row.setPixel(x, 0, 0, value);
  }

  int headerlen = bandoffset;
  int len = row.data.length - headerlen;

  for (int y = 0; y < height; y++)
  {
    int offset = calculateByteOffset(0, y, band);

    System.arraycopy(row.data, headerlen, data, offset, len);
  }
}

// Scaling algorithm taken from: http://willperone.net/Code/codescaling.php and modified to use
// Rasters. It is an optimized Bresenham's algorithm.
// Interpolated algorithm was http://tech-algorithm.com/articles/bilinear-image-scaling/
// Also used was http://www.compuphase.com/graphic/scale.htm, explaining interpolated
// scaling
public MrGeoRaster scale(final int dstWidth,
    final int dstHeight, final boolean interpolate, final double[] nodatas)
{

  MrGeoRaster src = this;

  double scaleW = (double) dstWidth / src.width;
  double scaleH = (double) dstHeight / src.height;

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

    final MrGeoRaster dst = createCompatibleRaster(dw, dh);

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
      throw new RasterWritable.RasterWritableException("Error trying to scale raster. Bad raster data type");
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

final public MrGeoRaster reduce(final int xfactor, final int yfactor, Aggregator aggregator, double[] nodatas)
{
  MrGeoRaster child = createCompatibleRaster(width / xfactor, height / yfactor);

  final int subsize = xfactor * yfactor;
  final int[] intsamples = new int[subsize];
  final float[] floatsamples = new float[subsize];
  final double[] doublesamples = new double[subsize];

  int ndx;

  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y += yfactor)
    {
      for (int x = 0; x < width; x += xfactor)
      {
        switch (datatype)
        {
        case DataBuffer.TYPE_BYTE:
        case DataBuffer.TYPE_INT:
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          ndx = 0;
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              intsamples[ndx++] = getPixelInt(xx, yy, b);
            }
          }

          int intSample = aggregator.aggregate(intsamples, (int)nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, intSample);
          break;
        case DataBuffer.TYPE_FLOAT:
          ndx = 0;
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              floatsamples[ndx++] = getPixelInt(xx, yy, b);
            }
          }

          float floatsample = aggregator.aggregate(floatsamples, (float)nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, floatsample);
          break;
        case DataBuffer.TYPE_DOUBLE:
          ndx = 0;
          for (int yy = y; yy < y + yfactor; yy++)
          {
            for (int xx = x; xx < x + xfactor; xx++)
            {
              doublesamples[ndx++] = getPixelInt(xx, yy, b);
            }
          }

          double doublesample = aggregator.aggregate(doublesamples, nodatas[b]);
          child.setPixel(x / xfactor, y / yfactor, b, doublesample);
          break;
        default:
          throw new RasterWritable.RasterWritableException(
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
          final byte p = other.getPixelByte(x, y, b);
          if (getPixelByte(x, y, b) != nodata[b])
          {
            setPixel(x, y, b, p);
          }
          break;
        }
        case DataBuffer.TYPE_FLOAT:
        {
          final float p = other.getPixelFloat(x, y, b);
          if (FloatUtils.isNotNodata(p, (float)nodata[b]))
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_DOUBLE:
        {
          final double p = other.getPixelDouble(x, y, b);
          if (FloatUtils.isNotNodata(p, nodata[b]))
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_INT:
        {
          final int p = other.getPixelInt(x, y, b);
          if (p != (int)nodata[b])
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_SHORT:
        {
          final short p = other.getPixelShort(x, y, b);
          if (p != (short)nodata[b])
          {
            setPixel(x, y, b, p);
          }

          break;
        }
        case DataBuffer.TYPE_USHORT:
        {
          final int p = other.getPixeUShort(x, y, b);
          if (p != (short)nodata[b])
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

final public Dataset toDataset(final Bounds bounds, final double[] nodatas)
{
  int gdaltype = GDALUtils.toGDALDataType(datatype);

  Dataset ds = GDALUtils.createEmptyMemoryRaster(width, height, bands, gdaltype, nodatas);

  double[] xform = new double[6];
  if (bounds != null) {

    xform[0] = bounds.w;
    xform[1] = bounds.width() / width;
    xform[2] = 0;
    xform[3] = bounds.n;
    xform[4] = 0;
    xform[5] = -bounds.height() / height;

    ds.SetProjection(GDALUtils.EPSG4326());
  }
  else
  {
    xform[0] = 0;
    xform[1] = width;
    xform[2] = 0;
    xform[3] = 0;
    xform[4] = 0;
    xform[5] = -height;
  }
  ds.SetGeoTransform(xform);

  byte[] data = new byte[datasize() * width * height];

  for (int b = 0; b < bands; b++)
  {
    Band band = ds.GetRasterBand(b + 1); // gdal bands are 1's based
    band.SetNoDataValue(nodatas[b]);


    System.arraycopy(this.data ,calculateByteOffset(0, 0, b), data, 0, data.length);
    GDALUtils.swapBytes(data, gdaltype);

    int success = band.WriteRaster(0, 0, width, height, data);

    if (success != gdalconstConstants.CE_None)
    {
      System.out.println("Failed writing raster. gdal error: " + success);
    }

  }

  return ds;
}

final public Raster toRaster()
{
  WritableRaster raster = Raster.createBandedRaster(datatype, width, height, bands, new Point(0,0));

  final ByteBuffer rasterBuffer = ByteBuffer.wrap(data);
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
    final byte[] bytedata = new byte[databytes];
    rasterBuffer.get(bytedata);

    raster.setDataElements(0, 0, width, height, bytedata);
    break;
  }
  case DataBuffer.TYPE_FLOAT:
  {
    final FloatBuffer floatbuff = rasterBuffer.asFloatBuffer();
    final float[] floatdata = new float[databytes / bytesPerPixel()];

    floatbuff.get(floatdata);

    raster.setDataElements(0, 0, width, height, floatdata);
    break;
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    final DoubleBuffer doublebuff = rasterBuffer.asDoubleBuffer();
    final double[] doubledata = new double[databytes / bytesPerPixel()];

    doublebuff.get(doubledata);

    raster.setDataElements(0, 0, width, height, doubledata);

    break;
  }
  case DataBuffer.TYPE_INT:
  {
    final IntBuffer intbuff = rasterBuffer.asIntBuffer();
    final int[] intdata = new int[databytes / bytesPerPixel()];

    intbuff.get(intdata);

    raster.setDataElements(0, 0, width, height, intdata);

    break;
  }
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
  {
    final ShortBuffer shortbuff = rasterBuffer.asShortBuffer();
    final short[] shortdata = new short[databytes / bytesPerPixel()];
    shortbuff.get(shortdata);
    raster.setDataElements(0, 0, width, height, shortdata);
    break;
  }
  default:
    throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
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
public abstract void setPixe(int x, int y, int band, int pixel);
public abstract void setPixel(int x, int y, int band, float pixel);
public abstract void setPixel(int x, int y, int band, double pixel);


final int calculateByteOffset(final int x, final int y, final int band)
{
  return ((y * width + x) + band * bandoffset) * bytesPerPixel() + dataoffset;
}

final int[] calculateByteRangeOffset(final int startx, final int starty, final int endx, final int endy, final int band)
{
  final int bpp = bytesPerPixel();
  final int bandoffset = band * this.bandoffset;

  return new int[] {
      ((starty * width + startx) + bandoffset) * bpp + dataoffset,
      ((endy   * width + endx)   + bandoffset) * bpp + dataoffset};
}

final int[] calculateByteRangeOffset(final int startx, final int starty, final int startband,
    final int endx, final int endy, final int endband)
{
  final int bpp = bytesPerPixel();

  return new int[] {
      ((starty * width + startx) + (startband * bandoffset)) * bpp + dataoffset,
      ((endy   * width + endx)   + (endband * bandoffset)) * bpp + dataoffset};
}

abstract int bytesPerPixel();



}
