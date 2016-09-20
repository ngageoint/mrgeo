package org.mrgeo.data.raster;

import org.mrgeo.utils.ByteArrayUtils;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;

public abstract class MrGeoRaster
{
private final static int VERSION_OFFSET = 0;    // start
private final static int WIDTH_OFFSET = 1;      // byte (VERSION)
private final static int HEIGHT_OFFSET = 5;     // byte (VERSION) + int (width)
private final static int BANDS_OFFSET = 9;      // byte (VERSION) + int (width) + int (height)
private final static int DATATYPE_OFFSET = 11;  // byte (VERSION) + int (width) + int (height) + short (bands)
final static int HEADER_LEN = 12;       // data offset: byte (VERSION) + int (width) + int (height) + short (bands) + byte (datatype)


private final static byte VERSION = 0x03;  // MUST NOT BE 0!
private final int width;
private final int height;
private final int bands;
private final int datatype;

final byte[] data;
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

static MrGeoRaster createRaster(byte[] data)
{
  final int[] header = MrGeoRaster.readHeader(data);
  return createRaster(header[1], header[2], header[3], header[4], data, header[5]);
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

  MrGeoRaster row = MrGeoRaster.createEmptyRaster(width, 1, 1, datatype);

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

  int headerlen = bandoffset;
  int len = row.data.length - headerlen;

  for (int b = 0; b < bands; b++)
  {
    for (int y = 0; y < height; y++)
    {
      int offset = calculateByteOffset(0, y, b);

      System.arraycopy(row.data, headerlen, data, offset, len);
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


abstract int bytesPerPixel();

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



}
