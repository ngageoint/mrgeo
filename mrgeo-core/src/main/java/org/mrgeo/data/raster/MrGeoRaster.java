package org.mrgeo.data.raster;

import org.mrgeo.utils.ByteArrayUtils;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;

public abstract class MrGeoRaster
{
private final int width;
private final int height;
private final int bands;
private final int datatype;

final byte[] data;
private final int dataoffset;

MrGeoRaster(int width, int height, int bands, int datatype, byte[] data, int dataoffset)
{
  this.width = width;
  this.height = height;
  this.bands = bands;
  this.datatype = datatype;
  this.data = data;
  this.dataoffset = dataoffset;
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands, int datatype)
{
  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
  {
    break;
  }
  case DataBuffer.TYPE_FLOAT:
  {
    return MrGeoFloatRaster.createEmptyRaster(width, height, bands);
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    break;
  }
  case DataBuffer.TYPE_INT:
  {
    break;
  }
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
  {
    return MrGeoShortRaster.createEmptyRaster(width, height, bands);
  }
  default:
    throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
  }

  throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
}

public static MrGeoRaster createRaster(int width, int height, int bands, int datatype, byte[] data, int dataOffset)
{
  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
  {
    break;
  }
  case DataBuffer.TYPE_FLOAT:
  {
    return new MrGeoFloatRaster(width, height, bands, data, dataOffset);
  }
  case DataBuffer.TYPE_DOUBLE:
  {
    break;
  }
  case DataBuffer.TYPE_INT:
  {
    break;
  }
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
  {
    return new MrGeoShortRaster(width, height, bands, data, dataOffset);
  }
  default:
    throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
  }

  throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
}

public static MrGeoRaster fromRaster(Raster raster) throws IOException
{
  RasterWritable.HeaderData header = new RasterWritable.HeaderData(raster.getWidth(), raster.getHeight(),
      raster.getNumBands(), raster.getTransferType());

  int bpp;
  switch (header.datatype)
  {
  case DataBuffer.TYPE_BYTE:
    bpp = 1;
    break;
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
    bpp = 2;
    break;
  case DataBuffer.TYPE_INT:
  case DataBuffer.TYPE_FLOAT:
    bpp = 4;
    break;
  case DataBuffer.TYPE_DOUBLE:
  {
    bpp = 8;
    break;
  }
  default:
    throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
  }

  int dataOffset = RasterWritable.HeaderData.getHeaderLength();

  byte[] data = new byte[header.width * header.height * header.bands * bpp + dataOffset];

  ByteArrayUtils.setInt(header.width, data, 0);
  ByteArrayUtils.setInt(header.height, data, 4);
  ByteArrayUtils.setInt(header.bands, data, 8);
  ByteArrayUtils.setInt(header.datatype, data, 12);

  for (int b = 0; b < header.bands; b++)
  {
    for (int y = 0; y < header.height; y++)
    {
      for (int x = 0; x < header.width; x++)
      {

        int offset = (y * header.width * bpp + x * bpp) + (b * header.height * header.width * bpp) + dataOffset;

        switch (header.datatype)
        {
        case DataBuffer.TYPE_BYTE:
          byte bpixel = (byte)raster.getSample(x, y, b);
          ByteArrayUtils.setByte(bpixel, data, offset);
          break;
        case DataBuffer.TYPE_INT:
          int ipixel = raster.getSample(x, y, b);
          ByteArrayUtils.setInt(ipixel, data, offset);
          break;
        case DataBuffer.TYPE_SHORT:
        case DataBuffer.TYPE_USHORT:
          short spixel = (short)raster.getSample(x, y, b);
          ByteArrayUtils.setShort(spixel, data, offset);
          break;
        case DataBuffer.TYPE_FLOAT:
          float fpixel = raster.getSampleFloat(x, y, b);
          ByteArrayUtils.setFloat(fpixel, data, offset);
          break;
        case DataBuffer.TYPE_DOUBLE:
          double dpixel = raster.getSampleDouble(x, y, b);
          ByteArrayUtils.setDouble(dpixel, data, offset);
          break;
        default:
          throw new RasterWritable.RasterWritableException("Error trying to read raster.  Bad raster data type");
        }
      }
    }
  }

  return MrGeoRaster.createRaster(raster.getWidth(), raster.getHeight(), raster.getNumBands(), raster.getTransferType(),
      data, RasterWritable.HeaderData.getHeaderLength());
}

public MrGeoRaster createCompatibleRaster(int width, int height)
{
  return createEmptyRaster(width, height, bands, datatype);
}


public MrGeoRaster createCompatibleEmptyRaster(int width, int height, double nodata)
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);

  MrGeoRaster row = raster.createCompatibleRaster(width, 1);

  for (int b = 0; b < bands; b++)
  {
    for (int x = 0; x < width; x++)
    {
      row.setPixel(x, 0, b, nodata);
    }
  }

  int headerlen = RasterWritable.HeaderData.getHeaderLength();
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

public MrGeoRaster createCompatibleEmptyRaster(int width, int height, Number[] nodata)
{
  MrGeoRaster raster = createEmptyRaster(width, height, bands, datatype);

  MrGeoRaster row = raster.createCompatibleRaster(width, 1);

  for (int b = 0; b < bands; b++)
  {
    for (int x = 0; x < width; x++)
    {
      row.setPixel(x, 0, b, nodata[b].doubleValue());
    }
  }

  int headerlen = RasterWritable.HeaderData.getHeaderLength();
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


int calculateByteOffset(int x, int y, int band)
{
  final int bpp = bytesPerPixel();
  return (y * width * bpp + x * bpp) + (band * height * width * bpp) + dataoffset;
}

int[] calculateByteRangeOffset(int startx, int starty, int endx, int endy, int band)
{
  final int[] offset = new int[2];

  final int bpp = bytesPerPixel();
  offset[0] = (starty * width * bpp + startx * bpp) + (band * height * width * bpp) + dataoffset;
  offset[1] = (endy * width * bpp + endx * bpp) + (band * height * width * bpp) + dataoffset;

  return offset;
}

public int width()
{
  return width;
}

public int height()
{
  return height;
}

public int bands()
{
  return bands;
}

public int datatype()
{
  return datatype;
}

public byte[] data()
{
  return data;
}

public int dataoffset()
{
  return dataoffset;
}

public MrGeoRaster clip(int x, int y, int width, int height)
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

public void copy(int dstx, int dsty, int width, int height, MrGeoRaster src, int srcx, int srcy)
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
