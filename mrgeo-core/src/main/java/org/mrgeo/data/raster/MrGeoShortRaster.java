package org.mrgeo.data.raster;

import org.mrgeo.utils.ByteArrayUtils;

import java.awt.image.DataBuffer;

class MrGeoShortRaster extends MrGeoRaster
{
private static final int BYTES_PER_PIXEL = 2;

public static MrGeoRaster createEmptyRaster(int width, int height, int bands)
{
  byte[] data = new byte[(width * height * bands * BYTES_PER_PIXEL) + RasterWritable.HeaderData.getHeaderLength()];
  ByteArrayUtils.setInt(width, data, 0);
  ByteArrayUtils.setInt(height, data, 4);
  ByteArrayUtils.setInt(bands, data, 8);
  ByteArrayUtils.setInt(DataBuffer.TYPE_SHORT, data, 12);

  return new MrGeoShortRaster(width, height, bands, data, RasterWritable.HeaderData.getHeaderLength());
}

MrGeoShortRaster(int width, int height, int bands, byte[] data, int dataOffset)
{
  super(width, height, bands, DataBuffer.TYPE_SHORT, data, dataOffset);
}

@Override
int bytesPerPixel()
{
  return BYTES_PER_PIXEL;
}

@Override
public byte getPixelByte(int x, int y, int band)
{
  return (byte)getPixelShort(x, y, band);
}

@Override
public short getPixelShort(int x, int y, int band)
{
  return ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixeUShort(int x, int y, int band)
{
  return (short)getPixelShort(x, y, band);
}

@Override
public int getPixelInt(int x, int y, int band)
{
  return (int)getPixelShort(x, y, band);
}

@Override
public float getPixelFloat(int x, int y, int band)
{
  return (float)getPixelShort(x, y, band);
}
//
@Override
public double getPixelDouble(int x, int y, int band)
{
  return (double)getPixelShort(x, y, band);
}

@Override
public void setPixel(int x, int y, int band, byte pixel)
{
  setPixel(x, y, band, (short)pixel);
}

@Override
public void setPixel(int x, int y, int band, short pixel)
{
  ByteArrayUtils.setShort(pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixe(int x, int y, int band, int pixel)
{
  setPixel(x, y, band, (short)pixel);
}

@Override
public void setPixel(int x, int y, int band, float pixel)
{
  setPixel(x, y, band, (short)pixel);

}

@Override
public void setPixel(int x, int y, int band, double pixel)
{
  setPixel(x, y, band, (short)pixel);
}
}
