package org.mrgeo.data.raster;

import org.mrgeo.utils.ByteArrayUtils;

import java.awt.image.DataBuffer;

final class MrGeoDoubleRaster extends MrGeoRaster
{
private static final int BYTES_PER_PIXEL = 8;

MrGeoDoubleRaster(int width, int height, int bands, byte[] data, int dataOffset)
{
  super(width, height, bands, DataBuffer.TYPE_DOUBLE, data, dataOffset);
}

static MrGeoRaster createEmptyRaster(int width, int height, int bands)
{
  byte[] data = new byte[(width * height * bands * BYTES_PER_PIXEL) + MrGeoRaster.HEADER_LEN];

  MrGeoRaster.writeHeader(width, height, bands, DataBuffer.TYPE_DOUBLE, data);

  return new MrGeoDoubleRaster(width, height, bands, data, MrGeoRaster.HEADER_LEN);
}

@Override
public byte getPixelByte(int x, int y, int band)
{
  return (byte)ByteArrayUtils.getDouble(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixelShort(int x, int y, int band)
{
  return (short)ByteArrayUtils.getDouble(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixeUShort(int x, int y, int band)
{
  return (short)ByteArrayUtils.getDouble(data, calculateByteOffset(x, y, band));
}

@Override
public int getPixelInt(int x, int y, int band)
{
  return (int)ByteArrayUtils.getDouble(data, calculateByteOffset(x, y, band));
}

@Override
public float getPixelFloat(int x, int y, int band)
{
  return (float)ByteArrayUtils.getDouble(data, calculateByteOffset(x, y, band));
}

@Override
public double getPixelDouble(int x, int y, int band)
{
  return ByteArrayUtils.getDouble(data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, byte pixel)
{
  ByteArrayUtils.setDouble((double)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, short pixel)
{
  ByteArrayUtils.setDouble((double)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixe(int x, int y, int band, int pixel)
{
  ByteArrayUtils.setDouble((double)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, float pixel)
{
  ByteArrayUtils.setDouble((double)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, double pixel)
{
  ByteArrayUtils.setDouble(pixel, data, calculateByteOffset(x, y, band));

}

@Override
int bytesPerPixel()
{
  return BYTES_PER_PIXEL;
}
}
