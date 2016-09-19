package org.mrgeo.data.raster;

import org.mrgeo.utils.ByteArrayUtils;
import java.awt.image.DataBuffer;

final class MrGeoFloatRaster extends MrGeoRaster
{
private static final int BYTES_PER_PIXEL = 4;

MrGeoFloatRaster(int width, int height, int bands, byte[] data, int dataOffset)
{
  super(width, height, bands, DataBuffer.TYPE_FLOAT, data, dataOffset);
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands)
{
  byte[] data = new byte[(width * height * bands * BYTES_PER_PIXEL) + RasterWritable.HeaderData.getHeaderLength()];

  ByteArrayUtils.setInt(width, data, 0);
  ByteArrayUtils.setInt(height, data, 4);
  ByteArrayUtils.setInt(bands, data, 8);
  ByteArrayUtils.setInt(DataBuffer.TYPE_FLOAT, data, 12);

  return new MrGeoFloatRaster(width, height, bands, data, RasterWritable.HeaderData.getHeaderLength());
}

@Override
public byte getPixelByte(int x, int y, int band)
{
  return (byte)ByteArrayUtils.getFloat(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixelShort(int x, int y, int band)
{
  return (short)ByteArrayUtils.getFloat(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixeUShort(int x, int y, int band)
{
  return (short)ByteArrayUtils.getFloat(data, calculateByteOffset(x, y, band));
}

@Override
public int getPixelInt(int x, int y, int band)
{
  return (int)ByteArrayUtils.getFloat(data, calculateByteOffset(x, y, band));
}

@Override
public float getPixelFloat(int x, int y, int band)
{
  return ByteArrayUtils.getFloat(data, calculateByteOffset(x, y, band));
}

@Override
public double getPixelDouble(int x, int y, int band)
{
  return (double)ByteArrayUtils.getFloat(data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, byte pixel)
{
  ByteArrayUtils.setFloat((float)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, short pixel)
{
  ByteArrayUtils.setFloat((float)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixe(int x, int y, int band, int pixel)
{
  ByteArrayUtils.setFloat((float)pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, float pixel)
{
  ByteArrayUtils.setFloat(pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, double pixel)
{
  ByteArrayUtils.setFloat((float)pixel, data, calculateByteOffset(x, y, band));

}

@Override
int bytesPerPixel()
{
  return BYTES_PER_PIXEL;
}
}
