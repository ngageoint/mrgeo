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

import org.mrgeo.utils.ByteArrayUtils;

import java.awt.image.DataBuffer;

final class MrGeoShortRaster extends MrGeoRaster
{
private static final long serialVersionUID = 1L;

private static final int BYTES_PER_PIXEL = 2;

MrGeoShortRaster(int width, int height, int bands, byte[] data, int dataOffset)
{
  super(width, height, bands, DataBuffer.TYPE_SHORT, data, dataOffset);
}

public static MrGeoRaster createEmptyRaster(int width, int height, int bands) throws MrGeoRasterException
{
  long bytes = ((long) width * (long) height * (long) bands * (long) BYTES_PER_PIXEL) + MrGeoRaster.HEADER_LEN;
  if (bytes > (long) Integer.MAX_VALUE)
  {
    throw new MrGeoRasterException(String.format("Error creating short raster.  Raster too large: width: %d " +
            "height: %d  bands: %d  (%d bytes per pixel, %d byte header length) (%d total bytes)",
        width, height, bands, BYTES_PER_PIXEL, MrGeoRaster.HEADER_LEN, bytes));
  }
  byte[] data = new byte[(int) bytes];

  MrGeoRaster.writeHeader(width, height, bands, DataBuffer.TYPE_SHORT, data);

  return new MrGeoShortRaster(width, height, bands, data, MrGeoRaster.HEADER_LEN);
}

@Override
public byte getPixelByte(int x, int y, int band)
{
  return (byte) ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixelShort(int x, int y, int band)
{
  return ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

@Override
public short getPixeUShort(int x, int y, int band)
{
  return ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

@Override
public int getPixelInt(int x, int y, int band)
{
  return (int) ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

@Override
public float getPixelFloat(int x, int y, int band)
{
  return (float) ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

//
@Override
public double getPixelDouble(int x, int y, int band)
{
  return (double) ByteArrayUtils.getShort(data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, byte pixel)
{
  ByteArrayUtils.setShort((short) pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, short pixel)
{
  ByteArrayUtils.setShort(pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, int pixel)
{
  ByteArrayUtils.setShort((short) pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, float pixel)
{
  ByteArrayUtils.setShort((short) pixel, data, calculateByteOffset(x, y, band));
}

@Override
public void setPixel(int x, int y, int band, double pixel)
{
  ByteArrayUtils.setShort((short) pixel, data, calculateByteOffset(x, y, band));
}

@Override
public int bytesPerPixel()
{
  return BYTES_PER_PIXEL;
}
}
