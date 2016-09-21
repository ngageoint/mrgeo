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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.*;
import org.mrgeo.utils.ByteArrayUtils;

import java.awt.image.*;
import java.io.*;
import java.nio.*;
import java.util.Arrays;

public class RasterWritable implements WritableComparable<RasterWritable>
{
private static final long serialVersionUID = 1L;

private static int HEADERSIZE = 5;

public static long serializeTime = 0;
public static long serializeCnt = 0;
public static long deserializeTime = 0;
public static long deserializeCnt = 0;

private static final Object serializeSync = new Object();
private static final Object deserializeSync = new Object();

private byte[] bytes;

public static class RasterWritableException extends RuntimeException
{

  private static final long serialVersionUID = 1L;
  private final Exception origException;

  public RasterWritableException(final Exception e)
  {
    this.origException = e;
  }

  public RasterWritableException(final String msg)
  {
    final Exception e = new Exception(msg);
    this.origException = e;
  }

  @Override
  public void printStackTrace()
  {
    origException.printStackTrace();
  }
}

public int compareTo(RasterWritable other)
{
  return Arrays.equals(bytes, other.bytes) ? 0 : 1;
}

@Override
public void write(DataOutput out) throws IOException
{
  out.write(bytes.length);
  out.write(bytes);
}

@Override
public void readFields(DataInput in) throws IOException
{
  int len = in.readInt();
  bytes = new byte[len];
  in.readFully(bytes);
}


private enum SampleModelType {
  PIXELINTERLEAVED, BANDED, SINGLEPIXELPACKED, MULTIPIXELPACKED, COMPONENT
}

public RasterWritable()
{
  super();
}

public RasterWritable(final byte[] bytes)
{
  this.bytes = bytes;
}

public RasterWritable(RasterWritable copy)
{
  this.bytes = copy.bytes;
}

// we could use the default serializations here, but instead we'll just do it manually
private void writeObject(ObjectOutputStream stream) throws IOException
{
  stream.writeInt(bytes.length);
  stream.write(bytes, 0, bytes.length);
}

private void readObject(ObjectInputStream stream) throws IOException
{
  int size = stream.readInt();
  bytes = new byte[size];

  stream.readFully(bytes, 0, size);
}

public int getSize()
{
  return bytes.length;
}

public byte[] getBytes()
{
  return bytes;
}

public byte[] copyBytes()
{
  byte[] copy = new byte[bytes.length];
  System.arraycopy(bytes, 0, copy, 0, bytes.length);

  return copy;
}

public static MrGeoRaster toMrGeoRaster(final RasterWritable writable) throws IOException
{
  long starttime = System.currentTimeMillis();
  try
  {
    int version = ByteArrayUtils.getByte(writable.bytes);
    if (version == 0)
    {
      // this is an old MrsPyramid v2 image, read it into a MrGeoRaster
      return RasterWritable.convertFromV2(writable.bytes);
    }
    return MrGeoRaster.createRaster(writable.bytes);
  }
  finally
  {
    synchronized (deserializeSync)
    {
      deserializeCnt++;
      deserializeTime += (System.currentTimeMillis() - starttime);
    }
  }

}

public static RasterWritable toWritable(MrGeoRaster raster) throws IOException
{
  long starttime = System.currentTimeMillis();
  try
  {
    return new RasterWritable(raster.data());
  }
  finally
  {
    synchronized (serializeSync)
    {
      serializeCnt++;
      serializeTime += (System.currentTimeMillis() - starttime);
    }
  }
}

private static MrGeoRaster convertFromV2(byte[] data)
{
  final ByteBuffer rasterBuffer = ByteBuffer.wrap(data);

  final int headersize = (rasterBuffer.getInt() + 1) * 4; // include the header! ( * sizeof(int) )
  final int height = rasterBuffer.getInt();
  final int width = rasterBuffer.getInt();
  final int bands = rasterBuffer.getInt();
  final int datatype = rasterBuffer.getInt();
  final SampleModelType sampleModelType = SampleModelType.values()[rasterBuffer.getInt()];

  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, bands, datatype);
  switch (sampleModelType)
  {
  case BANDED:
    // this one is easy, just make a new MrGeoRaster and copy the data
    int srclen = data.length - headersize;

    System.arraycopy(data, headersize, raster.data, raster.dataoffset(), srclen);
    break;
  case MULTIPIXELPACKED:
    throw new NotImplementedException("MultiPixelPackedSampleModel not implemented yet");
  case COMPONENT:
  case PIXELINTERLEAVED:
  {
    throw new NotImplementedException("PixelInterleaved and Component not implemented yet");
  }
  case SINGLEPIXELPACKED:
    throw new NotImplementedException("SinglePixelPackedSampleModel not implemented yet");
  default:
    throw new RasterWritableException("Unknown RasterSampleModel type");
  }

  return raster;
}

}
