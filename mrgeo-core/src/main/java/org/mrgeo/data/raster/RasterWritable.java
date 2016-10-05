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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.mrgeo.utils.ByteArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

@SuppressFBWarnings(value = "MS_PKGPROTECT", justification = "Serializer/Deserializers only!  Need to be removed after testing")
public class RasterWritable implements WritableComparable<RasterWritable>, Serializable, Cloneable
{
private static final Logger log = LoggerFactory.getLogger(RasterWritable.class);

private static final long serialVersionUID = 1L;

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

//  public RasterWritableException(final Exception e)
//  {
//    this.origException = e;
//  }

  RasterWritableException(final String msg)
  {
    this.origException = new Exception(msg);
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
public boolean equals(Object other)
{
  return other instanceof RasterWritable && Arrays.equals(bytes, ((RasterWritable) other).bytes);
}

@Override
public int hashCode()
{
  return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
      // if deriving: appendSuper(super.hashCode()).
          appendSuper(super.hashCode()).toHashCode();
}

@Override
public void write(DataOutput out) throws IOException
{
  if (bytes == null)
  {
    out.writeInt(0);
  }
  else
  {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

}

@Override
public void readFields(DataInput in) throws IOException
{
  int len = in.readInt();
  if (len > 0)
  {
    bytes = new byte[len];
    in.readFully(bytes);
  }
}


private enum SampleModelType {
  PIXELINTERLEAVED, BANDED, SINGLEPIXELPACKED, MULTIPIXELPACKED, COMPONENT
}

public RasterWritable()
{
  this.bytes = null;
}

protected RasterWritable(final byte[] bytes)
{
  this.bytes = bytes;
}

@SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "No super.clone() to call")
@Override
public Object clone()
{
  return new RasterWritable(copyBytes());
}

public RasterWritable copy()
{
  return new RasterWritable(copyBytes());
}

// we could use the default serializations here, but instead we'll just do it manually
private void writeObject(ObjectOutputStream stream) throws IOException
{
  if (bytes == null)
  {
    stream.writeInt(0);
  }
  else
  {
    stream.writeInt(bytes.length);
    stream.write(bytes, 0, bytes.length);
  }
}

private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException
{
  int len = stream.readInt();

  if (len > 0)
  {
    bytes = new byte[len];
    stream.readFully(bytes, 0, len);
  }
}

public int getSize()
{
  return bytes.length;
}

public byte[] copyBytes()
{
  byte[] copy = new byte[bytes.length];
  System.arraycopy(bytes, 0, copy, 0, bytes.length);

  return copy;
}

// should this do a copy of the bytes?
public static RasterWritable fromBytes(byte[] bytes)
{
  return new RasterWritable(bytes);
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

public static MrGeoRaster toMrGeoRaster(final RasterWritable writable,
    final CompressionCodec codec, final Decompressor decompressor) throws IOException
{
  decompressor.reset();
  final ByteArrayInputStream bis = new ByteArrayInputStream(writable.bytes, 0, writable.getSize());
  final CompressionInputStream gis = codec.createInputStream(bis, decompressor);
  final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  IOUtils.copyBytes(gis, baos, 1024 * 1024 * 2, true);

  return toMrGeoRaster(new RasterWritable(baos.toByteArray()));
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
  int srclen = data.length - headersize;

  switch (sampleModelType)
  {
  case BANDED:
    // this one is easy, just make a new MrGeoRaster and copy the data
    System.arraycopy(data, headersize, raster.data, raster.dataoffset(), srclen);
    break;
  case MULTIPIXELPACKED:
    throw new NotImplementedException("MultiPixelPackedSampleModel not implemented yet");
  case COMPONENT:
  case PIXELINTERLEAVED:
  {
    if (bands == 1)
    {
      if (srclen < raster.datasize())
      {
        log.warn(String.format("Input raster data size (%dB) is " +
            "less then than the calculated data size (%dB), " +
            "only copying (%dB)", srclen, raster.datasize(), srclen));
        System.arraycopy(data, headersize, raster.data, raster.dataoffset(), srclen);

      }
      else if (srclen > raster.datasize())
      {
        log.warn(String.format("Input raster data size (%dB) is " +
            "less then than the calculated data size (%dB), " +
            "only copying (%dB)", srclen, raster.datasize(), raster.datasize()));
        System.arraycopy(data, headersize, raster.data, raster.dataoffset(), raster.datasize());
      }
      else
      {
        System.arraycopy(data, headersize, raster.data, raster.dataoffset(), srclen);
      }
    }
    else
    {
      int offset = headersize;
      int bpp = raster.bytesPerPixel();
      double pixel;
      for (int y = 0; y < height; y++)
      {
        for (int x = 0; x < width; x++)
        {
          for (int b = 0; b < bands; b++)
          {
            switch (datatype)
            {
            case DataBuffer.TYPE_BYTE:
              pixel = ByteArrayUtils.getByte(data, offset);
              break;
            case DataBuffer.TYPE_SHORT:
            case DataBuffer.TYPE_USHORT:
              pixel = ByteArrayUtils.getShort(data, offset);
              break;
            case DataBuffer.TYPE_INT:
              pixel = ByteArrayUtils.getInt(data, offset);
              break;
            case DataBuffer.TYPE_FLOAT:
              pixel = ByteArrayUtils.getFloat(data, offset);
              break;
            case DataBuffer.TYPE_DOUBLE:
              pixel = ByteArrayUtils.getDouble(data, offset);
              break;
            default:
              throw new RasterWritableException("Bad data type");
            }

            raster.setPixel(x, y, b, pixel);

            offset += bpp;
          }
        }
      }
    }
    break;
  }
  case SINGLEPIXELPACKED:
    throw new NotImplementedException("SinglePixelPackedSampleModel not implemented yet");
  default:
    throw new RasterWritableException("Unknown RasterSampleModel type");
  }

  // The old data was big-endian, ours is little-endian.  The RasterWritable may persisted, so
  // we can't just swap the source.  Instead, we need to swap _after_ the copy.  We'll just swap
  // the data in the MrGeoRaster inplace.
  ByteArrayUtils.swapBytes(raster.data, datatype, raster.dataoffset());

  return raster;
}

}
