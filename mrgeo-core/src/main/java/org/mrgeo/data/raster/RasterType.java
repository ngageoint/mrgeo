/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.data.raster;

import java.awt.image.DataBuffer;

public class RasterType
{
  final String name;
  final int bits;
  final boolean isFloat;
  final int bytes;

  public static class RasterTypeException extends RuntimeException
  {

    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public RasterTypeException(final Exception e)
    {
      this.origException = e;
    }

    public RasterTypeException(final String msg)
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

  private static class BitType extends RasterType
  {
    BitType()
    {
      super("bit", 1, false);
    }

    @Override
    public int numBytes(final int size)
    {
      // round up to the next bigger byte
      return (size + 7) / 8;
    }

  }

  private static class ByteType extends RasterType
  {
    ByteType()
    {
      super("int8", 8, false);
    }
  }


  private static class ShortType extends RasterType
  {
    ShortType()
    {
      super("int16", 16, false);
    }
  }

  private static class IntType extends RasterType
  {
    IntType()
    {
      super("int32", 32, false);
    }
  }

  private static class FloatType extends RasterType
  {
    FloatType()
    {
      super("float32", 32, true);
    }
  }

  private static class DoubleType extends RasterType
  {
    DoubleType()
    {
      super("float64", 64, true);
    }
  }

  public enum DataType
  {
    BIT, BYTE, SHORT, INT, FLOAT, DOUBLE
  }

  RasterType(String name, int bits, boolean isFloat)
  {
    this.name = name;
    this.bits = bits;
    this.isFloat = isFloat;

    bytes = bits / 8;
  }

  public int numBytes(final int size)
  {
    return bytes * size;
  }

  public int numBytes(final int width, final int height)
  {
    return numBytes(width * height);
  }

  public static RasterType fromAwtType(final int awtType)
  {
    switch (awtType)
    {
    case DataBuffer.TYPE_BYTE:
    {
      return new ByteType();
    }
    case DataBuffer.TYPE_SHORT:
    {
      return new ShortType();
    }
    case DataBuffer.TYPE_INT:
    {
      return new IntType();
    }
    case DataBuffer.TYPE_FLOAT:
    {
      return new FloatType();
    }
    case DataBuffer.TYPE_DOUBLE:
    {
      return new DoubleType();
    }
    default:
      throw new RasterTypeException("Error converting from AWT data type.  Unsupported data type: " + awtType);
    }
  }

  public static int toAwtType(RasterType rasterType)
  {
    if (rasterType instanceof BitType)
    {
      return DataBuffer.TYPE_BYTE;
    }
    if (rasterType instanceof ShortType)
    {
      return DataBuffer.TYPE_SHORT;
    }
    if (rasterType instanceof IntType)
    {
      return DataBuffer.TYPE_INT;
    }
    if (rasterType instanceof FloatType)
    {
      return DataBuffer.TYPE_FLOAT;
    }
    if (rasterType instanceof DoubleType)
    {
      return DataBuffer.TYPE_DOUBLE;
    }

    throw new RasterTypeException("Error converting to AWT data type.  Unsupported raster data type");
  }

  public static RasterType fromString(String name)
  {
    RasterType type = new BitType();
    if (name == type.name)
    {
      return type;
    }
    type = new ByteType();
    if (name == type.name)
    {
      return type;
    }
    type = new ShortType();
    if (name == type.name)
    {
      return type;
    }
    type = new IntType();
    if (name == type.name)
    {
      return type;
    }
    type = new FloatType();
    if (name == type.name)
    {
      return type;
    }
    type = new DoubleType();
    if (name == type.name)
    {
      return type;
    }

    throw new RasterTypeException("Error converting from String.  Unsupported raster name: " + name);

  }


  public static String toString(RasterType rasterType)
  {
    return rasterType.name;
  }
}
