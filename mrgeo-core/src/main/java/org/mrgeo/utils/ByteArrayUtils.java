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

package org.mrgeo.utils;

import java.awt.image.DataBuffer;

// Bytes here are assumed to be little-endian...
public class ByteArrayUtils
{

public static byte getByte(byte[] bytes, int offset)
{
  return bytes[offset];
}

public static byte getByte(byte[] bytes)
{
  return getByte(bytes, 0);
}

public static void setByte(byte value, byte[] bytes, int offset)
{
  bytes[offset] = value;
}

public static void setByte(byte value, byte[] bytes)
{
  bytes[0] = value;
}

public static short getShort(byte[] bytes, int offset)
{
  return (short)
      (((bytes[offset + 1] & 0xff) << 8) |
          (bytes[offset] & 0xff));
}

public static short getShort(byte[] bytes)
{
  return (short)
      (((bytes[1] & 0xff) << 8) |
          (bytes[0] & 0xff));
}

public static void setShort(short value, byte[] bytes, int offset)
{
  bytes[offset + 1] = (byte) (value >> 8);
  bytes[offset] = (byte) (value & 0x00ff);
}

public static void setShort(short value, byte[] bytes)
{
  bytes[1] = (byte) (value >> 8);
  bytes[0] = (byte) (value & 0x00ff);
}

public static int getInt(byte[] bytes, int offset)
{
  return
      (((bytes[offset + 3] & 0xff) << 24) |
          ((bytes[offset + 2] & 0xff) << 16) |
          ((bytes[offset + 1] & 0xff) << 8) |
          (bytes[offset] & 0xff));
}

public static int getInt(byte[] bytes)
{
  return
      (((bytes[3] & 0xff) << 24) |
          ((bytes[2] & 0xff) << 16) |
          ((bytes[1] & 0xff) << 8) |
          (bytes[0] & 0xff));
}

public static void setInt(int value, byte[] bytes, int offset)
{
  bytes[offset + 3] = (byte) (value >> 24);
  bytes[offset + 2] = (byte) ((value & 0x00ff0000) >> 16);
  bytes[offset + 1] = (byte) ((value & 0x0000ff00) >> 8);
  bytes[offset] = (byte) (value & 0x000000ff);
}

public static void setInt(int value, byte[] bytes)
{
  bytes[3] = (byte) (value >> 24);
  bytes[2] = (byte) ((value & 0x00ff0000) >> 16);
  bytes[1] = (byte) ((value & 0x0000ff00) >> 8);
  bytes[0] = (byte) (value & 0x000000ff);
}

public static long getLong(byte[] bytes, int offset)
{
  return
      (((long) (bytes[offset + 7] & 0xff) << 56) |
          ((long) (bytes[offset + 6] & 0xff) << 48) |
          ((long) (bytes[offset + 5] & 0xff) << 40) |
          ((long) (bytes[offset + 4] & 0xff) << 32) |
          ((long) (bytes[offset + 3] & 0xff) << 24) |
          ((long) (bytes[offset + 2] & 0xff) << 16) |
          ((long) (bytes[offset + 1] & 0xff) << 8) |
          ((long) (bytes[offset] & 0xff)));
}

public static long getLong(byte[] bytes)
{
  return
      (((long) (bytes[7] & 0xff) << 56) |
          ((long) (bytes[6] & 0xff) << 48) |
          ((long) (bytes[5] & 0xff) << 40) |
          ((long) (bytes[4] & 0xff) << 32) |
          ((long) (bytes[3] & 0xff) << 24) |
          ((long) (bytes[2] & 0xff) << 16) |
          ((long) (bytes[1] & 0xff) << 8) |
          ((long) (bytes[0] & 0xff)));
}

public static void setLong(long value, byte[] bytes, int offset)
{
  bytes[offset + 7] = (byte) (value >> 56);
  bytes[offset + 6] = (byte) ((value & 0x00ff000000000000L) >> 48);
  bytes[offset + 5] = (byte) ((value & 0x0000ff0000000000L) >> 40);
  bytes[offset + 4] = (byte) ((value & 0x000000ff00000000L) >> 32);
  bytes[offset + 3] = (byte) ((value & 0x00000000ff000000L) >> 24);
  bytes[offset + 2] = (byte) ((value & 0x0000000000ff0000L) >> 16);
  bytes[offset + 1] = (byte) ((value & 0x000000000000ff00L) >> 8);
  bytes[offset] = (byte) (value & 0x00000000000000ffL);
}

public static void setLong(long value, byte[] bytes)
{
  bytes[7] = (byte) (value >> 56);
  bytes[6] = (byte) ((value & 0x00ff000000000000L) >> 48);
  bytes[5] = (byte) ((value & 0x0000ff0000000000L) >> 40);
  bytes[4] = (byte) ((value & 0x000000ff00000000L) >> 32);
  bytes[3] = (byte) ((value & 0x00000000ff000000L) >> 24);
  bytes[2] = (byte) ((value & 0x0000000000ff0000L) >> 16);
  bytes[1] = (byte) ((value & 0x000000000000ff00L) >> 8);
  bytes[0] = (byte) (value & 0x00000000000000ffL);
}

public static float getFloat(byte[] bytes, int offset)
{
  return Float.intBitsToFloat(
      ((bytes[offset + 3] & 0xff) << 24 |
          (bytes[offset + 2] & 0xff) << 16 |
          (bytes[offset + 1] & 0xff) << 8 |
          (bytes[offset] & 0xff)));
}

public static float getFloat(byte[] bytes)
{
  return Float.intBitsToFloat(
      ((bytes[3] & 0xff) << 24 |
          (bytes[2] & 0xff) << 16 |
          (bytes[1] & 0xff) << 8 |
          (bytes[0] & 0xff)));
}

public static void setFloat(float value, byte[] bytes, int offset)
{
  int intval = Float.floatToRawIntBits(value);

  bytes[offset + 3] = (byte) (intval >> 24);
  bytes[offset + 2] = (byte) ((intval & 0x00ff0000) >> 16);
  bytes[offset + 1] = (byte) ((intval & 0x0000ff00) >> 8);
  bytes[offset] = (byte) (intval & 0x000000ff);
}

public static void setFloat(float value, byte[] bytes)
{
  int intval = Float.floatToRawIntBits(value);

  bytes[3] = (byte) (intval >> 24);
  bytes[2] = (byte) ((intval & 0x00ff0000) >> 16);
  bytes[1] = (byte) ((intval & 0x0000ff00) >> 8);
  bytes[0] = (byte) (intval & 0x000000ff);
}

public static double getDouble(byte[] bytes, int offset)
{
  return Double.longBitsToDouble(
      (((long) (bytes[offset + 7] & 0xff) << 56) |
          ((long) (bytes[offset + 6] & 0xff) << 48) |
          ((long) (bytes[offset + 5] & 0xff) << 40) |
          ((long) (bytes[offset + 4] & 0xff) << 32) |
          ((long) (bytes[offset + 3] & 0xff) << 24) |
          ((long) (bytes[offset + 2] & 0xff) << 16) |
          ((long) (bytes[offset + 1] & 0xff) << 8) |
          ((long) (bytes[offset] & 0xff))));
}

public static double getDouble(byte[] bytes)
{
  return Double.longBitsToDouble(
      (((long) (bytes[7] & 0xff) << 56) |
          ((long) (bytes[6] & 0xff) << 48) |
          ((long) (bytes[5] & 0xff) << 40) |
          ((long) (bytes[4] & 0xff) << 32) |
          ((long) (bytes[3] & 0xff) << 24) |
          ((long) (bytes[2] & 0xff) << 16) |
          ((long) (bytes[1] & 0xff) << 8) |
          ((long) (bytes[0] & 0xff))));
}

public static void setDouble(double value, byte[] bytes, int offset)
{
  long longval = Double.doubleToRawLongBits(value);

  bytes[offset + 7] = (byte) (longval >> 56);
  bytes[offset + 6] = (byte) ((longval & 0x00ff000000000000L) >> 48);
  bytes[offset + 5] = (byte) ((longval & 0x0000ff0000000000L) >> 40);
  bytes[offset + 4] = (byte) ((longval & 0x000000ff00000000L) >> 32);
  bytes[offset + 3] = (byte) ((longval & 0x00000000ff000000L) >> 24);
  bytes[offset + 2] = (byte) ((longval & 0x0000000000ff0000L) >> 16);
  bytes[offset + 1] = (byte) ((longval & 0x000000000000ff00L) >> 8);
  bytes[offset] = (byte) (longval & 0x00000000000000ffL);
}

public static void setDouble(double value, byte[] bytes)
{
  long longval = Double.doubleToRawLongBits(value);

  bytes[7] = (byte) (longval >> 56);
  bytes[6] = (byte) ((longval & 0x00ff000000000000L) >> 48);
  bytes[5] = (byte) ((longval & 0x0000ff0000000000L) >> 40);
  bytes[4] = (byte) ((longval & 0x000000ff00000000L) >> 32);
  bytes[3] = (byte) ((longval & 0x00000000ff000000L) >> 24);
  bytes[2] = (byte) ((longval & 0x0000000000ff0000L) >> 16);
  bytes[1] = (byte) ((longval & 0x000000000000ff00L) >> 8);
  bytes[0] = (byte) (longval & 0x00000000000000ffL);
}

public static void swapBytes(byte[] bytes, int datatype, int offset)
{
  byte tmp;
  int i = offset;

  switch (datatype)
  {
  case DataBuffer.TYPE_BYTE:
    break;  // nothing to do
  case DataBuffer.TYPE_SHORT:
  case DataBuffer.TYPE_USHORT:
    // 2 byte value... 1, 2 becomes 2, 1  (swap byte 1 with 2)
    while (i + 1 < bytes.length)
    {
      // swap 0 & 1
      tmp = bytes[i];
      bytes[i] = bytes[i + 1];
      bytes[i + 1] = tmp;
      i += 2;
    }
    break;
  // 4 byte value... 1, 2, 3, 4 becomes 4, 3, 2, 1  (swap bytes 1 & 4, 2 & 3)
  case DataBuffer.TYPE_FLOAT:
  case DataBuffer.TYPE_INT:
    while (i + 3 < bytes.length)
    {
      // swap 0 & 3
      tmp = bytes[i];
      bytes[i] = bytes[i + 3];
      bytes[i + 3] = tmp;

      // swap 1 & 2
      tmp = bytes[i + 1];
      bytes[i + 1] = bytes[i + 2];
      bytes[i + 2] = tmp;
      i += 4;
    }
    break;
  case DataBuffer.TYPE_DOUBLE:
    // 8 byte value... 1, 2, 3, 4, 5, 6, 7 becomes 7, 6, 5, 4, 3, 2, 1
    // (swap bytes 1 & 8, 2 & 7, 3 & 6, 4 & 5)
    while (i + 7 < bytes.length)
    {
      // swap 0 & 7
      tmp = bytes[i];
      bytes[i] = bytes[i + 7];
      bytes[i + 7] = tmp;

      // swap 1 & 6
      tmp = bytes[i + 1];
      bytes[i + 1] = bytes[i + 6];
      bytes[i + 6] = tmp;

      // swap 2 & 5
      tmp = bytes[i + 2];
      bytes[i + 2] = bytes[i + 5];
      bytes[i + 5] = tmp;

      // swap 3 $ 4
      tmp = bytes[i + 3];
      bytes[i + 3] = bytes[i + 4];
      bytes[i + 4] = tmp;

      i += 8;
    }

    break;
  }
}


}
