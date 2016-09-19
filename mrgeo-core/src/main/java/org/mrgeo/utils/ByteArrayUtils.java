package org.mrgeo.utils;

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
      (   (bytes[offset + 1] <<  8) |
          (bytes[offset    ]));
}

public static short getShort(byte[] bytes)
{
  return (short)
      (   (bytes[1] <<  8) |
          (bytes[0]));
}

public static void setShort(short value, byte[] bytes, int offset)
{
  bytes[offset + 1] = (byte)((value & 0xff00) >>  8);
  bytes[offset    ] = (byte)(value & 0xff);
}

public static void setShort(short value, byte[] bytes)
{
  bytes[1] = (byte)((value & 0xff00) >>  8);
  bytes[0] = (byte)(value & 0xff);
}

public static int getInt(byte[] bytes, int offset)
{
  return
      (   (bytes[offset + 3]) << 24) |
          (bytes[offset + 2] << 16) |
          (bytes[offset + 1] <<  8) |
          (bytes[offset    ]);
}

public static int getInt(byte[] bytes)
{
  return
      (   (bytes[3]) << 24) |
          (bytes[2] << 16) |
          (bytes[1] <<  8) |
          (bytes[0]);
}

public static void setInt(int value, byte[] bytes, int offset)
{
  bytes[offset + 3] = (byte)(value >> 24);
  bytes[offset + 2] = (byte)((value & 0x00ff0000) >> 16);
  bytes[offset + 1] = (byte)((value & 0x0000ff00) >>  8);
  bytes[offset    ] = (byte)(value & 0xff);
}

public static void setInt(int value, byte[] bytes)
{
  bytes[3] = (byte)(value >> 24);
  bytes[2] = (byte)((value & 0x00ff0000) >> 16);
  bytes[1] = (byte)((value & 0x0000ff00) >>  8);
  bytes[0] = (byte)(value & 0xff);
}

public static long getLong(byte[] bytes, int offset)
{
  return
      (   ((long)bytes[offset + 7] << 56) |
          ((long)bytes[offset + 6] << 48) |
          ((long)bytes[offset + 5] << 40) |
          ((long)bytes[offset + 4] << 32) |
          ((long)bytes[offset + 3] << 24) |
          ((long)bytes[offset + 2] << 16) |
          ((long)bytes[offset + 1] <<  8) |
          ((long)bytes[offset    ]));
}

public static long getLong(byte[] bytes)
{
  return
      (   ((long)bytes[7] << 56) |
          ((long)bytes[6] << 48) |
          ((long)bytes[5] << 40) |
          ((long)bytes[4] << 32) |
          ((long)bytes[3] << 24) |
          ((long)bytes[2] << 16) |
          ((long)bytes[1] <<  8) |
          ((long)bytes[0]));
}

public static void setLong(long value, byte[] bytes, int offset)
{
  bytes[offset + 7] = (byte)(value >> 56);
  bytes[offset + 6] = (byte)((value & 0x00ff000000000000L) >> 48);
  bytes[offset + 5] = (byte)((value & 0x0000ff0000000000L) >> 40);
  bytes[offset + 4] = (byte)((value & 0x000000ff00000000L) >> 32);
  bytes[offset + 3] = (byte)((value & 0x00000000ff000000L) >> 24);
  bytes[offset + 2] = (byte)((value & 0x0000000000ff0000L) >> 16);
  bytes[offset + 1] = (byte)((value & 0x000000000000ff00L) >>  8);
  bytes[offset    ] = (byte)( value & 0x00000000000000ffL);
}

public static void setLong(long value, byte[] bytes)
{
  bytes[7] = (byte)(value >> 56);
  bytes[6] = (byte)((value & 0x00ff000000000000L) >> 48);
  bytes[5] = (byte)((value & 0x0000ff0000000000L) >> 40);
  bytes[4] = (byte)((value & 0x000000ff00000000L) >> 32);
  bytes[3] = (byte)((value & 0x00000000ff000000L) >> 24);
  bytes[2] = (byte)((value & 0x0000000000ff0000L) >> 16);
  bytes[1] = (byte)((value & 0x000000000000ff00L) >>  8);
  bytes[0] = (byte)( value & 0x00000000000000ffL);
}

public static float getFloat(byte[] bytes, int offset)
{
  return Float.intBitsToFloat(
      (   bytes[offset + 3] << 24 |
          bytes[offset + 2] << 16 |
          bytes[offset + 1] << 8 |
          bytes[offset    ]));
}

public static float getFloat(byte[] bytes)
{
  return Float.intBitsToFloat(
      (   bytes[3] << 24 |
          bytes[2] << 16 |
          bytes[1] << 8 |
          bytes[0]));
}

public static void setFloat(float value, byte[] bytes, int offset)
{
  int intval = Float.floatToRawIntBits(value);

  bytes[offset + 3] = (byte)(intval >> 24);
  bytes[offset + 2] = (byte)((intval & 0x00ff0000) >> 16);
  bytes[offset + 1] = (byte)((intval & 0x0000ff00) >>  8);
  bytes[offset    ] = (byte)(intval & 0xff);
}

public static void setFloat(float value, byte[] bytes)
{
  int intval = Float.floatToRawIntBits(value);

  bytes[3] = (byte)(intval >> 24);
  bytes[2] = (byte)((intval & 0x00ff0000) >> 16);
  bytes[1] = (byte)((intval & 0x0000ff00) >>  8);
  bytes[0] = (byte)(intval & 0xff);
}

public static double getDouble(byte[] bytes, int offset)
{
  return Double.longBitsToDouble(
      (   ((long)bytes[offset + 7] << 56) |
          ((long)bytes[offset + 6] << 48) |
          ((long)bytes[offset + 5] << 40) |
          ((long)bytes[offset + 4] << 32) |
          ((long)bytes[offset + 3] << 24) |
          ((long)bytes[offset + 2] << 16) |
          ((long)bytes[offset + 1] << 8) |
          ((long)bytes[offset    ])));
}

public static double getDouble(byte[] bytes)
{
  return Double.longBitsToDouble(
      (   ((long)bytes[7] << 56) |
          ((long)bytes[6] << 48) |
          ((long)bytes[5] << 40) |
          ((long)bytes[4] << 32) |
          ((long)bytes[3] << 24) |
          ((long)bytes[2] << 16) |
          ((long)bytes[1] << 8) |
          ((long)bytes[0])));
}

public static void setDouble(double value, byte[] bytes, int offset)
{
  long longval = Double.doubleToRawLongBits(value);

  bytes[offset + 7] = (byte)(longval >> 56);
  bytes[offset + 6] = (byte)((longval & 0x00ff000000000000L) >> 48);
  bytes[offset + 5] = (byte)((longval & 0x0000ff0000000000L) >> 40);
  bytes[offset + 4] = (byte)((longval & 0x000000ff00000000L) >> 32);
  bytes[offset + 3] = (byte)((longval & 0x00000000ff000000L) >> 24);
  bytes[offset + 2] = (byte)((longval & 0x0000000000ff0000L) >> 16);
  bytes[offset + 1] = (byte)((longval & 0x000000000000ff00L) >>  8);
  bytes[offset    ] = (byte)( longval & 0x00000000000000ffL);
}

public static void setDouble(double value, byte[] bytes)
{
  long longval = Double.doubleToRawLongBits(value);

  bytes[7] = (byte)(longval >> 56);
  bytes[6] = (byte)((longval & 0x00ff000000000000L) >> 48);
  bytes[5] = (byte)((longval & 0x0000ff0000000000L) >> 40);
  bytes[4] = (byte)((longval & 0x000000ff00000000L) >> 32);
  bytes[3] = (byte)((longval & 0x00000000ff000000L) >> 24);
  bytes[2] = (byte)((longval & 0x0000000000ff0000L) >> 16);
  bytes[1] = (byte)((longval & 0x000000000000ff00L) >>  8);
  bytes[0] = (byte)( longval & 0x00000000000000ffL);

}

}
