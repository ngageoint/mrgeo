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

package org.mrgeo.hdfs.vector.shp.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public final class Convert
{
  private static DecimalFormat ndf = null;
  private static DecimalFormat pdf = null;

  static
  {
    pdf = (DecimalFormat) NumberFormat.getInstance();
    pdf.applyPattern("0.000000E000");
    ndf = (DecimalFormat) NumberFormat.getInstance();
    ndf.applyPattern("0.00000E000");
  }

  public static short getByte(byte[] b, int j)
  {
    int ch1 = b[j] & 0xff;
    return (short) (ch1 << 0);
  }

  public static double getDouble(byte[] b, int j)
  {
    // big endian (byte order)
    return Double.longBitsToDouble(getLong(b, j));
  }

  public static int getInteger(byte[] b, int j)
  {
    // big endian (byte order)
    int ch1 = b[j] & 0xff;
    int ch2 = b[j + 1] & 0xff;
    int ch3 = b[j + 2] & 0xff;
    int ch4 = b[j + 3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static double getLEDouble(byte[] b, int j)
  {
    // little endian (byte order)
    return Double.longBitsToDouble(getLELong(b, j));
  }

  public static int getLEInteger(byte[] b, int j)
  {
    // little endian (byte order)
    int ch1 = b[j] & 0xff;
    int ch2 = b[j + 1] & 0xff;
    int ch3 = b[j + 2] & 0xff;
    int ch4 = b[j + 3] & 0xff;
    return ((ch4 << 24) | (ch3 << 16) | (ch2 << 8) | (ch1 << 0));
  }

  public static long getLELong(byte[] b, int j)
  {
    // little endian (byte order)
    long ch1 = b[j] & 0xff;
    long ch2 = b[j + 1] & 0xff;
    long ch3 = b[j + 2] & 0xff;
    long ch4 = b[j + 3] & 0xff;
    long ch5 = b[j + 4] & 0xff;
    long ch6 = b[j + 5] & 0xff;
    long ch7 = b[j + 6] & 0xff;
    long ch8 = b[j + 7] & 0xff;
    return ((ch8 << 56) | (ch7 << 48) | (ch6 << 40) | (ch5 << 32) | (ch4 << 24) | (ch3 << 16)
        | (ch2 << 8) | (ch1 << 0));
  }

  public static short getLEShort(byte[] b, int j)
  {
    // little endian (byte order)
    int ch1 = b[j] & 0xff;
    int ch2 = b[j + 1] & 0xff;
    return (short) ((ch2 << 8) | (ch1 << 0));
  }

  public static long getLong(byte[] b, int j)
  {
    // big endian (byte order)
    long ch1 = b[j] & 0xff;
    long ch2 = b[j + 1] & 0xff;
    long ch3 = b[j + 2] & 0xff;
    long ch4 = b[j + 3] & 0xff;
    long ch5 = b[j + 4] & 0xff;
    long ch6 = b[j + 5] & 0xff;
    long ch7 = b[j + 6] & 0xff;
    long ch8 = b[j + 7] & 0xff;
    return ((ch1 << 56) + (ch2 << 48) + (ch3 << 40) + (ch4 << 32) + (ch5 << 24) + (ch6 << 16)
        + (ch7 << 8) + (ch8 << 0));
  }

  public static short getShort(byte[] b, int j)
  {
    // big endian (byte order)
    int ch1 = b[j] & 0xff;
    int ch2 = b[j + 1] & 0xff;
    return (short) ((ch1 << 8) + (ch2 << 0));
  }

  /** GET'S **/

  public static String getString(byte[] b, int j, int length)
  {
    byte[] strBytes = new byte[length];
    System.arraycopy(b, j, strBytes, 0, length);
    String s = new String(strBytes);
    return s.trim();
  }

  /** SET'S **/

  public static void setBytes(byte[] b, int j, byte[] s)
  {
    // just places bytes
    System.arraycopy(s, 0, b, j, s.length);
  }

  public static void setDouble(byte[] b, int j, double n)
  {
    // big endian (byte order)
    setLong(b, j, Double.doubleToLongBits(n));
  }

  public static String setEString(double d)
  {
    // set exponent string notation
    if (d >= 0)
    {
      String temp = pdf.format(d);
      if (Math.abs(d) < 1 && d != 0)
      {
        return temp;
      }
      return temp.substring(0, 9) + "+" + temp.substring(9);
    }
    String temp = ndf.format(d);
    if (Math.abs(d) < 1)
    {
      return temp;
    }
    return temp.substring(0, 9) + "+" + temp.substring(9);
  }

  public static void setInteger(byte[] b, int j, int n)
  {
    // big endian (byte order)
    b[j + 0] = (byte) ((n >>> 24) & 0xFF);
    b[j + 1] = (byte) ((n >>> 16) & 0xFF);
    b[j + 2] = (byte) ((n >>> 8) & 0xFF);
    b[j + 3] = (byte) ((n >>> 0) & 0xFF);
  }

  public static void setLEDouble(byte[] b, int j, double n)
  {
    // little endian (byte order)
    setLELong(b, j, Double.doubleToLongBits(n));
  }

  public static void setLEInteger(byte[] b, int j, int n)
  {
    // little endian (byte order)
    b[j + 0] = (byte) ((n >>> 0) & 0xFF);
    b[j + 1] = (byte) ((n >>> 8) & 0xFF);
    b[j + 2] = (byte) ((n >>> 16) & 0xFF);
    b[j + 3] = (byte) ((n >>> 24) & 0xFF);
  }

  public static void setLELong(byte[] b, int j, long n)
  {
    // little endian (byte order)
    b[j + 0] = (byte) ((n >>> 0) & 0xFF);
    b[j + 1] = (byte) ((n >>> 8) & 0xFF);
    b[j + 2] = (byte) ((n >>> 16) & 0xFF);
    b[j + 3] = (byte) ((n >>> 24) & 0xFF);
    b[j + 4] = (byte) ((n >>> 32) & 0xFF);
    b[j + 5] = (byte) ((n >>> 40) & 0xFF);
    b[j + 6] = (byte) ((n >>> 48) & 0xFF);
    b[j + 7] = (byte) ((n >>> 56) & 0xFF);
  }

  public static void setLEShort(byte[] b, int j, short n)
  {
    // little endian (byte order)
    b[j + 0] = (byte) ((n >>> 0) & 0xFF);
    b[j + 1] = (byte) ((n >>> 8) & 0xFF);
  }

  public static void setLESingle(byte[] b, int j, float n)
  {
    // little endian (byte order)
    setLEInteger(b, j, Float.floatToIntBits(n));
  }

  public static void setLong(byte[] b, int j, long n)
  {
    // big endian (byte order)
    b[j + 0] = (byte) ((n >>> 56) & 0xFF);
    b[j + 1] = (byte) ((n >>> 48) & 0xFF);
    b[j + 2] = (byte) ((n >>> 40) & 0xFF);
    b[j + 3] = (byte) ((n >>> 32) & 0xFF);
    b[j + 4] = (byte) ((n >>> 24) & 0xFF);
    b[j + 5] = (byte) ((n >>> 16) & 0xFF);
    b[j + 6] = (byte) ((n >>> 8) & 0xFF);
    b[j + 7] = (byte) ((n >>> 0) & 0xFF);
  }

  public static void setRBytes(byte[] b, int j, byte[] s)
  {
    // reverse bytes (switch byte order)
    for (int i = 0; i < s.length; i++)
      b[j + (s.length - 1 - i)] = s[i];
  }

  public static void setRString(byte[] b, int j, int len, String s)
  {
    // reversed string
    setRString(b, j, len, s, (byte) 0);
  }

  public static void setRString(byte[] b, int j, int len, String s, byte padChar)
  {
    // reversed string
    int actual = s.getBytes().length;
    System.arraycopy(s.getBytes(), 0, b, j + (len - actual), actual);
    // pre-pend pad chars
    for (int i = 0; i < (len - actual); i++)
      b[j + i] = padChar;
  }

  public static void setShort(byte[] b, int j, short n)
  {
    // big endian (byte order)
    b[j + 0] = (byte) ((n >>> 8) & 0xFF);
    b[j + 1] = (byte) ((n >>> 0) & 0xFF);
  }

  public static void setSingle(byte[] b, int j, float n)
  {
    // big endian (byte order)
    setInteger(b, j, Float.floatToIntBits(n));
  }

  public static void setString(byte[] b, int j, int len, String s)
  {
    setString(b, j, len, s, (byte) 0);
  }

  public static void setString(byte[] b, int j, int len, String s, byte padChar)
  {
    int actual = s.getBytes().length;
    System.arraycopy(s.getBytes(), 0, b, j, actual);
    // post-pend pad chars
    for (int i = actual; i < len; i++)
      b[j + i] = padChar;
  }

  public static void setString(byte[] b, int j, String s)
  {
    System.arraycopy(s.getBytes(), 0, b, j, s.length());
  }
}
