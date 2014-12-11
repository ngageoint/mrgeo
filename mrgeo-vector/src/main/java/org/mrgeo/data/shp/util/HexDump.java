/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.data.shp.util;

public class HexDump
{
  public static String byteToHexString(byte b)
  {
    return intToHexString(b, 2, '0');
  } /* byteToHexString() */

  public static String hexDump(byte data[])
  {
    return hexDump(data, data.length, 0);
  }

  public static String hexDump(byte data[], int length)
  {
    return hexDump(data, length, 0);
  }

  public static String hexDump(byte data[], int length, int offset)
  {
    return hexDump(data, length, offset, 0);
  }

  public static String hexDump(byte data[], int length, int offset, int printoffset)
  {
    int i;
    int j;
    final int bytesPerLine = 16;
    String result = "";

    if (length == -1)
      length = data.length;
    for (i = offset; i < (offset + length); i += bytesPerLine)
    {
      // print the offset as a 4 digit hex number
      result = result + intToHexString(i + printoffset, 4, '0') + "  ";

      // print each byte in hex
      for (j = i; j < (offset + length) && (j - i) < bytesPerLine; j++)
      {
        result = result + byteToHexString(data[j]) + " ";
      }

      // skip over to the ascii dump column
      for (; (j - i) < bytesPerLine; j++)
      { // 0!=(j % bytesPerLine)
        result = result + "   ";
      }

      result = result + "  |";

      // print each byte in ascii
      for (j = i; j < (offset + length) && (j - i) < bytesPerLine; j++)
      {
        if (((data[j] & 0xff) > 0x001f) && ((data[j] & 0xff) < 0x007f))
        {
          Character ch = new Character((char) data[j]);
          result = result + ch;
        }
        else
        {
          result = result + ".";
        }
      }
      if ((i + bytesPerLine) < (offset + length))
      {
        result = result + "|\n";
      }
      else
      {
        result = result + "|";
      }
    }
    return result;
  } /* hexDump() */

  public static String intToHexString(int num, int width, char fill)
  {
    String result = "";

    if (num == 0)
    {
      result = "0";
      width--;
    }
    else
    {
      while (num != 0 && width > 0)
      {
        String tmp = Integer.toHexString(num & 0xf);
        result = tmp + result;
        num = (num >> 4);
        width--;
      }
    }
    for (; width > 0; width--)
    {
      result = fill + result;
    }
    return result;
  } /* intToHexString() */
}
