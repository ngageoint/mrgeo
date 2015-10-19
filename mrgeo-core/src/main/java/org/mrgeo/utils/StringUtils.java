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

package org.mrgeo.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringUtils
{
  /**
   * Joins an array of doubles into a string with the given delimiter.
   * 
   * There has got to be a better way. :(
   * 
   * @param tokens
   * @param delimiter
   * @return
   */
  public static String join(final double[] tokens, final String delimiter)
  {
    StringBuffer buffer = new StringBuffer(String.valueOf(tokens[0]));
    for (int i = 1; i < tokens.length; i++)
    {
      buffer.append(delimiter).append(String.valueOf(tokens[i]));
    }
    return buffer.toString();
  }

  public static String join(final int[] tokens, final String delimiter)
  {
    StringBuffer buffer = new StringBuffer(String.valueOf(tokens[0]));
    for (int i = 1; i < tokens.length; i++)
    {
      buffer.append(delimiter).append(String.valueOf(tokens[i]));
    }
    return buffer.toString();
  }

  public static <T> String join(final T[] tokens, final String delimiter)
  {
    StringBuffer buffer = new StringBuffer(tokens[0].toString());
    for (int i = 1; i < tokens.length; i++)
    {
      buffer.append(delimiter).append(String.valueOf(tokens[i]));
    }
    return buffer.toString();
  }

  public static String join(final Iterable<? extends Object> tokens, final String delimiter)
  {
    Iterator<? extends Object> oIter;
    if (tokens == null || (!(oIter = tokens.iterator()).hasNext()))
      return "";
    StringBuilder oBuilder = new StringBuilder(String.valueOf(oIter.next()));
    while (oIter.hasNext())
    {
      oBuilder.append(delimiter).append(oIter.next());
    }
    return oBuilder.toString();
  }
  
  public static String repeat(String s, int times)
  {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < times; i++)
    {
      buf.append(s);
    }
    return buf.toString();
  }

  public static String toString(final Map<?, ?> tokens)
  {
    StringBuilder buf = new StringBuilder("{ ");
    String comma = "";
    for (Map.Entry<?, ?> e : tokens.entrySet())
    {
      buf.append(comma + e.getKey() + ":" + e.getValue());
      comma = ", ";
    }
    return buf.toString();
  }
  
  public static <T extends Enum<T>> List<String> enumToStringList(Class<T> enumType)
  {
    List<String> values = new ArrayList<String>();
    for (T c : enumType.getEnumConstants())
    {
      values.add(c.name());
    }
    return values;
  }

public static String read(DataInput in) throws IOException
{
  byte[] data = new byte[in.readInt()];
  in.readFully(data);

  return new String(data,"UTF-8");
}

public static void write(String str, DataOutput out) throws IOException
{
  byte[] data=str.getBytes("UTF-8");
  out.writeInt(data.length);
  out.write(data);
}

public static String read(DataInputStream stream) throws IOException
{
  byte[] data = new byte[stream.readInt()];
  stream.readFully(data);

  return new String(data,"UTF-8");
}

public static void write(String str, DataOutputStream stream) throws IOException
{
  byte[] data=str.getBytes("UTF-8");
  stream.writeInt(data.length);
  stream.write(data);

}
}
