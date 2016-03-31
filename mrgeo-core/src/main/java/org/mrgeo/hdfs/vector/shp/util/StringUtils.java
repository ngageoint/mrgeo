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

package org.mrgeo.hdfs.vector.shp.util;

import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;

@SuppressWarnings("unchecked")
public class StringUtils
{

  public static String capitalizeFirst(String str)
  {
    if (str == null)
      return null;
    if (str.length() > 1)
    {
      return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
    else if (str.length() == 1)
    {
      return str.toUpperCase();
    }
    else
    {
      return str;
    }
  }

  public static String center(String str, int length)
  {
    if (str == null)
      return pad(length);
    int len = str.length();
    int diff = length - len;
    if (diff <= 0)
      return str;
    int left = diff / 2;
    int right = diff - left;
    String temp = "";
    temp += pad(left) + str + pad(right);
    return temp;
  }

  public static String getLeft(String str, int length)
  {
    return getLeft(str, length, false);
  }

  public static String getLeft(String str, int length, boolean indicator)
  {
    if (length < 0)
      return str;
    if (str == null)
      return null;
    if (str.length() > length)
    {
      if (indicator && length > 3)
      {
        return str.substring(0, length - 3) + "...";
      }
      return str.substring(0, length);
    }
    return str;
  }

  public static String getPaddedLeft(String str, int length)
  {
    if (str == null)
      return null;
    String temp = "";
    if (str.length() > length)
    {
      temp += str;
      return temp.substring(0, length);
    }
    temp = pad(str, length, ' ');
    return temp;
  }

  public static String getTableFormat(String str, int length)
  {
    if (str == null)
      return pad("<null>", length);
    String temp = getLeft(str, length, true);
    return pad(temp, length);
  }

  public static String insert(String str, int pos, String insertion)
  {
    if (str == null)
      return null;
    if (insertion == null)
      return str;
    StringBuffer result = new StringBuffer();
    result.append(str);
    result.insert(pos, insertion);
    return result.toString();
  }

  public static Object[] pack(String str)
  {
    return pack(str, ",");
  }

  public static Object[] pack(String str, String separator)
  {
    StringTokenizer st = new StringTokenizer(str, separator);
    Object[] temp = new Object[st.countTokens()];
    int i = 0;
    while (st.hasMoreTokens())
    {
      temp[i++] = st.nextToken();
    }
    return temp;
  }

  public static String pad(int length)
  {
    return pad("", length, ' ');
  }

  public static String pad(int length, char padding)
  {
    return pad("", length, padding);
  }

  public static String pad(String str, int length)
  {
    return pad(str, length, ' ');
  }

  public static String pad(String str, int length, boolean random)
  {
    if (str == null)
      return null;
    StringBuffer result = new StringBuffer();
    result.append(str);
    Random r = null;
    if (random)
      r = new Random();
    if (result.length() < length)
    {
      int n = length - result.length();
      for (int i = 0; i < n; i++)
        result.append((r != null) ? (char) (r.nextInt(126 - 32) + 32 + 1) : ' ');
    }
    return result.toString();
  }

  public static String pad(String str, int length, char padding)
  {
    if (str == null)
      return null;
    StringBuffer result = new StringBuffer();
    result.append(str);
    if (result.length() < length)
    {
      int n = length - result.length();
      for (int i = 0; i < n; i++)
        result.append(padding);
    }
    return result.toString();
  }

  public static String removeExtraInnerSpaces(String str)
  {
    if (str == null)
      return null;
    String temp = "" + str;
    while (temp.indexOf("  ") != -1)
      temp = replace(temp, "  ", " ");
    return temp;
  }

  public static String removeInnerSpaces(String str)
  {
    return replace(str, " ", "");
  }

  public static String replace(String str, String pattern, String replace)
  {
    if (str == null)
      return null;
    int s = 0;
    int e = 0;
    StringBuffer result = new StringBuffer();
    while ((e = str.indexOf(pattern, s)) >= 0)
    {
      result.append(str.substring(s, e));
      result.append(replace);
      s = e + pattern.length();
    }
    result.append(str.substring(s));
    return result.toString();
  }

  public static String[] split(String str, String delimiter)
  {
    if (str == null)
      return null;
    String[] temp = null;
    if (delimiter == null || delimiter.length() == 0)
    {
      temp = new String[0];
      temp[0] = str;
    }
    else
    {
      @SuppressWarnings("rawtypes")
      Vector v = new Vector(1);
      int pos = 0;
      int next = str.indexOf(delimiter);
      while (next != -1)
      {
        String fragment = str.substring(pos, next);
        v.add(fragment);
        pos += fragment.length() + delimiter.length();
        next = str.indexOf(delimiter, pos);
      }
      v.add(str.substring(pos));
      temp = new String[v.size()];
      v.toArray(temp);
    }
    // return
    return temp;
  }

  public static String trimExtended(String str)
  {
    if (str == null)
      return null;
    StringBuffer result = new StringBuffer();
    str = str.trim();
    int s = 0;
    int e = 0;
    while ((e = str.indexOf("  ", s)) >= 0)
    {
      result.append(str.substring(s, e));
      s = e + 1;
    }
    result.append(str.substring(s));
    return result.toString();
  }

  public static String trimLeft(String str, int length)
  {
    if (str == null)
      return null;
    if (str.length() <= length)
      return "";
    StringBuffer result = new StringBuffer();
    result.append(str.substring(str.length() - length));
    return result.toString();
  }

  public static String trimRight(String str, int length)
  {
    if (str == null)
      return null;
    if (str.length() <= length)
      return "";
    if (length <= 0)
      return "" + str;
    StringBuffer result = new StringBuffer();
    result.append(str.substring(0, str.length() - length));
    return result.toString();
  }
}
