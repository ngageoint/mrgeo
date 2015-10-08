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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Properties;

public class Misc extends Object
{

  public static void delay(long ms)
  {
    try
    {
      Thread.sleep(ms);
    }
    catch (InterruptedException ie)
    {
    }
  }

  public static double getPercentage(int portion, int total)
  {
    return getPercentage(portion, total, 0);
  }

  public static double getPercentage(int portion, int total, int rnd)
  {
    if (total == 0)
      return 0;
    double temp = (double) portion / (double) total * 100;
    return roundDouble(temp, rnd);
  }

  public static String getTimeRemaining(long seconds, boolean estimate)
  {
    String temp = "";
    boolean days = false;
    boolean hours = false;
    // days
    if (seconds > 86400)
    {
      temp += (seconds / 86400) + " days ";
      seconds = seconds % 86400;
      days = true;
    }
    // hours
    if (seconds > 3600)
    {
      temp += (seconds / 3600) + "h ";
      seconds = seconds % 3600;
      hours = true;
    }
    // return early if estimating and days present
    if (estimate && days)
      return temp.trim();
    // minutes
    if (seconds > 60)
    {
      temp += (seconds / 60) + "m ";
      seconds = seconds % 60;
    }
    // return early if estimating and hours present
    if (estimate && hours)
      return temp.trim();
    // seconds
    temp += seconds + "s";
    // return
    return temp;
  }

  public static Properties loadProperties(String fileName) throws IOException
  {
    // load database properties
    Properties props = new Properties();
    InputStream is = null;
    try
    {
      is = new java.io.FileInputStream(fileName);
      props.load(is);
    }
    catch (Exception e)
    {
      throw new IOException("Cannot load properties!");
    }
    finally
    {
      // failsafe closure
      try
      {
        if (is != null)
        {
          is.close();
        }
      }
      catch (Exception e)
      {
      }
      is = null;
    }
    // return
    return props;
  }

  public static double roundDouble(double d, int rnd)
  {
    if (rnd < 0)
      return d;
    long rounded = Math.round(d * Math.pow(10, rnd));
    d = rounded / Math.pow(10, rnd);
    return d;
  }

  public static BigDecimal sqrt(BigDecimal x)
  {
    return sqrt(x, x.scale(), 25);
  }

  public static BigDecimal sqrt(BigDecimal x, int scale)
  {
    return sqrt(x, scale, 25);
  }

  public static BigDecimal sqrt(BigDecimal x, int scale, int iterations)
  {
    BigDecimal a = new BigDecimal(".5");
    BigDecimal prev = x.multiply(a);
    for (int i = 0; i < iterations; i++)
      prev = a.multiply(prev.add(x.divide(prev, 30, BigDecimal.ROUND_HALF_EVEN)));
    prev = prev.setScale(scale, BigDecimal.ROUND_HALF_EVEN);
    return prev;
  }
}
