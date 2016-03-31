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

import org.mrgeo.hdfs.vector.shp.esri.geom.Coord;
import org.mrgeo.hdfs.vector.shp.exception.PropertyNotFoundException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;


public class CoordinateConvert
{
  private static transient ConfigurationFile cf;
  public static String command = "bin/convert.exe";
  public static String finalmgrs;

  public static String checkMGRS(String string)
  {
    boolean status = false;
    boolean bad = false;
    finalmgrs = "";
    // get sql resource
    String mgrsgrid = null;
    try
    {
      cf = new ConfigurationFile("config.properties");
      mgrsgrid = cf.getProperty("convert.mgrsgrid");
    }
    catch (PropertyNotFoundException e1)
    {
      e1.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    String temp = string.toUpperCase();
    System.out.println("Initial MGRS: " + temp);
    System.out.println("Length: " + temp.trim().length());
    // Try to convert first couple numbers to int
    try
    {
      Integer.parseInt(temp.substring(0, 1));
      Integer.parseInt(temp.substring(1, 2));
      System.out.println("First 2 positions are numeric.");
    }
    catch (java.lang.NumberFormatException e)
    {
      temp = mgrsgrid + temp;
      System.out.println("First 2 positions are not numeric, prepending " + mgrsgrid
          + " for final value of " + temp + ".");
    }

    for (int i = 5; i < temp.length(); i++)
    {
      try
      {
        Integer.parseInt(temp.substring(i, i + 1));
      }
      catch (java.lang.NumberFormatException e)
      {
        status = false;
        bad = true;
        break;
      }
    }

    if (temp.length() > 10 && bad == false)
    {
      switch (temp.length())
      {
      case 11:
        finalmgrs = temp.substring(0, 8) + "00" + temp.substring(8, 11) + "00";
        status = true;
        break;
      case 13:
        finalmgrs = temp.substring(0, 9) + "0" + temp.substring(9, 13) + "0";
        status = true;
        break;
      case 15:
        finalmgrs = temp;
        status = true;
        break;
      default:
        status = false;
      }
    }

    System.out.println("FinalMGRS: " + finalmgrs);
    if (status == false)
    {
      finalmgrs = null;
    }
    return finalmgrs;
  }

  public static Coord convertMGRS2DD(String mgrs) throws IOException
  {
    // check args
    if (mgrs == null)
      throw new IOException("Missing 'mgrs' argument!");

    // build arguments
    String[] args = new String[3];
    args[0] = command;
    args[1] = "MGRS2DD";

    Coord c = new Coord();

    // Check for valid MGRS coord
    String s = checkMGRS(mgrs);
    if (s == null)
    {
      try
      {
        throw new IOException("failed MGRS conversion!");
      }
      catch (IOException e)
      {
        System.out.println("Error: bad mgrs coord");
      }
    }
    else
    {

      args[2] = finalmgrs;

      // execute command
      Process child = Runtime.getRuntime().exec(args);

      // get input stream to read from it
      BufferedReader in = new BufferedReader(new InputStreamReader(child.getInputStream()));
      String line = null;
      while ((line = in.readLine()) != null)
      {
        if (line.equals("-1"))
          throw new IOException("MGRS argument could not be converted!");
        if (line.equals("-2"))
          throw new IOException("Cannot find '.dat' file!");
        StringTokenizer st = new StringTokenizer(line, " ");
        try
        {
          c.x = Double.parseDouble(st.nextToken());
          c.y = Double.parseDouble(st.nextToken());
        }
        catch (NumberFormatException e)
        {
          throw new IOException("Error during conversion!");
        }
        break; // only expecting 1 line
      }
      in.close();
    }

    // return
    return c;
  }

  public static Coord convertMGRS2UTM(String zone, String mgrs) throws IOException
  {
    // check args
    if (zone == null)
      throw new IOException("Missing 'zone' argument!");
    if (mgrs == null)
      throw new IOException("Missing 'mgrs' argument!");

    // build arguments
    String[] args = new String[4];
    args[0] = command;
    args[1] = "MGRS2UTM";
    args[2] = zone;
    args[3] = mgrs;

    // execute command
    Process child = Runtime.getRuntime().exec(args);
    Coord c = new Coord();

    // Check for valid MGRS coord
    String s = checkMGRS(mgrs);
    if (s == null)
    {
      try
      {
        throw new IOException("failed MGRS conversion!");
      }
      catch (IOException e)
      {
        System.out.println("Error: bad mgrs coord");
      }
    }
    else
    {

      // get input stream to read from it
      BufferedReader in = new BufferedReader(new InputStreamReader(child.getInputStream()));
      String line = null;
      while ((line = in.readLine()) != null)
      {
        if (line.equals("-1"))
          throw new IOException("MGRS argument could not be converted!");
        if (line.equals("-2"))
          throw new IOException("Cannot find '.dat' file!");
        StringTokenizer st = new StringTokenizer(line, " ");
        try
        {
          c.x = Double.parseDouble(st.nextToken());
          c.y = Double.parseDouble(st.nextToken());
        }
        catch (NumberFormatException e)
        {
          throw new IOException("Error during conversion!");
        }
        break; // only expecting 1 line
      }
      in.close();
    }

    // return
    return c;
  }

  public static String getfinalmgrs()
  {
    return finalmgrs;
  }

  public static void main(String[] args)
  {
    try
    {
      // build arguments
      String[] temp = args;
      args = new String[args.length + 1];
      args[0] = command;
      System.arraycopy(temp, 0, args, 1, temp.length);

      // execute command
      Process child = Runtime.getRuntime().exec(args);

      // get input stream to read from it
      BufferedReader in = new BufferedReader(new InputStreamReader(child.getInputStream()));
      String line = null;
      while ((line = in.readLine()) != null)
      {
        System.out.println(">" + line);
      }
      in.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
}
