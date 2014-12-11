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

import org.mrgeo.data.shp.exception.EnvironmentVariableNotFoundException;
import org.mrgeo.data.shp.exception.MissingArgumentException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;


/**
 * Initialization utility functions.
 */
@SuppressWarnings("unchecked")
public class Initialization
{

  /**
   * @param argm
   * @param key
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static boolean getAsBoolean(HashMap argm, String key) throws MissingArgumentException
  {
    if (argm == null)
      throw new NullPointerException();
    String temp = (String) argm.get(key);
    if (temp == null)
      throw new MissingArgumentException("Missing required argument [" + key + "].");
    return new Boolean(temp).booleanValue();
  }

  /**
   * @param argm
   * @param key
   * @param defaultvalue
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static boolean getAsBoolean(HashMap argm, String key, boolean defaultvalue)
  {
    try
    {
      return getAsBoolean(argm, key);
    }
    catch (MissingArgumentException e)
    {
      return defaultvalue;
    }
  }

  /**
   * @param argm
   * @param key
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static double getAsDouble(HashMap argm, String key) throws MissingArgumentException,
      NumberFormatException
  {
    if (argm == null)
      throw new NullPointerException();
    String arg = (String) argm.get(key);
    if (arg == null)
      throw new MissingArgumentException("Missing required argument [" + key + "].");
    return Double.parseDouble(arg);
  }

  /**
   * @param argm
   * @param key
   * @param defaultvalue
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static double getAsDouble(HashMap argm, String key, double defaultvalue)
      throws NumberFormatException
  {
    try
    {
      return getAsDouble(argm, key);
    }
    catch (MissingArgumentException e)
    {
      return defaultvalue;
    }
  }

  /**
   * @param argm
   * @param key
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static int getAsInteger(HashMap argm, String key) throws NumberFormatException,
      MissingArgumentException
  {
    if (argm == null)
      throw new NullPointerException();
    String arg = (String) argm.get(key);
    if (arg == null)
      throw new MissingArgumentException("Missing required argument [" + key + "].");
    return Integer.parseInt(arg);
  }

  /**
   * @param argm
   * @param key
   * @param defaultvalue
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static int getAsInteger(HashMap argm, String key, int defaultvalue)
      throws NumberFormatException
  {
    try
    {
      return getAsInteger(argm, key);
    }
    catch (MissingArgumentException e)
    {
      return defaultvalue;
    }
  }

  /**
   * @param argm
   * @param key
   * @param delimiter
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static int[] getAsIntegerArray(HashMap argm, String key, String delimiter)
      throws NumberFormatException, MissingArgumentException
  {
    String raw = getAsString(argm, key);
    StringTokenizer st = new StringTokenizer(raw, delimiter);
    int[] temp = new int[st.countTokens()];
    int i = 0;
    while (st.hasMoreTokens())
    {
      temp[i++] = Integer.parseInt(st.nextToken());
    }
    return temp;
  }

  /**
   * @param argm
   * @param key
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static String getAsString(HashMap argm, String key) throws MissingArgumentException
  {
    if (argm == null)
      throw new NullPointerException();
    String arg = (String) argm.get(key);
    if (arg == null)
      throw new MissingArgumentException("Missing required argument [" + key + "].");
    return arg;
  }

  /**
   * @param argm
   * @param key
   * @param defaultvalue
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static String getAsString(HashMap argm, String key, String defaultvalue)
  {
    try
    {
      return getAsString(argm, key);
    }
    catch (MissingArgumentException e)
    {
      return defaultvalue;
    }
  }

  /**
   * @param argm
   * @param string
   * @param string2
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static String[] getAsStringArray(HashMap argm, String key, String delimiter)
      throws MissingArgumentException
  {
    String raw = getAsString(argm, key);
    StringTokenizer st = new StringTokenizer(raw, delimiter);
    String[] temp = new String[st.countTokens()];
    int i = 0;
    while (st.hasMoreTokens())
    {
      temp[i++] = st.nextToken();
    }
    return temp;
  }

  /**
   * @param file
   * @return
   */
  public static String getConfig(String file)
  {
    // try reverse engineering from path variable
    try
    {
      String test = getEnvironmentVariable("PATH").toLowerCase();
      int pos = test.indexOf("orion");
      if (pos != -1)
      {
        int pos2 = test.indexOf(";", pos);
        int pos3 = test.lastIndexOf(";", pos);
        if (pos2 == -1)
        {
          test = test.substring(pos3 + 1).trim();
        }
        else
        {
          test = test.substring(pos3 + 1, pos2).trim();
        }
        test = test + System.getProperty("file.separator") + file;
        File f = new File(test);
        if (f.exists())
          return test;
      }
    }
    catch (Exception e)
    {
    }
    // try ORION_HOME\bin first
    try
    {
      String test = getEnvironmentVariable("ORION_HOME") + System.getProperty("file.separator")
          + "bin" + System.getProperty("file.separator") + file;
      File f = new File(test);
      if (f.exists())
        return test;
    }
    catch (Exception e)
    {
    }
    // try user dir
    try
    {
      String test = System.getProperty("user.dir") + System.getProperty("file.separator") + file;
      File f = new File(test);
      if (f.exists())
        return test;
    }
    catch (Exception e)
    {
    }
    // try User's My Documents
    try
    {
      String test = System.getProperty("user.home") + System.getProperty("file.separator")
          + "My Documents" + System.getProperty("file.separator") + file;
      File f = new File(test);
      if (f.exists())
        return test;
    }
    catch (Exception e)
    {
    }
    // return default (without test)
    return System.getProperty("user.home") + System.getProperty("file.separator") + file;
  }

  /**
   * Returns an environment variable. This operation is costly.
   * 
   * @param key
   * @return
   * @throws IOException
   */
  public static String getEnvironmentVariable(String key) throws IOException,
      EnvironmentVariableNotFoundException
  {
    Process p = Runtime.getRuntime().exec("CMD /c \"echo %" + key + "%\"");
    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String temp = reader.readLine();
    if (temp == null || temp.length() == 0)
      throw new EnvironmentVariableNotFoundException();
    return temp;
  }

  /**
   * Simple function that returns the config file for a user.
   * 
   * @return The config file in the user's directory.
   */
  public static String getUserHome()
  {
    return System.getProperty("user.home") + System.getProperty("file.separator");
  }

  public static void main(String[] args)
  {
    Properties props = System.getProperties();
    @SuppressWarnings("rawtypes")
    Iterator i = props.keySet().iterator();
    while (i.hasNext())
    {
      Object key = i.next();
      System.out.println(key + " = " + props.getProperty((String) key));
    }
  }

  /**
   * Parses arguments in the specified form where every argument trails a unique
   * flag (e.g., "run.bat -a 1 -b 2 -c 3").
   * 
   * @param args
   *          Command line argumens to parse.
   * @return A HashMap containing the arguments hashed by their respective
   *         flags.
   */
  @SuppressWarnings("rawtypes")
  public static HashMap parseArgs(String[] args)
  {
    HashMap argm = new HashMap(args.length);
    for (int i = 0; i < (args.length - 1); i = i + 2)
    {
      argm.put(args[i], args[i + 1]);
    }
    return argm;
  }
}
