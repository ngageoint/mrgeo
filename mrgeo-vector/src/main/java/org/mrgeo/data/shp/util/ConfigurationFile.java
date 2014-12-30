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

import org.mrgeo.data.shp.exception.InvalidPropertyException;
import org.mrgeo.data.shp.exception.PropertyNotFoundException;
import org.mrgeo.data.shp.util.jdbc.JDBCConnectionImpl;
import org.mrgeo.data.shp.util.jdbc.JDBCListener;
import org.mrgeo.data.shp.util.jdbc.JDBCPoolException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;


public class ConfigurationFile
{
  JDBCConnectionImpl.JDBCPool connectionPool = null;
  private Properties props = null;

  public ConfigurationFile()
  {
    props = new Properties();
  }

  public ConfigurationFile(String file) throws IOException
  {
    initialize(file);
  }

  public void connectToDatabase(String prefix) throws JDBCPoolException
  {
    connectToDatabase(prefix, this.getPropertyAsBoolean(prefix + ".debug", false));
  }

  public synchronized void connectToDatabase(String prefix, boolean debug) throws JDBCPoolException
  {
    try
    {
      String db_key = props.getProperty(prefix + ".name", "");
      System.out.println("Initializing pooled database connections for '" + db_key + "'...");
      connectionPool = JDBCConnectionImpl.JDBCPool.getInstance();
      JDBCConnectionImpl.JDBCPool.setDebug(debug);
      connectionPool.initializeImpl(prefix, props);
      // show database list
      System.out.println(connectionPool.debug());
      System.out.println("Ready.");
    }
    catch (SQLException e)
    {
      throw new JDBCPoolException(e.getMessage());
    }
    // add cleanup call once
    if (connectionPool != null)
      return;
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        if (connectionPool != null)
        {
          try
          {
            connectionPool.close();
          }
          catch (Exception e)
          {
            e.printStackTrace(System.err);
          }
        }
      }
    });
  }

  public String getProperty(String key) throws PropertyNotFoundException
  {
    if (!props.containsKey(key))
      throw new PropertyNotFoundException(key);
    return props.getProperty(key);
  }

  public String getProperty(String key, String defaultValue)
  {
    return props.getProperty(key, defaultValue);
  }

  public boolean getPropertyAsBoolean(String key) throws PropertyNotFoundException
  {
    if (!props.containsKey(key))
      throw new PropertyNotFoundException(key);
    return getPropertyAsBoolean(key, false);
  }

  public boolean getPropertyAsBoolean(String key, boolean defaultValue)
  {
    String temp = props.getProperty(key);
    if (temp != null)
    {
      return new Boolean(temp).booleanValue();
    }
    return defaultValue;
  }

  public double getPropertyAsDouble(String key) throws PropertyNotFoundException,
  InvalidPropertyException
  {
    if (!props.containsKey(key))
      throw new PropertyNotFoundException(key);
    return getPropertyAsDouble(key, 0);
  }

  public double getPropertyAsDouble(String key, double defaultValue)
      throws InvalidPropertyException
      {
    String temp = props.getProperty(key);
    if (temp != null)
    {
      try
      {
        return Double.parseDouble(temp);
      }
      catch (NumberFormatException e)
      {
        throw new InvalidPropertyException(key);
      }
    }
    return defaultValue;
      }

  public int getPropertyAsInt(String key) throws PropertyNotFoundException,
  InvalidPropertyException
  {
    if (!props.containsKey(key))
      throw new PropertyNotFoundException(key);
    return getPropertyAsInt(key, 0);
  }

  public int getPropertyAsInt(String key, int defaultValue) throws InvalidPropertyException
  {
    String temp = props.getProperty(key);
    if (temp != null)
    {
      try
      {
        return Integer.parseInt(temp);
      }
      catch (NumberFormatException e)
      {
        throw new InvalidPropertyException(key);
      }
    }
    return defaultValue;
  }

  private void initialize(String file) throws IOException
  {
    props = new Properties();
    InputStream is = null;
    try
    {
      File f = new File(file);
      is = new FileInputStream(f.getCanonicalPath());
      props.load(is);
    }
    finally
    {
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
  }

  public boolean propertyExists(String key)
  {
    String temp = props.getProperty(key);
    return ((temp == null) ? false : true);
  }

  public static void registerDatabaseListener(JDBCListener listener)
  {
    JDBCConnectionImpl.JDBCPool.addListener(listener);
  }
}
