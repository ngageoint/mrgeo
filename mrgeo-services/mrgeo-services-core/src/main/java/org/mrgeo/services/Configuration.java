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

package org.mrgeo.services;

import org.mrgeo.core.MrGeoConstants;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *
 */
public class Configuration
{
  private static Configuration instance = null;

  /**
   * Made Configuration not throw a checked exception
   * @return Configuration
   */
  public static Configuration getInstance()
  {
    if (instance == null)
    {
      // This reduces the concurrency to just when an instance needs creating versus every access
      synchronized(Configuration.class) {
        if ( instance == null ) instance = new Configuration();
      }
    }
    return instance;
  }

  private Properties properties;

  /**
   * Private constructor to comply with Singleton Pattern
   */
  private Configuration()
  {
    properties = new Properties();

    String home = System.getenv(MrGeoConstants.MRGEO_ENV_HOME);
    if (home == null)
    {

      // Try loading from properties file. This is the method used for
      // JBoss deployments
      try
      {
        properties.load(this.getClass().getClassLoader().getResourceAsStream(MrGeoConstants.MRGEO_SETTINGS));
      }
      catch(Exception e)
      {
        // An empty props object is fine
      }

      home = properties.getProperty(MrGeoConstants.MRGEO_ENV_HOME);

      if (home == null)
      {
        throw new IllegalStateException(MrGeoConstants.MRGEO_ENV_HOME + " environment variable must be set.");
      }
    }
    if (!home.endsWith("/"))
    {
      home += "/";
    }

    // If we read it from JBoss module, no need to load properties from file system

    String conf = home + MrGeoConstants.MRGEO_CONF;

    try {
      FileInputStream fis = new FileInputStream(conf);
      properties.load(fis);
    } catch (IOException ioe) {
      throw new IllegalStateException("Error loading properties", ioe);
    }
  }

  public Properties getProperties()
  {
    return properties;
  }
}
