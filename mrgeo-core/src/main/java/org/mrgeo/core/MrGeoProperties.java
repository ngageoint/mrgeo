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

package org.mrgeo.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MrGeoProperties {
  static final String MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY = "mrgeo.encryption.masterPassword";
  static Properties properties;


  private static Logger log = LoggerFactory.getLogger(MrGeoProperties.class);

  private MrGeoProperties() {

    // not even this class can instantiate the constructor
    throw new AssertionError();

  } // end base constructor


  @SuppressFBWarnings(value = {"DE_MIGHT_IGNORE",
          "PATH_TRAVERSAL_IN"}, justification = "Ignored exception causes empty properties object, which is fine, false positive, user can't control the path")
  public static synchronized Properties getInstance() {
    if( properties == null ) {
      // If a master password has been specified, create an EncryptableProperties.  Otherwise create a normal properties
      String masterPassword = System.getProperty(MRGEO_ENCRYPTION_MASTER_PASSWORD_PROPERTY);
      if (masterPassword != null) {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(masterPassword);
        properties = new EncryptableProperties(encryptor);
      }
      else {
        properties = new Properties();
      }
      FileInputStream fis = null;
      try
      {
        String conf = findMrGeoConf();
        fis = new FileInputStream(conf);
        properties.load(fis);
      }
      catch (IOException e)
      {
        // Try loading from properties file. This is the method used for JBoss deployments
        try {
          properties.load( MrGeoProperties.class.getClassLoader().getResourceAsStream( MrGeoConstants.MRGEO_SETTINGS ) );
        }
        catch ( Exception ignored ) {
          // An empty props object is fine
        }
      }
      finally
      {
        if (fis != null)
        {
          try
          {
            fis.close();
          }
          catch (IOException ignored)
          {
          }
        }
      }
    }
    return properties;
  }

  public static boolean isDevelopmentMode()
  {
    boolean developmentMode = false; // default value
    Properties props = getInstance();
    String strDevelopmentMode = props.getProperty(MrGeoConstants.MRGEO_DEVELOPMENT_MODE);
    if (strDevelopmentMode != null && strDevelopmentMode.equalsIgnoreCase("true"))
    {
      developmentMode = true;
    }
    return developmentMode;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "false positive, no user control of path")
  public static String findMrGeoConf() throws IOException
  {
    String conf = System.getenv(MrGeoConstants.MRGEO_CONF_DIR);
    if (conf == null)
    {
      String home = System.getenv(MrGeoConstants.MRGEO_HOME);
      File dir = new File(home, MrGeoConstants.MRGEO_HOME_CONF_DIR);

      if (dir.exists())
      {
        log.error(MrGeoConstants.MRGEO_HOME + " environment variable has been deprecated.  " +
                "Use " + MrGeoConstants.MRGEO_CONF_DIR + " and " + MrGeoConstants.MRGEO_COMMON_HOME + " instead");
        conf = dir.getCanonicalPath();
      }
    }

    File file = new File(conf, MrGeoConstants.MRGEO_CONF);
    if (file.exists())
    {
      return file.getCanonicalPath();
    }

    throw new IOException(MrGeoConstants.MRGEO_CONF_DIR + " not set, or can not find " + file.getCanonicalPath());
  }

  // Test method only!   Clear the properties object, so it can be reloaded later.
  static void clearProperties()
  {
    properties = null;
  }
}
