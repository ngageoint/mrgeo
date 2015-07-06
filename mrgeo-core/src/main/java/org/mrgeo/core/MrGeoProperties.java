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

package org.mrgeo.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class MrGeoProperties {
    private static Properties properties;

    private static Logger log = LoggerFactory.getLogger(MrGeoProperties.class);

    private MrGeoProperties() {

      // not even this class can instantiate the constructor
      throw new AssertionError();

    } // end base constructor


    public static Properties getInstance() {
        if( properties == null ) {
            // This reduces concurrency to just when properties need creating, making access more efficient once created
            synchronized (MrGeoProperties.class) {
                if ( properties == null ) {
                    properties = new Properties();
                    String home = System.getenv( MrGeoConstants.MRGEO_ENV_HOME );

                    if( home == null ) {
                        // Try loading from properties file. This is the method used for
                        // JBoss deployments
                        try {
                            properties.load( MrGeoProperties.class.getClassLoader().getResourceAsStream( MrGeoConstants.MRGEO_SETTINGS ) );
                        } catch( Exception e ) {
                            // An empty props object is fine
                        }
                        home = properties.getProperty( MrGeoConstants.MRGEO_ENV_HOME );
                        // If we loaded properties from JBoss module, no need to load from file system
                        if ( home == null ) log.error(MrGeoConstants.MRGEO_ENV_HOME + " environment variable not set!");
                    }
                    if ( home != null )
                    {
                        if(! home.endsWith(File.separator)){
                            home += File.separator;
                        }
                        try {

                            File mrgeoConf = new File( home + MrGeoConstants.MRGEO_CONF );
                            if( mrgeoConf.exists() ) {
                                properties.load( new FileInputStream( mrgeoConf ) );
                            }
                        } catch( FileNotFoundException e ) {
                            e.printStackTrace();
                        } catch( IOException e ) {
                            e.printStackTrace();
                        }
                    } else {
                        log.error(MrGeoConstants.MRGEO_ENV_HOME + " environment variable not set!");
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
}
