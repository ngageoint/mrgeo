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

package org.mrgeo.data.accumulo.utils;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.codec.binary.Base64;
import org.mrgeo.data.DataProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.SortedSet;




public class AccumuloConnector {
  
  private static Logger log = LoggerFactory.getLogger(AccumuloConnector.class);
  
	private static Connector conn = null;
	
	public static String encodeAccumuloProperties(String r){
	  StringBuffer sb = new StringBuffer();
	  //sb.append(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX);
	  Properties props = getAccumuloProperties();
	  props.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE, r);
	  Enumeration<?> e = props.keys();
	  while(e.hasMoreElements()){
	    String k = (String)e.nextElement();
	    String v = props.getProperty(k);
	    if(sb.length() > 0){
	      sb.append(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_DELIM);
	    }
	    sb.append(k + "=" + v);
	  }
	  byte[] enc = Base64.encodeBase64(sb.toString().getBytes());
	  String retStr = new String(enc);
	  retStr = MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX + retStr;

	  return retStr;
	} // end encodeAccumuloProperties
	
	
	public static Properties decodeAccumuloProperties(String s){
	  Properties retProps = new Properties();
	  
	  if(!s.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX)){
	    return retProps;
	  }
	  String enc = s.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX, "");
	  byte[] decBytes = Base64.decodeBase64(enc.getBytes());
	  String dec = new String(decBytes);
	  String[] pairs = dec.split(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_DELIM);
	  for(String p : pairs){
	    String[] els = p.split("=");
	    if(els.length == 1){
	      retProps.setProperty(els[0], "");
	    } else {
	      retProps.setProperty(els[0], els[1]);
	    }
	  }
	  	  
	  return retProps;
	} // end decodeAccumuloProperties
	
	
	public static boolean isEncoded(String s){
	  return s.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX);
	}
	
	
	// TODO: need to make sure the path is correctly set - think about MRGEO environment variables
	public static String getAccumuloPropertiesLocation(){
	  String conf = System.getenv("MRGEO_HOME");
	  if(conf == null){
	    conf = "/opt/mrgeo";
	  }	  
    if(conf != null){
      if(! conf.endsWith(File.separator)){
        conf += File.separator;
      }
    } else {
      conf = "";
    }
    conf += "conf" + File.separator + MrGeoAccumuloConstants.MRGEO_ACC_CONF_FILE_NAME;
    File f = new File(conf);
    if(! f.exists()){
      return null;
    }
    return conf;
	}
	
	public static Properties getAccumuloProperties(){

	  if(checkMock()){
	    Properties p = new Properties();
	    p.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE, System.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE));
      p.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER, System.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER));
      p.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD, System.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
      p.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS, MrGeoAccumuloConstants.MRGEO_ACC_AUTHS_U);
      return p;
	  }
	  
	  // find the config file
    String conf = getAccumuloPropertiesLocation();
    if (conf != null)
    {
      File f = new File(conf);
      if(! f.exists()){
        return null;
      }
      log.info("using " + f.getAbsolutePath());
  	  
  	  Properties p = new Properties();
      try{
        p.load(new FileInputStream(f));
        return p;
      } catch(FileNotFoundException fnfe){
        return null;
      } catch(Exception e){
        return null;
      }
    }
    return null;
	} // end getAccumuloProperties
	
	public static Connector getConnector() throws DataProviderException{
	  
	  if(checkMock()){
	    return getMockConnector();
	  }
	  
	  // find the config file
	  String conf = getAccumuloPropertiesLocation();
	  if (conf != null)
	  {
  	  File f = new File(conf);
  	  if(f.exists()){
  
  	    log.debug("connector from information in " + f.getAbsolutePath());
  	    
  	    return getConnector(f);
  	  }
	  }

	  // if you are here - now look for the file in the CLASSPATH
	  try{
	    InputStream is = AccumuloConnector.class.getResourceAsStream(MrGeoAccumuloConstants.MRGEO_ACC_CONF_FILE_NAME);
	    Properties p = new Properties();
	    p.load(is);
	    is.close();
	    
	    log.debug("connector information found in classpath file.");
	    
	    return getConnector(p);
	    
	  } catch(Exception e){
	    log.debug("failure to load file from classpath file");
	    e.printStackTrace();
	    throw new DataProviderException("failure to load file from classpath file - " + e.getMessage());
	    
	  }
	  
	  //return null;
	} // end getConnector
	
	public static Connector getConnector(File f) throws DataProviderException{
    Properties p = new Properties();
    try{
      p.load(new FileInputStream(f));
    } catch(FileNotFoundException fnfe){
      throw new DataProviderException(fnfe.getLocalizedMessage());
    } catch(Exception e){
      throw new DataProviderException("problem loading connector for file " + f.getAbsolutePath() + " - " + e.getMessage());
    }
    
    return getConnector(p);
	} // end getConnector - File

	
	public static Connector getConnector(Properties p) throws DataProviderException{
    String pw = p.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD);
    String pwenc = p.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64);
    if(pwenc != null){
      if(pwenc.toLowerCase().equals("true")){
        pw = new String(Base64.decodeBase64(pw.getBytes()));
      }
    }
    
    return getConnector(
        p.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE),
        p.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS),
        p.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER),
        pw
        );
    
	} // end getConnector - File
	
	public static Connector getConnector(String instance, String zookeepers, String user, String pass) throws DataProviderException{
		
	  if(conn != null){
			return conn;
		}
	  if(checkMock()){
	    return getMockConnector(instance, user, pass);
	  }
		
		Instance inst = new ZooKeeperInstance(instance, zookeepers);
		try {
		  conn = inst.getConnector(user, new PasswordToken(pass.getBytes()));
		  return conn;
		} catch (Exception e) {
		  e.printStackTrace();
		  throw new DataProviderException("problem creating connector " + e.getMessage());
		}
	} // end getConnector
	
	
	public static boolean checkMock(){
	  if(System.getProperty("mock") != null){
	    return true;
	  }
	  
	  return false;
	} // end checkMock
	
	public static Connector getMockConnector() throws DataProviderException {
	  
	  return getMockConnector(System.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE),
	                          System.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER),
	                          System.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
	  
	} // end getMockConnector
	
	/**
	 * For testing.
	 * 
	 * @param instance
	 * @param user
	 * @param pass
	 * @return
	 */
	public static Connector getMockConnector(String instance, String user, String pass) throws DataProviderException{
	  Instance mock = new MockInstance(instance);
	  Connector conn = null;
	  try {
	    conn = mock.getConnector(user, pass.getBytes());
	  } catch(Exception e){
	    throw new DataProviderException("problem creating mock connector - " + e.getMessage());
	  }
	  
	  return conn;
	} // end getMockConnector
	
	
	public static String getReadAuthorizations(String curAuths){

		// if the incoming string is valid - use that
		if(curAuths != null){
			return curAuths;
		}

		String retStr = MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS;
		// get the properties of the system
		Properties props = AccumuloConnector.getAccumuloProperties();

		// look for items in the properties
		if(props != null && props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_DEFAULT_READ_AUTHS)){
			String a = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_DEFAULT_READ_AUTHS);

			// check if the default read is "null"
			if(! a.equals("null")){
				// ah - valid defaults
				retStr = a;
			}
		}

		return retStr;
		
	} // end getReadAuthorizations
	
	
	public static void main(String args[]) throws Exception{
//	  String myEnc = AccumuloConnector.encodeAccumuloProperties("rrr");
//	  System.out.println("Encoded: " + myEnc);
//	  Properties props = AccumuloConnector.decodeAccumuloProperties(myEnc);
//	  Enumeration<?> e = props.keys();
//	  while(e.hasMoreElements()){
//	    String k = (String)e.nextElement();
//	    String v = props.getProperty(k);
//	    System.out.println("\t" + k + "\t= " + v);
//	  }

//	  Connector c = AccumuloConnector.getMockConnector("accumulo", "root", "");
//	  Connector c2 = AccumuloConnector.getMockConnector("accumulo", "root", "");
//	  c.tableOperations().create("junk");
//	  SortedSet<String> list = c.tableOperations().list();
//	  for(String l : list){
//	    System.out.println(l);
//	  }
//	  SortedSet<String> list2 = c2.tableOperations().list();
//    for(String l2 : list2){
//      System.out.println(l2);
//    }
	  
	  
	} // end main
		
} // end AccumuloConnector
