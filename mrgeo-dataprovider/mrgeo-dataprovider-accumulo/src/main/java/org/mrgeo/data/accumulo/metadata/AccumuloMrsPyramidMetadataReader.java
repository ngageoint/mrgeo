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

package org.mrgeo.data.accumulo.metadata;

import com.google.common.base.Predicates;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.accumulo.image.AccumuloMrsImageDataProvider;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.image.MrsPyramidMetadataReaderContext;
import org.mrgeo.image.MrsPyramidMetadata;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class AccumuloMrsPyramidMetadataReader implements MrsPyramidMetadataReader
{

  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidMetadataReader.class);

  private MrsPyramidMetadata metadata = null;
  private final AccumuloMrsImageDataProvider dataProvider;
  private final String name;

  private Connector conn = null;
  private Authorizations auths = null;
  private ColumnVisibility cv = null;
  	
  
  public AccumuloMrsPyramidMetadataReader(AccumuloMrsImageDataProvider dataProvider,
                                          MrsPyramidMetadataReaderContext context){
    this.dataProvider = dataProvider;
    this.name = dataProvider.getResourceName();
    if(dataProvider.getColumnVisibility() == null){
    	this.cv = new ColumnVisibility();
    } else {
    	this.cv = dataProvider.getColumnVisibility();
    }
 
  } // end constructor
  
  public AccumuloMrsPyramidMetadataReader(String name){
    this.dataProvider = null;
    this.name = name;
    this.cv = new ColumnVisibility();
  }
  
  public void reset(MrsPyramidMetadata m){
	  metadata = m;
  }
  
  @Override
  public MrsPyramidMetadata read() throws IOException
  {
//    if(dataProvider == null){
//      throw new IOException("DataProvider not set!");
//    }
//    String name = dataProvider.getResourceName();
  
    if(name == null || name.length() == 0){
      throw new IOException("Can not load metadata, resource name is empty!");
    }

    if(metadata != null){
    	return metadata;
    }

    metadata = loadMetadata();
    
    // what if the loadMetadata fails?
    if(metadata == null){
    	log.info("Did not read metadata for " + name);
      Thread.dumpStack();
        throw new IOException("Can not load metadata, resource name is empty!");
    } else {
    	metadata.setPyramid(name);        
    	log.info("Read metadata for " + name + " with max zoom level at " + metadata.getMaxZoomLevel());
    }
    
    return metadata;

  } // end read

  @Override
  public MrsPyramidMetadata reload() throws IOException
  {
    if (metadata == null)
    {
      return read();
    }
    
    if (dataProvider == null)
    {
      throw new IOException("DataProvider not set!");
    }

    String name = dataProvider.getResourceName();
    if (name == null || name.length() == 0)
    {
      throw new IOException("Can not load metadata, resource name is empty!");
    }

    MrsPyramidMetadata copy = loadMetadata();

    Set<Method> getters = ReflectionUtils.getAllMethods(MrsPyramidMetadata.class,
        Predicates.<Method>and (
            Predicates.<AnnotatedElement>not(ReflectionUtils.withAnnotation(JsonIgnore.class)),
            ReflectionUtils.withModifier(Modifier.PUBLIC), 
            ReflectionUtils.withPrefix("get"), 
            ReflectionUtils. withParametersCount(0)));

    Set<Method> setters = ReflectionUtils.getAllMethods(MrsPyramidMetadata.class,
        Predicates.<Method>and(
            Predicates.<AnnotatedElement>not(ReflectionUtils.withAnnotation(JsonIgnore.class)),
            ReflectionUtils.withModifier(Modifier.PUBLIC), 
            ReflectionUtils.withPrefix("set"), 
            ReflectionUtils. withParametersCount(1)));


    //    System.out.println("getters");
    //    for (Method m: getters)
    //    {
    //      System.out.println("  " + m.getName());
    //    }
    //    System.out.println();
    //  
    //    System.out.println("setters");
    //    for (Method m: setters)
    //    {
    //      System.out.println("  " + m.getName());
    //    }
    //    System.out.println();

    for (Method getter: getters)
    {
      String gettername = getter.getName();
      String settername = gettername.replaceFirst("get", "set");

      for (Method setter: setters)
      {
        if (setter.getName().equals(settername))
        {
          //          System.out.println("found setter: " + setter.getName() + " for " + getter.getName() );
          try
          {
            setter.invoke(metadata, getter.invoke(copy, new Object[] {}));
          }
          catch (IllegalAccessException e)
          {
          }
          catch (IllegalArgumentException e)
          {
          }
          catch (InvocationTargetException e)
          {
          }
          break;
        }
      }
    }

    return metadata;
  } // end reload

  public void setConnector(Connector conn){
    this.conn = conn;
  } // end setConnector

  private MrsPyramidMetadata loadMetadata() throws IOException{

    Properties mrgeoAccProps = AccumuloConnector.getAccumuloProperties();
    String authsString = mrgeoAccProps.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
    Properties p1 = null;
    Properties p2 = null;
    
    if(dataProvider == null){
    	//log.info("no data provider used");
      mrgeoAccProps = AccumuloConnector.getAccumuloProperties();
    } else {

    	// get authorizations from dataProvider
    	p1 = AccumuloUtils.providerPropertiesToProperties(dataProvider.getProviderProperties());
    	p2 = dataProvider.getQueryProperties();
      if(p1 != null){
    	  mrgeoAccProps.putAll(p1);
      }
      if(p2 != null){
    	  mrgeoAccProps.putAll(p2);
      }

    }
    
    if(p1 != null){
    	if(p1.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS) != null && 
    			p1.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS).length() > 0){
    		authsString = p1.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
    	}    	
    }
    if(mrgeoAccProps.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES) != null &&
    		mrgeoAccProps.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES).length() > 0){
    	authsString = mrgeoAccProps.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
    }

    auths = AccumuloUtils.createAuthorizationsFromDelimitedString(authsString);

    if(conn == null){
      try{
        conn = AccumuloConnector.getConnector(mrgeoAccProps);
      } catch(DataProviderException dpe){
        dpe.printStackTrace();
        throw new RuntimeException("No connection to Accumulo!");
      }
    }
    
    if(name == null || name.length() == 0){
      throw new IOException("Can not load metadata, resource name is empty!");
    }
    
    Scanner scan = null;
    try{
      scan = conn.createScanner(name, auths);
    } catch(Exception e){
      throw new IOException("Can not connect to table " + name + " with auths " + auths + " - " + e.getMessage());
    }

    MrsPyramidMetadata retMeta = null;
    Range range = new Range(MrGeoAccumuloConstants.MRGEO_ACC_METADATA, MrGeoAccumuloConstants.MRGEO_ACC_METADATA + " ");
    scan.setRange(range);
    scan.fetchColumn(new Text(MrGeoAccumuloConstants.MRGEO_ACC_METADATA), new Text(MrGeoAccumuloConstants.MRGEO_ACC_CQALL));
    for(Entry<Key, Value> entry : scan){
      ByteArrayInputStream bis = new ByteArrayInputStream(entry.getValue().get());
      retMeta = MrsPyramidMetadata.load(bis);
      bis.close();
      break;
    }
    
    return retMeta;
  } // end loadMetadata
  
  
//  private String metadataToString(){
//    if(metadata == null){
//      return null;
//    }
//    String retStr = null;
//    try{
//      ByteArrayOutputStream bos = new ByteArrayOutputStream();
//      metadata.save(bos);
//      retStr = new String(bos.toByteArray());
//      bos.close();
//    } catch(IOException ioe){
//      return null;
//    }
//
//    return retStr;
//  } // end toString
  
} // end AccumuloMrsPyramidMetadataReader
