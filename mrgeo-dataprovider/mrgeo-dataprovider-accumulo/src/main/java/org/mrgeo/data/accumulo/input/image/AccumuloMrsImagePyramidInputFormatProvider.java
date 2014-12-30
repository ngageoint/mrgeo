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

package org.mrgeo.data.accumulo.input.image;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.accumulo.image.AccumuloMrsImagePyramidInputFormat;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImageInputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AccumuloMrsImagePyramidInputFormatProvider extends MrsImageInputFormatProvider
{

  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsImagePyramidInputFormatProvider.class);

  //private ArrayList<Integer> zoomLevelsInPyramid;

  private String table;
  private Authorizations auths;
  private Properties props;
  
  public AccumuloMrsImagePyramidInputFormatProvider(TiledInputFormatContext context)
  {
    super(context);
    this.table = context.getFirstInput();
  } // end constructor
  
  public AccumuloMrsImagePyramidInputFormatProvider(Properties props, TiledInputFormatContext context)
  {
    super(context);
    this.table = context.getFirstInput();
    this.props = new Properties();
    this.props.putAll(props);
  } // end constructor
  
  
  @Override
  public InputFormat<TileIdWritable, RasterWritable> getInputFormat(String input)
  {

    table = input;
    if(table.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
      table = table.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
    }
    
//    if(context.getBounds() == null){
//      //return new AccumuloMrsImagePyramidInputFormat(input, context.getZoomLevel());
//      return null;
//    } else {
      return new AccumuloMrsImagePyramidInputFormat();
//    }
    
//    if(context.getBounds() == null){
//      log.debug("instantiating input format");
//      
//      //return new AccumuloMrsImagePyramidInputFormat(input, context.getZoomLevel());
//      return null;
//    } //else {
//      // don't know what the AllTilesSingle means
//      //return new AccumuloMrsImagePyramidAllTilesSingleInputFormat();
//    //}
//    
//    return null;
    
  } // end getInputFormat
  
  @Override
  public void setupJob(Job job,
      final Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);

    //zoomLevelsInPyramid = new ArrayList<Integer>();

    log.info("Setting up job " + job.getJobName());
    
    // set the needed information
    if(props == null){
      props = new Properties();
      props.putAll(AccumuloConnector.getAccumuloProperties());
    }
    
    String connUser = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER);
    log.info("connecting to accumulo as user " + connUser);
    
    if(providerProperties != null){
    	props.putAll(providerProperties);
    }
    if(props.size() == 0){
      throw new RuntimeException("No configuration for Accumulo!");
    }

    // just in case this gets overwritten
    
    for(String k : MrGeoAccumuloConstants.MRGEO_ACC_KEYS_CONNECTION){
      job.getConfiguration().set(k, props.getProperty(k));
    }
    for(String k : MrGeoAccumuloConstants.MRGEO_ACC_KEYS_DATA){
      job.getConfiguration().set(k, props.getProperty(k));
    }
    
    if(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE) == null){
      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE, this.table);
    } else {
      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE,
          props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE));
    }
    
    // make sure the password is set with Base64Encoding
    String pw = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD);
    String isEnc = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64, "false");
    String pwDec = pw;

    if(isEnc.equalsIgnoreCase("true")){
      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
          props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
      
      pwDec = new String(Base64.decodeBase64(pw.getBytes()));
      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
          pwDec);


    } else {
      byte[] p = Base64.encodeBase64(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD).getBytes());
      
      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
          new String(p));
      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64,
          new String("true"));
    }

    // get the visualizations
    if(job.getConfiguration().get("protectionLevel") != null){
    	job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ,
    			job.getConfiguration().get("protectionLevel"));
    }
    
//    if(props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ)){
//      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ,
//          props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));
//    }

    if(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS) != null){
      auths = new Authorizations(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS).split(","));
    } else {
      auths = new Authorizations();
    }

    String enc = AccumuloConnector.encodeAccumuloProperties(context.getFirstInput());
    job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ENCODED, enc);


    job.setInputFormatClass(AccumuloMrsImagePyramidInputFormat.class);

    // check for base64 encoded password
//    AccumuloMrsImagePyramidInputFormat.setInputInfo(job.getConfiguration(),
//        props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER),
//        pwDec.getBytes(),
//        table,
//        auths);
    
    AccumuloMrsImagePyramidInputFormat.setZooKeeperInstance(job,
        props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE),
        props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS));

    PasswordToken pt = new PasswordToken(
    		//job.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD)
    		pwDec
    		);
    log.info("connecting to accumulo with user " + connUser);
    //log.info("password used to connect is " + job.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
    log.info("password (decoded) used to connect is " + pwDec);
    log.info("scan authorizations are " + auths);
	log.info("authorizations from config = " + job.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS));

    try{
    	AccumuloMrsImagePyramidInputFormat.setConnectorInfo(
    			job,
    			connUser,
    			//props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER),
    			//job.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER),
    			pt);
    } catch(AccumuloSecurityException ase){
    	log.info("problem with authentication elements.");
    	return;
    }
    AccumuloInputFormat.setScanAuthorizations(job, auths);
    
    // get the input table
    for(final String input : context.getInputs()){
      // put encoded string for Accumulo connections 
      
    	
    	//TODO what needs to be done here?
    	log.info("working with source " + input + " with auths = " + auths);
      
    	AccumuloMrsImagePyramidInputFormat.setInputTableName(job, input);
      
      
    } // end for loop
    

    log.info("setting column family to regex " + context.getZoomLevel());
    // think about scanners - set the zoom level of the job
    IteratorSetting regex = new IteratorSetting(51, "regex", RegExFilter.class);
    RegExFilter.setRegexs(regex, null, Integer.toString(context.getZoomLevel()), null, null, false);
    AccumuloMrsImagePyramidInputFormat.addIterator(job, regex);
    
  } // end setupJob

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }

} // end AccumuloMrsImagePyramidInputFormatProvider
