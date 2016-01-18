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

package org.mrgeo.data.accumulo.input.image;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.accumulo.image.AccumuloMrsPyramidInputFormat;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageInputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.image.ImageInputFormatContext;
import org.mrgeo.utils.Base64Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AccumuloMrsPyramidInputFormatProvider extends MrsImageInputFormatProvider
{

  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidInputFormatProvider.class);

  //private ArrayList<Integer> zoomLevelsInPyramid;

  private String table;
  private Authorizations auths;
  private Properties props;
  
  public AccumuloMrsPyramidInputFormatProvider(ImageInputFormatContext context)
  {
    super(context);
    this.table = context.getInput();
  } // end constructor
  
  public AccumuloMrsPyramidInputFormatProvider(Properties props, ImageInputFormatContext context)
  {
    super(context);
    this.table = context.getInput();
    this.props = new Properties();
    Properties providerProps = AccumuloUtils.providerPropertiesToProperties(context.getProviderProperties());
    this.props.putAll(providerProps);
  }
  
  
  @Override
  public InputFormat<TileIdWritable, RasterWritable> getInputFormat(String input)
  {

    table = input;
    if(table.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
      table = table.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
    }
    
//    if(context.getBounds() == null){
//      //return new AccumuloMrsPyramidInputFormat(input, context.getZoomLevel());
//      return null;
//    } else {
      return new AccumuloMrsPyramidInputFormat();
//    }
    
//    if(context.getBounds() == null){
//      log.debug("instantiating input format");
//      
//      //return new AccumuloMrsPyramidInputFormat(input, context.getZoomLevel());
//      return null;
//    } //else {
//      // don't know what the AllTilesSingle means
//      //return new AccumuloMrsImagePyramidAllTilesSingleInputFormat();
//    //}
//    
//    return null;
    
  } // end getInputFormat

  @Override
  public Configuration setupSparkJob(final Configuration conf, final MrsImageDataProvider provider)
    throws DataProviderException
  {
    try
    {
      Configuration conf1 = super.setupSparkJob(conf, provider);
      Job job = new Job(conf1);
      setupConfig(job, provider);
      log.info("Accumulo IFP returning configuration " + job.getConfiguration());
      return job.getConfiguration();
    }
    catch(IOException e)
    {
      throw new DataProviderException("Error while configuring Accumulo input format provider for " +
                                      provider.getResourceName(), e);
    }
  }

  @Override
  public void setupJob(Job job,
      final MrsImageDataProvider provider) throws DataProviderException
  {
    super.setupJob(job, provider);

    log.info("Setting up job " + job.getJobName());
    setupConfig(job, provider);
  }

  private void setupConfig(final Job job,
                           final MrsImageDataProvider provider) throws DataProviderException
  {
    Properties oldProviderProperties = AccumuloUtils.providerPropertiesToProperties(provider.getProviderProperties());
    //zoomLevelsInPyramid = new ArrayList<Integer>();

    // lets look into the properties coming in
    if(oldProviderProperties != null){
    	Set<Object> k1 = oldProviderProperties.keySet();
    	ArrayList<String> k2 = new ArrayList<String>();
    	for(Object o : k1){
    		k2.add(o.toString());
    	}
    	Collections.sort(k2);
    	for(int x = 0; x < k2.size(); x++){
    		log.info("provider property " + x + ": k='" + k2.get(x) + "' v='" + oldProviderProperties.getProperty(k2.get(x)) + "'");
    	}
    }
    
    // set the needed information
    if(props == null){
      props = new Properties();
      props.putAll(AccumuloConnector.getAccumuloProperties());
    }
    
    String connUser = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER);
    log.info("connecting to accumulo as user " + connUser);
    
    if(oldProviderProperties != null){
    	props.putAll(oldProviderProperties);
    }
    if(props.size() == 0){
      throw new RuntimeException("No configuration for Accumulo!");
    }

    // just in case this gets overwritten
    
    for(String k : MrGeoAccumuloConstants.MRGEO_ACC_KEYS_CONNECTION){
      String v = props.getProperty(k);
      if (v != null)
      {
        job.getConfiguration().set(k, props.getProperty(k));
      }
    }
    for(String k : MrGeoAccumuloConstants.MRGEO_ACC_KEYS_DATA){
      String v = props.getProperty(k);
      if (v != null)
      {
        job.getConfiguration().set(k, v);
      }
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

      try
      {
        pwDec = Base64Utils.decodeToString(pw);
      }
      catch (IOException | ClassNotFoundException e)
      {
        throw new DataProviderException("Error Decoding Base64", e);
      }

      job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
          pwDec);


    } else {

      try
      {
        String s = Base64Utils.encodeObject(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));

        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD, s);
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64, "true");
      }
      catch (IOException e)
      {
        throw new DataProviderException("Error Base64 encoding", e);
      }

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

    String strAuths = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
    auths = AccumuloUtils.createAuthorizationsFromDelimitedString(strAuths);

    String enc = AccumuloConnector.encodeAccumuloProperties(context.getInput());
    job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ENCODED, enc);


    //job.setInputFormatClass(AccumuloMrsPyramidInputFormat.class);

    // check for base64 encoded password
//    AccumuloMrsPyramidInputFormat.setInputInfo(job.getConfiguration(),
//        props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER),
//        pwDec.getBytes(),
//        table,
//        auths);
    
    AccumuloMrsPyramidInputFormat.setZooKeeperInstance(job,
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
    	AccumuloMrsPyramidInputFormat.setConnectorInfo(
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
    String input = context.getInput();
    // put encoded string for Accumulo connections


    //TODO what needs to be done here?
    log.info("working with source " + input + " with auths = " + auths);

    AccumuloMrsPyramidInputFormat.setInputTableName(job, input);
    log.info("Set accumulo table name " + input + " into Configuration " + job.getConfiguration());


    log.info("setting column family to regex " + Integer.toString(context.getZoomLevel()));
    // think about scanners - set the zoom level of the job
    IteratorSetting regex = new IteratorSetting(51, "regex", RegExFilter.class);
    RegExFilter.setRegexs(regex, null, Integer.toString(context.getZoomLevel()), null, null, false);
    Collection<Pair<Text, Text>> colFamColQual = new ArrayList<Pair<Text,Text>>();
    Pair<Text, Text> p1 = new Pair<Text, Text>(new Text(Integer.toString(context.getZoomLevel())), null);
    colFamColQual.add(p1);
    AccumuloMrsPyramidInputFormat.fetchColumns(job, colFamColQual);
    AccumuloMrsPyramidInputFormat.addIterator(job, regex);
    //job.setJarByClass(this.getClass());
    String cp = job.getConfiguration().get("mapred.job.classpath.files");
    log.info("mapred.job.classpath.files = " + cp);
    
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }

} // end AccumuloMrsPyramidInputFormatProvider
