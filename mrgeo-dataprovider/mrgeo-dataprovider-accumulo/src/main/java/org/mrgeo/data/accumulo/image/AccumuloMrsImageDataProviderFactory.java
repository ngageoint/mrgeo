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

package org.mrgeo.data.accumulo.image;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageDataProviderFactory;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * This class is the entry point for interacting with Accumulo as a
 * data source for the first MrGeo Accumulo implementation.
 * <p>
 * This implementation follows very closely the implementation of
 * the HDFS data source for MrGeo.
 * <p>
 * There are two types of items stored in this implementation of
 * a data provider.  The first entry is for raster.
 * <p>
 * Accumulo key for a raster entry:<br>
 * --rowId: [TMS TileId as 8 bytes of long]<br>
 * ----colFam: [zoom level as string]<br>
 * ------colQual: [TMS TileId long value as string]<br>
 * --------colViz: [protection as needed]<br>
 * ----------timeStamp: [controlled by Accumulo]<br>
 * ------------value: [bytes of raster]<br>
 * <p>
 * The second entry type is for the Metadata object.
 * <p>
 * Accumulo key for Metadata entry:<br>
 * --rowId: METADATA<br>
 * ----colFam: METADATA<br>
 * ------colQual: ALL<br>
 * --------colViz: [protection as needed]<br>
 * ----------timeStamp: [controlled by Accumulo]<br>
 * ------------value: [json string of Metadata objec]<br>
 *
 */
public class AccumuloMrsImageDataProviderFactory implements MrsImageDataProviderFactory
{
  // logging for the class
private static Logger log = LoggerFactory.getLogger(AccumuloMrsImageDataProviderFactory.class);

// connector for interfacing with Accumulo
private AccumuloConnector connector = null;

/*
 * This is a list of the tables that have valid METADATA
 * key = table name
 * value = visualization of the image
 */
private Hashtable<String, String> ADPF_ImageToTable = null;

private Set<String> ADPF_AllTables = null;

// the table that will be utilized for the task
private String table = null;

// the object for connecting to Accumulo with authorizations
//private Properties connectionProps = null;  // this hangs around longer then the queryProps

private String[] KEYS_CONN = { MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE,
    MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS,
    MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER,
    MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD
};
private String[] KEYS_DATA = { MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS,
    MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ
};



/**
 * Basic constructor for the factory.  This constructor does nothing.
 */
public AccumuloMrsImageDataProviderFactory(){
} // end constructor


@Override
public boolean isValid()
{
  try
  {
    Properties props = AccumuloConnector.getAccumuloProperties();
    if (props != null)
    {
      return true;
    }
  }
  catch (DataProviderException e)
  {
    // Got an error - Accumulo provider is not valid
    log.info(e.getMessage());
  }
  log.info("Unable to load accumulo connection properties, accumulo data provider is not valid");
  return false;
}

@Override
public void initialize(Configuration conf) throws DataProviderException
{
  // Make sure that we initialize the connection settings. Note
  // that this call will not do anything if this data provider is
  // being used on the remote side because the settings will have
  // already been initialized by the data provider framework.
  AccumuloConnector.initialize();
}

  /**
 * The instantiation of data providers needs to know about
 * the prefix that will be recognized by each provider.
 *
 * @return The prefix that is recognized by this factory for
 * identifying data sources.  For example, a data resource
 * designated as "accumulo:paris" would be associated with this
 * factory because the "accumulo:" prefix would be matched.
 */
@Override
public String getPrefix()
{
  return MrGeoAccumuloConstants.MRGEO_ACC_PREFIX_NC;
}

  @Override
  public Map<String, String> getConfiguration()
  {
    return AccumuloConnector.getAccumuloPropertiesAsMap();
  }

  @Override
  public void setConfiguration(Map<String, String> properties)
  {
    AccumuloConnector.setAccumuloProperties(properties);
  }

@Override
public MrsImageDataProvider createTempMrsImageDataProvider(ProviderProperties providerProperties) throws IOException
{
  return createMrsImageDataProvider(HadoopUtils.createRandomString(40), providerProperties);
}

/**
 * This is in the context of setting up a job or WMS/WTS queries.
 */
@Override
public MrsImageDataProvider createMrsImageDataProvider(String input,
    final ProviderProperties providerProperties)
{
  // set the table name
  table = input;

  // see if the prefix needs to be stripped away
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    table = table.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
  // return a new interface to the accumulo data instance
  return new AccumuloMrsImageDataProvider(providerProperties, table);

} // end createMrsImageDataProvider


@Override
public boolean canOpen(String input, final ProviderProperties providerProperties)
{
  //TODO: work through the idea of empty and image tables

  if(input == null){
    throw new NullPointerException("Cannot open null.");
  }

  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    //TODO: check if there is an existing table
    return true;
  }

  if(input.contains(":")){
    return false;
  }

  // strip off the prefix if needed
  String t = input;
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    t = input.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
//    if(input.contains("-")){
//    	String[] els = input.split("-");
//    	t = els[0];
//    }

  Properties oldProviderProperties = AccumuloUtils.providerPropertiesToProperties(providerProperties);
  try
  {
    ADPF_AllTables = AccumuloUtils.getListOfTables(oldProviderProperties);
    ADPF_ImageToTable = AccumuloUtils.getGeoTables(oldProviderProperties);


    log.info("Asked to open: " + input + " (" + t + ")");


    return ADPF_AllTables.contains(t);
  }
  catch (DataProviderException e)
  {
    log.error("Failure in Accumulo MrsImage canOpen for input " + input, e);
  }
  return false;
} // end canOpen

/**
 * This is the list of images taken from the tables
 * that can be read from the information in tables.
 * The term image is in reference to a complete image
 * made up of rasters.  Accumulo stores the rasters of
 * a complete image.
 *
 * @returns array of table names
 * @throws IOException when there is a problem connecting to or reading from Accumulo
 */
@Override
public String[] listImages(final ProviderProperties providerProperties) throws IOException
{
  Properties oldProviderProperties = AccumuloUtils.providerPropertiesToProperties(providerProperties);
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(oldProviderProperties);

  ArrayList<String> keys = new ArrayList<String>();
  for(String k : ADPF_ImageToTable.keySet()){
    keys.add(k);
  }
  Collections.sort(keys);

  return (String[]) keys.toArray(new String[keys.size()]);
} // listImages


@Override
public boolean canWrite(String input, final ProviderProperties providerProperties) throws IOException
{
  String t = input;
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    t = input.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
  Properties oldProviderProperties = AccumuloUtils.providerPropertiesToProperties(providerProperties);
  ADPF_AllTables = AccumuloUtils.getListOfTables(oldProviderProperties);
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(oldProviderProperties);

  ArrayList<String> ignoreTables = AccumuloUtils.getIgnoreTables();
  if(ignoreTables.contains(t)){
    return false;
  }

  if(ADPF_AllTables.contains(t) && !ADPF_ImageToTable.contains(t)){
    return false;
  }

  // make the assumption that if the table exists that it is available to be writen
  //return ADPF_AllTables.contains(t);
  return true;

} // canWrite


@Override
public boolean exists(String input, final ProviderProperties providerProperties) throws IOException
{

  Properties oldProviderProperties = AccumuloUtils.providerPropertiesToProperties(providerProperties);
    /*
     * since there is no way to determine if things have changed inside Accumulo
     * there needs to be a refresh of the information of what is there
     */
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(oldProviderProperties);
  if(ADPF_AllTables == null){
    ADPF_AllTables = AccumuloUtils.getListOfTables(oldProviderProperties);
  }

  // clean off the prefix if needed
  String t = input;
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    t = input.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
//    if(t.contains("-")){
//    	String[] els = input.split("-");
//    	t = els[0];
//    }

  if (ADPF_ImageToTable != null)
  {
    // prepare some information for logging
    StringBuffer sb = new StringBuffer();
    for (String k : ADPF_ImageToTable.keySet())
    {
      if (sb.length() != 0)
      {
        sb.append(" ");
      }
      sb.append(k);
    }

    log.info("table " + t + " exists=" +
             Boolean.toString(ADPF_AllTables.contains(t)) +
             " and is a map table=" +
             Boolean.toString(ADPF_ImageToTable.containsKey(t)));
    //log.info("search for '" + input + " -> " + t + "' was " + Boolean.toString(ADPF_ImageToTable.containsKey(t)) + " : " + sb.toString());

    // does the table exist
    return ADPF_ImageToTable.containsKey(t);
  }
  return false;
} // end exists


@Override
public void delete(String name, final ProviderProperties providerProperties) throws IOException
{
  AccumuloConnector.deleteTable(name);
  return;
} // end delete


} // end AccumuloMrsImageDataProviderFactory
