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

package org.mrgeo.data.accumulo.image;

import org.apache.hadoop.conf.Configuration;
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
@Override
public boolean isValid()
{
  // TODO: This is an initial guess at how this method should be
  // implemented. We need to revisit.
  Properties props = AccumuloConnector.getAccumuloProperties();
  if (props == null)
  {
    return false;
  }
  return true;
}

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
private Properties queryProps = null; // this should be user/query specific

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
    return null;
  }

  @Override
  public void setConfiguration(Map<String, String> properties)
  {
  }

  @Override
public MrsImageDataProvider createTempMrsImageDataProvider(Configuration conf) throws IOException
{
  return createMrsImageDataProvider(HadoopUtils.createRandomString(40), conf);
}

@Override
public MrsImageDataProvider createTempMrsImageDataProvider(Properties providerProperties) throws IOException
{
  return createMrsImageDataProvider(HadoopUtils.createRandomString(40), providerProperties);
}

/**
 * This will create an interface to the class that will create
 * the input and output classes for this data provider instance.
 *
 * This happens in the context of Mappers and Reducers!!!!
 *
 * @param input is the name of the resource inside accumulo.  This is
 * usually a table name.
 * @return A new interface to an Accumulo table with map rasters.
 */
@Override
public MrsImageDataProvider createMrsImageDataProvider(String input,
    final Configuration conf)
{
  // set the table name
  table = input;

  // see if the prefix needs to be stripped away
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    table = table.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }

  // return a new interface to the accumulo data instance
  return new AccumuloMrsImageDataProvider(conf, queryProps, table);

} // end createMrsImageDataProvider


/**
 * This is in the context of setting up a job or WMS/WTS queries.
 */
@Override
public MrsImageDataProvider createMrsImageDataProvider(String input,
    final Properties providerProperties)
{
  // set the table name
  table = input;

  // see if the prefix needs to be stripped away
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    table = table.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
  if(queryProps == null){
    queryProps = new Properties();
  }
  if(providerProperties != null){
    queryProps.putAll(providerProperties);
  }
  // return a new interface to the accumulo data instance
  return new AccumuloMrsImageDataProvider(queryProps, table);

} // end createMrsImageDataProvider


/**
 * Determine if there is a table that exists that is a geospatial table.
 * @param input the name of the table to see if it exists and is a geospatial table.
 * @return positive if there exists a geospatial tables.
 */
@Override
public boolean canOpen(String input, final Configuration conf)
{
  //TODO: work through the idea of empty and image tables

  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    //TODO: check if there is an existing table
    return true;
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
  //queryProps = AccumuloConnector.getAccumuloProperties();

  // TODO: look at authorizations coming in from the conf!!!
  ADPF_AllTables = AccumuloUtils.getListOfTables(null);
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(null);

  log.info("Asked to open: " + input + " ("+t+")");


  // TODO make sure we can work with this correctly - geotable vs just a table
  return ADPF_AllTables.contains(t);
  //return ADPF_ImageToTable.contains(t);

} // end canOpen

@Override
public boolean canOpen(String input, final Properties providerProperties)
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
  //queryProps = AccumuloConnector.getAccumuloProperties();

  ADPF_AllTables = AccumuloUtils.getListOfTables(providerProperties);
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(providerProperties);


  log.info("Asked to open: " + input + " ("+t+")");


  return ADPF_AllTables.contains(t);

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
public String[] listImages(final Properties providerProperties) throws IOException
{
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(providerProperties);

  ArrayList<String> keys = new ArrayList<String>();
  for(String k : ADPF_ImageToTable.keySet()){
    keys.add(k);
  }
  Collections.sort(keys);

  return (String[]) keys.toArray(new String[keys.size()]);
} // listImages


/**
 * This is the determination of a feasible output for this data provider.
 * @param input is the name to see if there is a corresponding table
 * @return if the table exists and can be written
 * @throws IOException because there is a problem connection to the Accumulo instance
 */
@Override
public boolean canWrite(String input, final Configuration conf) throws IOException
{
  String t = input;
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    t = input.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
  ADPF_AllTables = AccumuloUtils.getListOfTables(null);
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(null);

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
public boolean canWrite(String input, final Properties providerProperties) throws IOException
{
  String t = input;
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    t = input.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
  ADPF_AllTables = AccumuloUtils.getListOfTables(providerProperties);
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(providerProperties);

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


/**
 * This method returns true if there exists a table with a given name.
 * The table in question does not have to be a geospatial table.  It can
 * just exist in Accumulo.
 * @input input is the table name in question.
 * @return true if there exists a table in Accumulo.
 * @throws IOException if there is a problem connection to Accumulo.
 */
@Override
public boolean exists(String input, final Configuration conf) throws IOException
{

    /*
     * since there is no way to determine if things have changed inside Accumulo
     * there needs to be a refresh of the information of what is there
     */
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(null);
  ADPF_AllTables = AccumuloUtils.getListOfTables(null);
  // clean off the prefix if needed
  String t = input;
  if(input.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
    t = input.replaceFirst(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
//    if(t.contains("-")){
//    	String[] els = input.split("-");
//    	t = els[0];
//    }

  // prepare some information for logging
  StringBuffer sb = new StringBuffer();
  for(String k : ADPF_ImageToTable.keySet()){
    if(sb.length() != 0){
      sb.append(" ");
    }
    sb.append(k);
  }

  log.info("search for '" + input + " -> " + t + "' was " + Boolean.toString(ADPF_ImageToTable.containsKey(t)) + " : " + sb.toString());

  // does the table exist
  return ADPF_ImageToTable.containsKey(t);
} // end exists


@Override
public boolean exists(String input, final Properties providerProperties) throws IOException
{

    /*
     * since there is no way to determine if things have changed inside Accumulo
     * there needs to be a refresh of the information of what is there
     */
  ADPF_ImageToTable = AccumuloUtils.getGeoTables(providerProperties);
  if(ADPF_AllTables == null){
    ADPF_AllTables = AccumuloUtils.getListOfTables(providerProperties);
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

  // prepare some information for logging
  StringBuffer sb = new StringBuffer();
  for(String k : ADPF_ImageToTable.keySet()){
    if(sb.length() != 0){
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
} // end exists


/**
 * This does not do anything.  The ability to delete an image is not implemented.
 */
@Override
public void delete(String name, final Configuration conf) throws IOException
{
  log.info("asked to delete " + name + " -- doing nothing.");
  AccumuloConnector.deleteTable(name);
  return;
} // end delete


@Override
public void delete(String name, final Properties providerProperties) throws IOException
{
  AccumuloConnector.deleteTable(name);
  return;
} // end delete


} // end AccumuloMrsImageDataProviderFactory
