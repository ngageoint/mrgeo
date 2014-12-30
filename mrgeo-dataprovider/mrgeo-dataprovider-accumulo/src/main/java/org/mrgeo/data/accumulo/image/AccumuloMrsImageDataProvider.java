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

package org.mrgeo.data.accumulo.image;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.accumulo.input.image.AccumuloMrsImagePyramidInputFormatProvider;
import org.mrgeo.data.accumulo.metadata.AccumuloMrsImagePyramidMetadataReader;
import org.mrgeo.data.accumulo.metadata.AccumuloMrsImagePyramidMetadataWriter;
import org.mrgeo.data.accumulo.output.image.AccumuloMrsImagePyramidOutputFormatProvider;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.*;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Properties;


/**
 * This is the class that provides the input and output formats for
 * storing and retrieving data from Accumulo.  Under this implementation,
 * the indexing scheme implemented is the TMS tiling scheme.  This is the
 * same indexing being done by the HDFS implementation.  There is one
 * addition to the scheme for this implementation.  The key used is:<br>
 * <br>
 * rowID: [bytes of long which is tile id]<br>
 * --cf: [zoom level of tile]<br>
 * ----cq: [string value of tile id]<br>
 * ------vis: [what is passed]<br>
 * --------value: [bytes of raster]<br>
 * 
 */
public class AccumuloMrsImageDataProvider extends MrsImageDataProvider
{

  // the classes that are used for metadata
  private AccumuloMrsImagePyramidMetadataReader metaReader = null;
  private MrsImagePyramidMetadataWriter metaWriter = null;

  // table we are connecting to for data
  private String table;

  // The following should store the original resource name and any Accumulo
  // property settings required to access that resource. Internally, this
  // data provider should make use of the resolved resource name. It should
  // never use this property directly, but instead call 
  private String resolvedResourceName;

  // output controller
  private AccumuloMrsImagePyramidOutputFormatProvider outFormatProvider = null;

  // logging
  private static Logger log = LoggerFactory.getLogger(AccumuloMrsImageDataProvider.class);
  
  private Properties queryProps;

  private Configuration conf;
  private ColumnVisibility cv;
  private String pl; // protection level
  
  /**
   * Base constructor for the provider.
   * 
   * @param resourceName - the table being utilized.  The input may be encoded or may not be.
   * The encoded resource name has all the information needed to connect to Accumulo.
   */
  public AccumuloMrsImageDataProvider(final String resourceName)
  {
    super();
    
    // Determine if the resourceName is resolved or not. If it is, then call
    // setResourceName. If not, then we'll need to resolve it on demand - see
    // getResolvedResourceName().
    boolean nameIsResolved = resourceName.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX);

    if (nameIsResolved){
      resolvedResourceName = resourceName;
    } else {
      setResourceName(resourceName);
    }
    if(queryProps == null){
      queryProps = new Properties();
    }

    // TODO: get this resolved to get the right authorizations put into a query
    queryProps.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS, MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS);

  } // end constructor

  
  // this is used for WMS/WTS queries
  public AccumuloMrsImageDataProvider(Properties props, String resourceName) {
	  super();
	  
	  boolean nameIsResolved = resourceName.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX);

	  if (nameIsResolved) {
		  resolvedResourceName = resourceName;
	  } else {
		  setResourceName(resourceName);
	  }

	  // it is possible that we are being called for the first time
	  if (queryProps == null) {
		  queryProps = new Properties();
	  }
	  
	  // make sure this is correct
	  String auths = MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS;
	  if(props != null && props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES) != null){
		  auths = props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
	  }
	  queryProps.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS, auths);

	  //cv = new ColumnVisibility();
	  
	  if(props != null) {
		  queryProps.putAll(props);
	  } else {
		  // ???????
		  queryProps.putAll(AccumuloConnector.getAccumuloProperties());
	  }
	  
  } // end constructor
  

  /**
   * This is assumed to be used within mappers and reducers
   * 
   * @param conf
   * @param props
   * @param resourceName
   */
  public AccumuloMrsImageDataProvider(Configuration conf, Properties props, String resourceName){
    super();
    
    this.conf = conf;
    
    boolean nameIsResolved = resourceName.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_ENCODED_PREFIX);

    if (nameIsResolved){
      resolvedResourceName = resourceName;
    } else {
      setResourceName(resourceName);
    }
    
    // it is possible that we are being called for the first time
    if(queryProps == null){
      queryProps = new Properties();
    }
    
    // make sure this is correct
    String auths; // = props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
    if(props == null){
    	auths = MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS;
    } else {
    	auths = props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
    }
    
    if(auths != null){
    	queryProps.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS, auths);
    } else {
    	queryProps.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS,
    			MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS);
    }
    
    if(props != null){
      queryProps.putAll(props);
    } else {
      queryProps.putAll(AccumuloConnector.getAccumuloProperties());
    }
    
    // get the protection level
    pl = conf.get("protectionLevel");
    if(pl == null){
    	if(conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ) != null){
    		log.info("Ingest will use protection of: " + conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));
    		cv = new ColumnVisibility(conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));
    		pl = new String(cv.getExpression());
    	} else {
    		log.info("Ingest has no protection level set.");
    	
    		cv = new ColumnVisibility();
    		pl = "";
    	}
    } else {
    	cv = new ColumnVisibility(pl);
    	conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ, pl);
    }
    log.info("protection level is: " + pl);
    
  } // end constructor
  
  /**
   * If the resourceName is null, then this method should extract/compute it
   * from the resolvedResourceName and return it.
   * 
   * @return The name of the resource being utilized.
   */
  @Override
  public String getResourceName()
  {
    String result = super.getResourceName();
    if (result == null){

      // decode the properties
      Properties props = AccumuloConnector.decodeAccumuloProperties(resolvedResourceName);

      // get the resource
      result = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE);
      
      setResourceName(result);
    }

    return result;
  } // end getResourceName
  

  /**
   * If the resolvedResourceName is null, then it computes it based on the
   * resourceName and the Accumulo configuration properties. The
   * resolvedResourceName consists of the resourceName and the properties encoded
   * together into a single String which is only used internally to this
   * plugin.
   * 
   * @return The encoded elements used to connect to Accumulo.
   */
  public String getResolvedName(){
    if (resolvedResourceName == null){
      resolvedResourceName = AccumuloConnector.encodeAccumuloProperties(this.getResourceName());
    }
    return resolvedResourceName;
  } // end getResolvedName

  
  /**
   * Used for retrieving the name of the table in use.
   * @return The name of table being used.
   */
  public String getTable(){
    if(table == null){
      table = this.getResourceName();
    }
    return table;
  } // end getTable
  
  
  /**
   * This will return the class that handles the input classes for a map reduce job.
   * 
   * @param context - is the image context for the input for the job.
   * 
   * @return An instance of AccumuloMrsImagePyramidInputFormatProvider is returned.
   */
  @Override
  public MrsImageInputFormatProvider getTiledInputFormatProvider(TiledInputFormatContext context)
  {
    return new AccumuloMrsImagePyramidInputFormatProvider(context);
  } // end getTiledInputFormatProvider

  
  /**
   * This will return the class that handles the classes for output for a map reduce job.
   * 
   * @param context - is the image context for the output of the job.
   * 
   * @return An instance of AccumuloMrsImagePyramidOutputFormatProvider.
   */
  @Override
  public MrsImageOutputFormatProvider getTiledOutputFormatProvider(TiledOutputFormatContext context)
  {
    return new AccumuloMrsImagePyramidOutputFormatProvider(this, context, cv);
  } // end getTiledOutputFormatProvider


  /**
   * Delete is not implemented.  Administration of what is in Accumulo
   * is not handled in this code base.  It is left to the Data Administrator
   * to delete data.
   */
  @Override
  public void delete()
  {
    // TODO: Need to implement
    log.info("Asked to delete the resource " + table + ".  Not deleting anything!!!");
    //throw new NotImplementedException();
  } // end delete

  
  /**
   * Delete is not implemented.  Administration of what is in Accumulo
   * is not handled in this code base.  It is left to the Data Administrator
   * to delete data.
   * 
   * @param zoomLevle - the zoom level to delete.
   */
  @Override
  public void delete(int zoomLevel)
  {
    // TODO: Need to implement
    log.info("Asked to delete " + zoomLevel + ".  Not deleting anything!!!");
    //throw new NotImplementedException();
  } // end delete

  
  /**
   * Move is not implemented.  dministration of what is in Accumulo
   * is not handled in this code base.  It is left to the Data Administrator
   * to move data.
   * 
   * @param toResource - the destination for the move.
   */
  @Override
  public void move(final String toResource)
  {
    // TODO: Need to implement
    throw new NotImplementedException();
  } // end move
  

  @Override
  public boolean validateProtectionLevel(final String protectionLevel)
  {
    return AccumuloUtils.validateProtectionLevel(protectionLevel);
  }

  /**
   * The class that provides reading metadata from the Accumulo table is returned.
   * @param context - is the context of what is to be read.
   * @return An instance of AccumuloMrsImagePyramidMetadataReader that is able to read from Accumulo.
   */
  @Override
  public MrsImagePyramidMetadataReader getMetadataReader(
      MrsImagePyramidMetadataReaderContext context){

    //TODO: get Authorizations for reading from the context
    
    // check if the metadata reader has been created
    if(metaReader == null){
      metaReader = new AccumuloMrsImagePyramidMetadataReader(this, context);
    }
    
    return metaReader;
  } // end getMetadataReader

  
  /**
   * This class will return the class that write the metadata information to
   * the Accumulo table.
   * @param context - is the context of what is to be written.
   * @return An instance of AccumuloMrsImagePyramidMetadataWriter that is able to write to Accumulo.
   */
  @Override
  public MrsImagePyramidMetadataWriter getMetadataWriter(
      MrsImagePyramidMetadataWriterContext context){
    
    // check to see if the metadata writer exists already
    if(metaWriter == null){
      if(outFormatProvider != null && outFormatProvider.bulkJob()){
        //metaWriter = new AccumuloMrsImagePyramidMetadataFileWriter(outFormatProvider.getWorkDir(), this, context);

        //TODO: think about working with file output - big jobs may need to work that way
        metaWriter = new AccumuloMrsImagePyramidMetadataWriter(this, context);
        
      } else {
      
        metaWriter = new AccumuloMrsImagePyramidMetadataWriter(this, context);

      }
    }
    
    return metaWriter;
  } // end getMetadataWriter

  
  /**
   * This is the method to get a tile reader for Accumulo.
   * @param context - is the context for reading tiles.
   * @return An instance of AccumuloMrsImageReader is returned.
   * @throws IOException if there is a problem connecting to or reading from Accumulo.
   */
  @Override
  public MrsTileReader<Raster> getMrsTileReader(MrsImagePyramidReaderContext context)
      throws IOException
  {
    return new AccumuloMrsImageReader(queryProps, this, context);
  } // end getMrsTileReader

  
  /**
   * This is the method to get a tile write for Accumulo.
   * @param context - is the context for writing tiles.
   * @return An instance of AccumuloMrsImageWriter.
   */
  @Override
  public MrsTileWriter<Raster> getMrsTileWriter(MrsImagePyramidWriterContext context) throws IOException
  {
    // TODO Auto-generated method stub
	  String pltmp = getMetadataReader().read().getProtectionLevel();
	  return new AccumuloMrsImageWriter(this, context, pltmp);
	  
  } // end getMrsTileWriter
  

  /**
   * This will instantiate a record reader that can be used in a map reduce job.
   * @return An instance of a RecordReader that can pull from Accumulo and prepare the correct keys and values.
   */
  @Override
  public RecordReader<TileIdWritable, RasterWritable> getRecordReader()
  {
    // TODO Auto-generated method stub
    log.info("trying to load record reader.");
    
    return AccumuloMrsImagePyramidInputFormat.makeRecordReader();
  } // end getRecordReader

  
  /**
   * This is not implemented at this time.
   */
  @Override
  public RecordWriter<TileIdWritable, RasterWritable> getRecordWriter()
  {
    // TODO Auto-generated method stub
    log.info("failing to load record writer.");
    return null;
  } // end getRecordWriter

  
  /**
   * 
   * @return the column visibility of this instance
   */
  public ColumnVisibility getColumnVisibility(){

	  if(cv != null){
		  return cv;
	  }
	  
	  // set the cv if needed
	  if(pl != null){
		  cv = new ColumnVisibility(pl);
	  } else {
		  cv = new ColumnVisibility();
	  }

	  return cv;
  } // end getColumnVisibility
  
  public Properties getQueryProperties(){
	  return queryProps;
  }
  
  
} // end AccumuloMrsImageDataProvider
