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

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.image.ImageInputFormatContext;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.RuntimeErrorException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

//import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;


/**
 * AccumuloMrsPyramidInputFormat handles Accumulo as an input format to map reduce jobs.
 */
public class AccumuloMrsPyramidInputFormat extends InputFormatBase<TileIdWritable, RasterWritable>
{

  // table to use as input
  private String input;
  
  // the zoom level to be used
  private int inputZoom;

  // logger for the class
  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidInputFormat.class);
  

  /**
   * getSplits will retrieve all the splits for a job given a zoom level.
   * 
   * @param context - the Job context.
   * @return The list of splits from the table.
   * @throws IOException when there is an issue getting the splits for the job.
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {

    // get the configuration
    Configuration conf = context.getConfiguration();

    // get the input context for the image
    ImageInputFormatContext tifc = ImageInputFormatContext.load(conf);
    
    // get the zoom level of the pyramid
    int zl = tifc.getZoomLevel();

    // get the splits from the super (Accumulo InputFormatBase)
    List<InputSplit> splits = super.getSplits(context);

    // prepare the return list
    List<InputSplit> retList = new ArrayList<InputSplit>(splits.size());
    
    // TODO: make this pull back integer pairs - so there is no need to smooth and go through things again
    
    // make sure all the splits will conform to the splits type expected by the core
    //List<RangeInputSplit>splits2 = smoothSplits(splits);

    // go through all the splits and create the output splits
    for(InputSplit is : splits){

      // an Accumulo split is a RangeInputSplit
   	  org.apache.accumulo.core.client.mapreduce.RangeInputSplit ris =
   			  (org.apache.accumulo.core.client.mapreduce.RangeInputSplit)is;

      // get the range
      Range r = ris.getRange();
      
      log.info("Range: " + r.toString());
      
      // get the start
      Key sk = r.getStartKey();

      // get the end
      Key ek = r.getEndKey();

      // get the tile ids at the start and end of the range
      long sl = 0;
      long el = Long.MAX_VALUE >> 8;

      // check the start of the range - make sure it is a usable value
      if(sk != null){
        Text sr = sk.getRow();
        if(sr.toString().equals(MrGeoAccumuloConstants.MRGEO_ACC_METADATA)){
          continue;
        }
        sl = AccumuloUtils.toLong(sr);        
      }
      
      // check the end of the range - make sure it is a usable value
      if(ek != null){
        Text er = ek.getRow();
        if(er.toString().equals(MrGeoAccumuloConstants.MRGEO_ACC_METADATA)){
          continue;
        }
        el = AccumuloUtils.toLong(er);
      }

      // build the split used by core
      TiledInputSplit tis = new TiledInputSplit(
          is, // input split
          sl, // start tile id
          el, // end tile id
          tifc.getZoomLevel(), // zoom level
          tifc.getTileSize() // tile size
          );
      retList.add(tis);
      
      TMSUtils.Tile tile1 = TMSUtils.tileid(sl, tifc.getZoomLevel());
      TMSUtils.Tile tile2 = TMSUtils.tileid(el, tifc.getZoomLevel());
      
      log.info("\tSplit starting at " + sl + " ("+tile1.tx+","+tile1.ty+")" + " and ending at " + el + " ("+tile2.tx+","+tile2.ty+")");
      
    }
    
    return retList;
    
  } // end getSplits

  
  
  /**
   * createRecordReader will create a RecordReader that will be used in a map reduce job.  This
   * will transform the key from Accumulo to a TileIdWritable.
   * @param split - the input split to utilize for reading.
   * @param context - the TaskAttemptContext for this part of input from Accumulo.
   * @return a valid RecordReader
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<TileIdWritable, RasterWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    
//    // need to get authorizations
//    String authStr = context.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
//    Authorizations auths = new Authorizations();
//    if(authStr != null){
//      auths = new Authorizations(authStr.split(","));
//    }
	  return makeRecordReader();
    /**
     * This RecordReaderBase takes apart the key and produces the TileIDWritable.
     * It also prepares the RasterWritable for the value.
     */
//    return new RecordReaderBase<TileIdWritable, RasterWritable>() {
//
//    	@Override
//    	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
//    	  {
//    	    if (split instanceof TiledInputSplit)
//    	    {
//    	      super.initialize(((TiledInputSplit)split).getWrappedSplit(), context);
//    	    }
//    	    else
//    	    {
//    	      // Should never happen
//    	      super.initialize(split, context);
//    	    }
//    	  } 
//    	
//    	
//    	@Override
//      public boolean nextKeyValue() throws IOException, InterruptedException {
//        if (scannerIterator.hasNext()) {
//          ++numKeysRead;
//          Entry<Key,Value> entry = scannerIterator.next();
//          // transform key and value
//          long id = AccumuloUtils.toLong(entry.getKey().getRow());
//          currentKey = entry.getKey();
//          currentValue = entry.getValue();
//
//          currentK = new TileIdWritable(id);
//          currentV = new RasterWritable(entry.getValue().get());
//          if (log.isTraceEnabled())
//            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
//          return true;
//        }
//        return false;
//      }
//    }; //end RecordReaderBase
    
  } // end RecordReader
  
  
  
  public static RecordReader<TileIdWritable, RasterWritable> makeRecordReader(){
    return new RecordReaderBase<TileIdWritable, RasterWritable>() {
      
      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        
//        RangeInputSplit ris = (RangeInputSplit) ((TiledInputSplit)inSplit).getWrappedSplit();
//
//        log.info("initializing with instance of " + ris.getInstanceName());
//        log.info("initializing with auths of " + ris.getAuths().toString());
//        
//        super.initialize(((TiledInputSplit)inSplit).getWrappedSplit(), attempt);

    	  log.info("initializing input splits of type " + inSplit.getClass().getCanonicalName());
    	  String[] locs;
    	  try{
    		  locs = inSplit.getLocations();
    		  for(int x = 0; x < locs.length; x++){
    			  log.info("location " + x + " -> " + locs[x]);
    		  }
    	  } catch(InterruptedException ie){
    		  ie.printStackTrace();
    		  return;
    	  }
        if(inSplit instanceof TiledInputSplit){
        	
        	// deal with this
        	org.apache.accumulo.core.client.mapreduce.RangeInputSplit ris =
        			new org.apache.accumulo.core.client.mapreduce.RangeInputSplit();
        	InputSplit inS = ((TiledInputSplit)inSplit).getWrappedSplit();
        	log.info("input split class: " + inS.getClass().getCanonicalName());
        	long startId = ((TiledInputSplit) inSplit).getStartTileId();
        	long endId = ((TiledInputSplit) inSplit).getEndTileId();
        	Key startKey = AccumuloUtils.toKey(startId);
        	Key endKey = AccumuloUtils.toKey(endId);
        	int zoomL = ((TiledInputSplit)inSplit).getZoomLevel();
        	Range r = new Range(startKey, endKey);

        	
        	log.info("Zoom Level = " + zoomL);
        	log.info("Range " + startId + " to " + endId);
        	
        	try{
        		locs = inS.getLocations();
        		for(int x = 0; x < locs.length; x++){
        			log.info("split " + x + " -> " + locs[x]);
        		}
        		ris.setRange(r);
        		ris.setLocations(locs);
        		ris.setTableName(((org.apache.accumulo.core.client.mapreduce.RangeInputSplit)inS).getTableName());
        		ris.setTableId(((org.apache.accumulo.core.client.mapreduce.RangeInputSplit)inS).getTableId());
        		
        		// there can be more added here
        		
        	} catch(InterruptedException ie){
        		throw new RuntimeErrorException(new Error(ie.getMessage()));
        	}
        	if(ris == null){
        		log.info("range input split is null");
        	} else {
        		log.info("table " + ris.getTableName() + " is offline: " + ris.isOffline());
        	}
        	super.initialize(ris, attempt);

        	//super.initialize(((TiledInputSplit) inSplit).getWrappedSplit(), attempt);
        	
        } else {
        	super.initialize(inSplit, attempt);
        }
        
        
        
        
        
      } // end initialize
      
      @Override
      public void close(){
        log.info("Record Reader closing!");
      }
      
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = scannerIterator.next();
          // transform key and value
          long id = AccumuloUtils.toLong(entry.getKey().getRow());
          currentKey = entry.getKey();
          //currentValue = entry.getValue();
          
          log.info("Processing " + id + " -> " + entry.getValue().getSize());

          currentK = new TileIdWritable(id);
          currentV = new RasterWritable(entry.getValue().get());
          
          //log.info("current key = " + id);
//          if (log.isTraceEnabled())
//            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    }; //end RecordReaderBase
  }
  
  
} // end AccumuloMrsPyramidRecordReader
