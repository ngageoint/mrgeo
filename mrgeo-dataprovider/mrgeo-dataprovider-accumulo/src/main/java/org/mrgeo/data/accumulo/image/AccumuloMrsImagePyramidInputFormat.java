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

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;


/**
 * AccumuloMrsImagePyramidInputFormat handles Accumulo as an input format to map reduce jobs.
 */
public class AccumuloMrsImagePyramidInputFormat extends InputFormatBase<TileIdWritable, RasterWritable>
{

  // table to use as input
  private String input;
  
  // the zoom level to be used
  private int inputZoom;

  // logger for the class
  private static final Logger log = LoggerFactory.getLogger(AccumuloMrsImagePyramidInputFormat.class);
  

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
    TiledInputFormatContext tifc = TiledInputFormatContext.load(conf);
    
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
      RangeInputSplit ris = (RangeInputSplit)is;

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
      
      log.info("\tSplit starting at " + sl + " and ending at " + el);
      
    }
    
    return retList;
    
  } // end getSplits

//  private ArrayList<RangeInputSplit> smoothSplits(List<InputSplit> inputSplits){
//    ArrayList<RangeInputSplit> retList = new ArrayList<RangeInputSplit>();
//
//    for(InputSplit is : inputSplits){
//      RangeInputSplit ris = (RangeInputSplit)is;
//      Range r = ris.getRange();
//      Range r2;
//      Key sk = r.getStartKey();
//      Key ek = r.getEndKey();
//      Key sk2 = null;
//      Key ek2 = null;
//
//      long sl = 0;
//      long el = Long.MAX_VALUE >> 8;
//      if(sk != null){
//        Text sr = sk.getRow();
//        //sl = AccumuloUtils.toLong(sr);        
//        sl = AccumuloUtils.toLong(sr.getBytes());
//        sk2 = new Key(AccumuloUtils.toText(sl), sk.getColumnFamily(), sk.getColumnQualifier());
//      }
//      if(ek != null){
//        Text er = ek.getRow();
//        
//        //el = AccumuloUtils.toLong(er);
//        el = AccumuloUtils.toLong(er.getBytes());
//        ek2 = new Key(AccumuloUtils.toText(el), ek.getColumnFamily(), ek.getColumnQualifier());
//        
//        
//      }
//      r2 = new Range(sk2, ek2);
//      ris.setRange(r2);
//      
//    }
//    
//    return retList;
//  } // end smoothSplits
  
  
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
    
    /**
     * This RecordReaderBase takes apart the key and produces the TileIDWritable.
     * It also prepares the RasterWritable for the value.
     */
    return new RecordReaderBase<TileIdWritable, RasterWritable>() {
      @Override

      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Entry<Key,Value> entry = scannerIterator.next();
          // transform key and value
          long id = AccumuloUtils.toLong(entry.getKey().getRow());
          currentKey = entry.getKey();
          currentValue = entry.getValue();

          currentK = new TileIdWritable(id);
          currentV = new RasterWritable(entry.getValue().get());
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    }; //end RecordReaderBase
    
  } // end RecordReader
  
  
  
  public static RecordReader<TileIdWritable, RasterWritable> makeRecordReader(){
    return new RecordReaderBase<TileIdWritable, RasterWritable>() {
      
      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        
        RangeInputSplit ris = (RangeInputSplit) ((TiledInputSplit)inSplit).getWrappedSplit();

        log.info("initializing with instance of " + ris.getInstanceName());
        log.info("initializing with auths of " + ris.getAuths().toString());
        
        super.initialize(((TiledInputSplit)inSplit).getWrappedSplit(), attempt);
        
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
          currentValue = entry.getValue();

          currentK = new TileIdWritable(id);
          currentV = new RasterWritable(entry.getValue().get());
          
          //log.info("current key = " + id);
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    }; //end RecordReaderBase
  }
  
  
} // end AccumuloMrsImagePyramidRecordReader
