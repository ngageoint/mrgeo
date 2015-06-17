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

package org.mrgeo.data.accumulo.output.image;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.accumulo.image.AccumuloMrsImageDataProvider;
import org.mrgeo.data.accumulo.partitioners.AccumuloMrGeoRangePartitioner;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataWriter;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledOutputFormatContext;
import org.mrgeo.hadoop.multipleoutputs.DirectoryMultipleOutputs;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;

public class AccumuloMrsImagePyramidOutputFormatProvider extends MrsImageOutputFormatProvider
{
  final MrsImageDataProvider provider;
  final static Logger log = LoggerFactory.getLogger(AccumuloMrsImagePyramidOutputFormatProvider.class);
  
  private int zoomLevel = -1;
  private int tileSize = -1;
  private Bounds bounds = null;
  
  private TileBounds tileBounds = null;
  private String table = null;
  
  private long bulkThreshold = Long.MAX_VALUE;
  private long tileCount = -1;
  
  private boolean doBulk = false;
  private boolean forceBulk = true;

  private Properties props;
  
  private ColumnVisibility cv = null;
  //private TiledOutputFormatContext context;
  
  /*
   *  it is assumed that output for bulk ingest will be of the form
   *  tld_workdir/classname/outputtable/timestamp/
   */
  private String workDir = null;
  
  
  public AccumuloMrsImagePyramidOutputFormatProvider(final AccumuloMrsImageDataProvider provider,
                                                     final TiledOutputFormatContext context,
                                                     final ColumnVisibility cv){
    
    super(context);
    
    this.provider = provider;
    this.cv = cv; //provider.getColumnVisibility();
    
    //TODO - program things to get rid of this
    if(this.cv == null){
    	this.cv = new ColumnVisibility();
    }
    log.info("column visibility of: " + this.cv.toString());
    
    this.zoomLevel = context.getZoomlevel();
    this.tileSize = context.getTilesize();

    // get the tile bounds
    this.bounds = context.getBounds();
    this.tileBounds = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(this.bounds), this.zoomLevel, this.tileSize);
    
    this.table = context.getOutput();
    if(table.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX)){
      table = table.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
    }
    log.info("Accumulo working with output table of: " + this.table);

    // figure out the size of output
    tileCount = (this.tileBounds.e - this.tileBounds.w + 1) * (this.tileBounds.n - this.tileBounds.s + 1);
    
    props = AccumuloConnector.getAccumuloProperties();
    
    if(props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_BULK_THRESHOLD)){
      bulkThreshold = Long.parseLong(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_BULK_THRESHOLD));
    }
    if(props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_FORCE_BULK)){
      if(Boolean.parseBoolean(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_FORCE_BULK))){
        log.info("Forcing bulk ingest!");
        forceBulk = true;
        doBulk = true;
      }
    }
    
    log.info("working with output tile count of " + tileCount +
        " (" + (this.tileBounds.e - this.tileBounds.w + 1) + "x" +
        (this.tileBounds.n - this.tileBounds.s + 1) + ") threshold at " + bulkThreshold);
    
    if(tileCount > bulkThreshold){
      // doing bulk ingest
      log.info("Doing Bulk ingest");
      doBulk = true;
    }
    
  } // end constructor

  @Override
  public OutputFormat getOutputFormat()
  {
    // TODO Auto-generated method stub

    if(doBulk || forceBulk){
      log.info("file output format being used at zoom level = " + zoomLevel);
      return new AccumuloMrsImagePyramidFileOutputFormat(zoomLevel, cv);
      //return new AccumuloMrsImagePyramidFileOutputFormat();
    } else {
      log.info("accumulo going direct for output at zoom level = " + zoomLevel);
      
      return new AccumuloMrsImagePyramidOutputFormat(zoomLevel, cv);
    }
  } // end getOutputFormat

  public boolean bulkJob(){
    //return false;
    return doBulk;
  }
  
//  public String getWorkDir(){
//    return workDir + "files" + File.separator;    
//    
//  } // end getWorkDir
  
  
  @Override
  public MrsImagePyramidMetadataWriter getMetadataWriter()
  {
    return provider.getMetadataWriter();
  }

  @Override
  public MrsImageDataProvider getImageProvider()
  {
    return provider;
  }

  @Override
  public void setupJob(final Job job) throws DataProviderException{
    try{
      //TODO: there is an assumption here that the output is going to accumulo directly - not bulk
      super.setupJob(job);
      
      job.getConfiguration().addResource(AccumuloConnector.getAccumuloPropertiesLocation());
      
      
      // zoom level - output zoom level
      zoomLevel = context.getZoomlevel();
//      zoomLevel = job.getConfiguration().getInt("zoomlevel", 0);
      if(zoomLevel != 0){
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOMLEVEL, Integer.toString(zoomLevel));
      }

      
      
      if(context.isMultipleOutputFormat()){
        // get ready for multiple outputs!!!
        
        DirectoryMultipleOutputs.addNamedOutput(job, context.getName(),
            new Path("accumulo-"+context.getName()),
            AccumuloMrsImagePyramidOutputFormat.class,
            TileIdWritable.class,
            RasterWritable.class);
        
      }
      
      //job.getConfiguration().set("zoomLevel", Integer.toString(zoomLevel));
      if(doBulk){
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_JOBTYPE,
                                   MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_BULK);
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PREFIX + Integer.toString(zoomLevel),
            MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_BULK);
      } else {
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_JOBTYPE,
                                   MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_DIRECT);
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PREFIX + Integer.toString(zoomLevel),
            MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_DIRECT);
        
      }
      Properties props = AccumuloConnector.getAccumuloProperties();
      if(props != null){
        String n = context.getName();
        if(n == null){
          n = "";
        }

        String enc = AccumuloConnector.encodeAccumuloProperties(n);
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE, enc);
        
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE,
            props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE));
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS,
            props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS));

        if(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE) == null){
          job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE, this.table);
        } else {
          job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE,
              props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE));
        }
        
        // username and password
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER,
            props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER));

        // make sure the password is set with Base64Encoding
        String pw = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD);
        String isEnc = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64, "false");
        
        if(isEnc.equalsIgnoreCase("true")){
          job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
              props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
        } else {
          byte[] p = Base64.encodeBase64(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD).getBytes());

          job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
              new String(p));
          job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64,
              new String("true"));
        }
        
        if(job.getConfiguration().get(MrGeoConstants.MRGEO_PROTECTION_LEVEL) != null){
        	cv = new ColumnVisibility(job.getConfiguration().get(MrGeoConstants.MRGEO_PROTECTION_LEVEL));
        }
        if(cv == null){

        	if(props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ)){
          	
        		job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ,
        				props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));

          		cv = new ColumnVisibility(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));

        	}
          	
        } else {
          	job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ, new String(cv.getExpression()));
        }
        
      }

      if(doBulk){
        
        LongRectangle outTileBounds = tileBounds.toLongRectangle();
        
        // setup the output for the job
        if(props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR)){
          workDir = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR);
          if(workDir != null){
            workDir += File.separator;
          }
        } else {
          workDir = "";
        }
        workDir += AccumuloMrsImagePyramidFileOutputFormat.class.getSimpleName() +
            File.separator +
            this.table +
            File.separator;// +
//            System.currentTimeMillis() +
//            File.separator;
        
        // delete the work dir if possible
//        Path wd = new Path(workDir);
//        FileSystem fs = HadoopFileUtils.getFileSystem(wd);        
//        if (fs.exists(wd))
//        {
//          fs.delete(wd, false);
//        }
        
        job.getConfiguration().set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR, workDir);
        
     // determine the starting points for the splits
        ArrayList<Pair<Long, Long>> splitPoints = new ArrayList<Pair<Long, Long>>();
        
        // think about the multiple levels and creating other splits!!!
        
        long step = bulkThreshold / outTileBounds.getWidth();
        long rem = bulkThreshold % outTileBounds.getWidth();
        if(rem > 0){
          step++;
        }
        for(long y = outTileBounds.getMinY(); y <= outTileBounds.getMaxY(); y += step){
          Pair<Long, Long> cur = new Pair<Long, Long>(outTileBounds.getMinX(), y);
          splitPoints.add(cur);
        }
        
        // we now have our list of split points
        // now build the splits file!!!
        FileSystem fs = null;
        //FileSystem.get(job.getConfiguration());
        PrintStream out = null;

        try{
          Path wd = new Path(workDir);
          fs = FileSystem.get(job.getConfiguration());
          if(fs.exists(wd)){
            fs.delete(wd, true);
          }
          
          out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDir + "splits.txt"))));
          
          for(Pair<Long, Long> p : splitPoints){
            long split = TMSUtils.tileid(p.getFirst(), p.getSecond(), zoomLevel);
            //TileIdWritable t = new TileIdWritable(split);
            Text t = new Text(longToBytes(split));
            out.println(new String(Base64.encodeBase64(TextUtil.getBytes(t))));
            log.debug("Point: " + p.getFirst() + "\t" + p.getSecond() + "\t" + split + "\t" + t.getLength());
          }

          job.setNumReduceTasks(splitPoints.size() + 1);
          out.close();

          job.setPartitionerClass(AccumuloMrGeoRangePartitioner.class);
          AccumuloMrGeoRangePartitioner.setSplitFile(job, workDir + "splits.txt");
          
        
        } catch(IOException ioe){
          ioe.printStackTrace();
          throw new DataProviderException("Problem creating output splits.txt for bulk ingest directory.");
        }
        
        job.setOutputFormatClass(AccumuloMrsImagePyramidFileOutputFormat.class);
        
        AccumuloMrsImagePyramidFileOutputFormat.setOutputPath(job, new Path(workDir + "files"));
        //AccumuloMrsImagePyramidFileOutputFormat.setZoomLevel(zoomLevel);
        
      } else {
        
    	  log.info("Setting the output format of: " +
    			  	AccumuloMrsImagePyramidOutputFormat.class.getCanonicalName());
    	
    	  job.setOutputFormatClass(AccumuloMrsImagePyramidOutputFormat.class);
    	  AccumuloMrsImagePyramidOutputFormat.setJob(job);

    	  log.info("Setting zoom level to " + zoomLevel);
    	  log.info("Visibility is " + cv.toString());
    	  log.info("Setting the number of reducers to " + MrGeoAccumuloConstants.MRGEO_DEFAULT_NUM_REDUCERS);
    	  job.setNumReduceTasks(MrGeoAccumuloConstants.MRGEO_DEFAULT_NUM_REDUCERS);
      }
      
      job.setOutputKeyClass(TileIdWritable.class);
      job.setOutputValueClass(RasterWritable.class);
      
    } catch(IOException ioe){
      throw new DataProviderException("Error running job setup", ioe);
    }
    
  } // end setupJob
  
  
  @Override
  public void teardown(Job job) throws DataProviderException
  {
    String myJobType = job.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PREFIX + Integer.toString(zoomLevel));
    
    
    
    // TODO Auto-generated method stub
    if(myJobType.equals(MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_BULK)){
      // do bulk ingest now
      Connector conn = AccumuloConnector.getConnector();
      FileSystem fs = null;
      PrintStream out = null;

      if(workDir == null){
        workDir = job.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR);
      }
      
      try{
        log.info("Bulk ingest starting from working directory of " + workDir);
        fs = FileSystem.get(job.getConfiguration());

        
        Path working = new Path(workDir + File.separator, MrGeoAccumuloConstants.MRGEO_ACC_FILE_NAME_BULK_WORKING);
        Path completed = new Path(workDir + File.separator, MrGeoAccumuloConstants.MRGEO_ACC_FILE_NAME_BULK_DONE);
        if(fs.exists(working) || fs.exists(completed)){
          log.info("Bulk ingest completed already.");
          return;
        } else {
          FSDataOutputStream fout = fs.create(working);
          fout.write(("zoom level = " + Integer.toString(zoomLevel) + "\n").getBytes());
          fout.close();
        }
        
        
        // at this point - there should be something to bulk ingest
        
        // find the _SUCCESS file
//        Path success = new Path(workDir + "files" + File.separator, "_SUCCESS");
//        if(! fs.exists(success)){
//          // failure in the job
//          throw new DataProviderException("Hadoop job did not finish correctly.");
//        } else {
//          fs.delete(success, true);
//        }
//        
//        Path logs = new Path(workDir + "files" + File.separator, "_logs");
//        if(! fs.exists(logs)){
//          // failure in the job
//          throw new DataProviderException("Hadoop job did not finish correctly.");
//        } else {
//          fs.delete(logs, true);
//        }
        log.info("Setting work indication file.");
        

        
        // make sure there is a failures directory
        Path failures = new Path(workDir, "failures");  
        fs.delete(failures, true);
        fs.mkdirs(new Path(workDir, "failures"));
        
        if(! conn.tableOperations().exists(table)){
        	conn.tableOperations().create(table, true);
        	HashMap<String, Set<Text>> groups = new HashMap<String, Set<Text>>();
        	for(int i = 1; i <= 18; i++){
        		String k = Integer.toString(i);
        		HashSet<Text> hs = new HashSet<Text>();
        		
        		hs.add(new Text(Integer.toString(i)));
        	}
        	conn.tableOperations().setLocalityGroups(table, groups);
        }
        
        conn.tableOperations().importDirectory(table, workDir + "files", workDir + "failures", true);
        conn.tableOperations().compact(table, new Text("" + 0x00), new Text("" + 0xFF), true, false);
        
        FSDataOutputStream fout = fs.create(completed);
        fout.write(("zoom level = " + Integer.toString(zoomLevel) + "\n").getBytes());
        fout.close();
        fs.delete(working, true);
        
      } catch(Exception e){
        e.printStackTrace();
        throw new DataProviderException("Problem doing bulk ingest.");
      }
      
    }
    
  } // end teardown
  
  
  public byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(x);
    return buffer.array();
  }

  @Override
  public boolean validateProtectionLevel(String protectionLevel)
  {
    return AccumuloUtils.validateProtectionLevel(protectionLevel);
  }

} // end AccumuloMrsImagePyramidOutputFormatProvider
