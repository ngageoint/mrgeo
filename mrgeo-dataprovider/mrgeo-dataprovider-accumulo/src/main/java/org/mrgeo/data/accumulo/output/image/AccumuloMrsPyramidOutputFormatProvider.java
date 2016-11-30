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

package org.mrgeo.data.accumulo.output.image;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
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
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.rdd.RasterRDD;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.image.ImageOutputFormatContext;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;

public class AccumuloMrsPyramidOutputFormatProvider extends MrsImageOutputFormatProvider
{
final MrsImageDataProvider provider;
final static Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidOutputFormatProvider.class);

private int zoomLevel = -1;
private int tileSize = -1;
private Bounds bounds = null;

private TileBounds tileBounds = null;
private String table = null;

private long bulkThreshold = Long.MAX_VALUE;
private long tileCount = -1;

private boolean doBulk = false;
private boolean forceBulk = false;

private Properties props;

private ColumnVisibility cv = null;
//private ImageOutputFormatContext context;

/*
 *  it is assumed that output for bulk ingest will be of the form
 *  tld_workdir/classname/outputtable/timestamp/
 */
private String workDir = null;


public AccumuloMrsPyramidOutputFormatProvider(final AccumuloMrsImageDataProvider provider,
    final ImageOutputFormatContext context,
    final ColumnVisibility cv)
{

  super(context);

  this.provider = provider;
  this.cv = cv; //provider.getColumnVisibility();

  //TODO - program things to get rid of this
  if (this.cv == null)
  {
    this.cv = new ColumnVisibility();
  }
  log.info("column visibility of: " + this.cv.toString());

  this.zoomLevel = context.getZoomLevel();
  this.tileSize = context.getTileSize();

  // get the tile bounds
  this.bounds = context.getBounds();
  this.tileBounds = TMSUtils.boundsToTile(this.bounds, this.zoomLevel, this.tileSize);

  this.table = context.getOutput();
  if (table.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX))
  {
    table = table.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
  }
  log.info("Accumulo working with output table of: " + this.table);

  // figure out the size of output
  tileCount = (this.tileBounds.e - this.tileBounds.w + 1) * (this.tileBounds.n - this.tileBounds.s + 1);

  try
  {
    props = AccumuloConnector.getAccumuloProperties();

    if (props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_BULK_THRESHOLD))
    {
      bulkThreshold = Long.parseLong(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_BULK_THRESHOLD));
    }
    if (props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_FORCE_BULK))
    {
      if (Boolean.parseBoolean(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_FORCE_BULK)))
      {
        log.info("Forcing bulk ingest!");
        forceBulk = true;
        doBulk = true;
      }
    }

    log.info("working with output tile count of " + tileCount +
        " (" + (this.tileBounds.e - this.tileBounds.w + 1) + "x" +
        (this.tileBounds.n - this.tileBounds.s + 1) + ") threshold at " + bulkThreshold);

    if (tileCount > bulkThreshold)
    {
      // doing bulk ingest
      log.info("Doing Bulk ingest");
      doBulk = true;
    }
  }
  catch (DataProviderException e)
  {
    log.error("Unable to get Accumulo connection properties", e);
  }
//  catch (Exception e)
//  {
//    log.error("Unable to get Accumulo connection properties", e);
//  }

} // end constructor

@Override
public OutputFormat getOutputFormat()
{
  // TODO Auto-generated method stub

  if (doBulk || forceBulk)
  {
    return new AccumuloMrsPyramidFileOutputFormat(zoomLevel, cv);
    //return new AccumuloMrsPyramidFileOutputFormat();
  }
  else
  {

    return new AccumuloMrsPyramidOutputFormat(zoomLevel, cv);
  }
} // end getOutputFormat

@Override
public void save(RasterRDD raster, Configuration conf)
{
  // IMPLEMENT THIS SCALA CODE IN JAVA!
  throw new NotImplementedException("AccumuloMrsPyramidOutputFormatProvider.save not yet implemeted");
//  val sparkPartitioner = tofp.getSparkPartitioner
//  val conf1 = tofp.setupOutput(conf)
//
//  // Repartition the output if the output data provider requires it
//  val wrappedTiles = new OrderedRDDFunctions[TileIdWritable, RasterWritable, (TileIdWritable, RasterWritable)](tiles)
//    val sorted: RasterRDD = RasterRDD(
//  if (sparkPartitioner != null) {
//    wrappedTiles.repartitionAndSortWithinPartitions(sparkPartitioner)
//  }
//  else {
//    wrappedTiles.sortByKey()
//  })
//  //val sorted: RasterRDD = RasterRDD(tiles.sortByKey())
//
//
//  val wrappedForSave = new PairRDDFunctions(sorted)
//  wrappedForSave.saveAsNewAPIHadoopDataset(conf1)
//
////    if (localpersist) {
////      tiles.unpersist()
////    }
//
//  if (sparkPartitioner != null)
//  {
//    sparkPartitioner.writeSplits(sorted, output, zoom, conf1)
//  }
//  tofp.teardownForSpark(conf1)

}

public boolean bulkJob()
{
  //return false;
  return doBulk;
}

//  public String getWorkDir(){
//    return workDir + "files" + File.separator;    
//    
//  } // end getWorkDir


//  private MrsImageDataProvider getImageProvider()
//  {
//    return provider;
//  }

@Override
public Configuration setupOutput(final Configuration conf) throws DataProviderException
{
  Configuration conf1 = provider.setupSparkJob(conf);
  Configuration conf2 = super.setupOutput(conf1);
  setupConfig(conf2, null);
  return conf2;
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
private void setupConfig(final Configuration conf, final Job job) throws DataProviderException
{
  try
  {
    // zoom level - output zoom level
    zoomLevel = context.getZoomLevel();
//      zoomLevel = conf.getInt("zoomlevel", 0);
    if (zoomLevel != 0)
    {
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOMLEVEL, Integer.toString(zoomLevel));
    }

    //conf.set("zoomLevel", Integer.toString(zoomLevel));
    if (doBulk || forceBulk)
    {
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_JOBTYPE,
          MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_BULK);
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PREFIX + Integer.toString(zoomLevel),
          MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_BULK);
    }
    else
    {
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_JOBTYPE,
          MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_DIRECT);
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PREFIX + Integer.toString(zoomLevel),
          MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_DIRECT);

    }
    Properties props = AccumuloConnector.getAccumuloProperties();

    // this used to be the variable "name" in ImageOutputFormatContext, but was always "".
    String enc = AccumuloConnector.encodeAccumuloProperties("");
    conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE, enc);

//        conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE,
//                 props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE));
//        conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS,
//                 props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS));

    if (props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE) == null)
    {
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE, this.table);
    }
    else
    {
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE,
          props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE));
    }

//        // username and password
//        conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER,
//                 props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER));
//
//        // make sure the password is set with Base64Encoding
//        String pw = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD);
//        String isEnc = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64, "false");
//
//        if(isEnc.equalsIgnoreCase("true")){
//          conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
//                   props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD));
//        } else {
//          byte[] p = Base64.encodeBase64(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD).getBytes());
//
//          conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD,
//                   new String(p));
//          conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64,
//                   new String("true"));
//        }

    if (conf.get(MrGeoConstants.MRGEO_PROTECTION_LEVEL) != null)
    {
      cv = new ColumnVisibility(conf.get(MrGeoConstants.MRGEO_PROTECTION_LEVEL));
    }
    if (cv == null)
    {

      if (props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ))
      {

        conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ,
            props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));

        cv = new ColumnVisibility(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ));

      }

    }
    else
    {
      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ, new String(cv.getExpression()));
    }


    if (doBulk || forceBulk)
    {

      LongRectangle outTileBounds = tileBounds.toLongRectangle();

      // setup the output for the job
      if (props.containsKey(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR))
      {
        workDir = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR);
        if (workDir != null)
        {
          workDir += File.separator;
        }
      }
      else
      {
        workDir = "";
      }
      workDir += AccumuloMrsPyramidFileOutputFormat.class.getSimpleName() +
          File.separator +
          this.table +
          File.separator;// +
//            System.currentTimeMillis() +
//            File.separator;

      // delete the work dir if possible
      Path wd = new Path(workDir);
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(wd))
      {
        fs.delete(wd, true);
      }

      conf.set(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR, workDir);

      if (job != null)
      {
        // determine the starting points for the splits
        ArrayList<Pair<Long, Long>> splitPoints = new ArrayList<Pair<Long, Long>>();

        // think about the multiple levels and creating other splits!!!

        long step = bulkThreshold / outTileBounds.getWidth();
        long rem = bulkThreshold % outTileBounds.getWidth();
        if (rem > 0)
        {
          step++;
        }
        for (long y = outTileBounds.getMinY(); y <= outTileBounds.getMaxY(); y += step)
        {
          Pair<Long, Long> cur = new Pair<Long, Long>(outTileBounds.getMinX(), y);
          splitPoints.add(cur);
        }

        // we now have our list of split points
        // now build the splits file!!!
        try (BufferedOutputStream bos = new BufferedOutputStream(fs.create(new Path(workDir + "splits.txt"))))
        {
          try (PrintStream out = new PrintStream(bos))
          {
            for (Pair<Long, Long> p : splitPoints)
            {
              long split = TMSUtils.tileid(p.getFirst(), p.getSecond(), zoomLevel);
              //TileIdWritable t = new TileIdWritable(split);
              Text t = new Text(longToBytes(split));
              out.println(Base64Utils.encodeObject(t.toString()));
              log.debug("Point: " + p.getFirst() + "\t" + p.getSecond() + "\t" + split + "\t" + t.getLength());
            }

            job.setNumReduceTasks(splitPoints.size() + 1);
            out.close();

            job.setPartitionerClass(AccumuloMrGeoRangePartitioner.class);
            AccumuloMrGeoRangePartitioner.setSplitFile(job, workDir + "splits.txt");

          }
        }
        catch (IOException ioe)
        {
          throw new DataProviderException("Problem creating output splits.txt for bulk ingest directory.", ioe);
        }

        job.setOutputFormatClass(AccumuloMrsPyramidFileOutputFormat.class);
      }
      Path workFilesPath = new Path(workDir + "files");
      if (job != null)
      {
        AccumuloMrsPyramidFileOutputFormat.setOutputPath(job, workFilesPath);
        //AccumuloMrsPyramidFileOutputFormat.setZoomLevel(zoomLevel);
      }
      else
      {
        Path outputDir = workFilesPath.getFileSystem(conf).makeQualified(
            workFilesPath);
//          conf.set(AccumuloMrsPyramidFileOutputFormat.OUTDIR, outputDir.toString());
        conf.set("mapred.output.dir", outputDir.toString());
        conf.set("mapreduce.output.fileoutputformat.outputdir", outputDir.toString());
      }

    }
    else
    {
      if (job != null)
      {
        log.info("Setting the output format of: " +
            AccumuloMrsPyramidOutputFormat.class.getCanonicalName());

        job.setOutputFormatClass(AccumuloMrsPyramidOutputFormat.class);
        AccumuloMrsPyramidOutputFormat.setJob(job);

        log.info("Setting zoom level to " + zoomLevel);
        log.info("Visibility is " + cv.toString());
        log.info("Setting the number of reducers to " + MrGeoAccumuloConstants.MRGEO_DEFAULT_NUM_REDUCERS);
        job.setNumReduceTasks(MrGeoAccumuloConstants.MRGEO_DEFAULT_NUM_REDUCERS);
      }
    }

    if (job != null)
    {
      job.setOutputKeyClass(TileIdWritable.class);
      job.setOutputValueClass(RasterWritable.class);
    }

  }
  catch (IOException ioe)
  {
    throw new DataProviderException("Error running job setup", ioe);
  }

} // end setupJob


@Override
public void finalizeExternalSave(Configuration conf) throws DataProviderException
{
  performTeardown(conf);
}

//  private void teardownForSpark(final Configuration conf) throws DataProviderException
//  {
//    performTeardown(conf);
//  }
@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
private void performTeardown(final Configuration conf) throws DataProviderException
{
  String myJobType = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PREFIX + Integer.toString(zoomLevel));


  // TODO Auto-generated method stub
  if (myJobType.equals(MrGeoAccumuloConstants.MRGEO_ACC_VALUE_JOB_BULK))
  {
    // do bulk ingest now
    Connector conn = AccumuloConnector.getConnector();
    FileSystem fs = null;
    PrintStream out = null;

    if (workDir == null)
    {
      workDir = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_WORKDIR);
    }

    try
    {
      log.info("Bulk ingest starting from working directory of " + workDir);
      fs = FileSystem.get(conf);


      Path working = new Path(workDir + File.separator, MrGeoAccumuloConstants.MRGEO_ACC_FILE_NAME_BULK_WORKING);
      Path completed = new Path(workDir + File.separator, MrGeoAccumuloConstants.MRGEO_ACC_FILE_NAME_BULK_DONE);
      if (fs.exists(working) || fs.exists(completed))
      {
        log.info("Bulk ingest completed already.");
        return;
      }
      else
      {
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

      if (!conn.tableOperations().exists(table))
      {
        conn.tableOperations().create(table, true);
        HashMap<String, Set<Text>> groups = new HashMap<String, Set<Text>>();
//        for(int i = 1; i <= 18; i++){
////        		String k = Integer.toString(i);
//          HashSet<Text> hs = new HashSet<Text>();
//
//          hs.add(new Text(Integer.toString(i)));
//        }
        conn.tableOperations().setLocalityGroups(table, groups);
      }

      conn.tableOperations().importDirectory(table, workDir + "files", workDir + "failures", true);
      conn.tableOperations().compact(table, new Text("" + 0x00), new Text("" + 0xFF), true, false);

      FSDataOutputStream fout = fs.create(completed);
      fout.write(("zoom level = " + Integer.toString(zoomLevel) + "\n").getBytes());
      fout.close();
      fs.delete(working, true);

    }
    catch (IOException | TableExistsException | TableNotFoundException | AccumuloSecurityException | AccumuloException e)
    {
      throw new DataProviderException("Problem doing bulk ingest.", e);

    }
  }

} // end finalizeExternalSave


public byte[] longToBytes(long x)
{
  ByteBuffer buffer = ByteBuffer.allocate(8);
  buffer.putLong(x);
  return buffer.array();
}

@Override
public boolean validateProtectionLevel(String protectionLevel)
{
  return AccumuloUtils.validateProtectionLevel(protectionLevel);
}

} // end AccumuloMrsPyramidOutputFormatProvider
