/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.accumulo.output.image;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase;
import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.data.accumulo.utils.Base64Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AccumuloMrsPyramidOutputFormat extends OutputFormat<TileIdWritable, RasterWritable>
{
private static final Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidOutputFormat.class);
private static boolean outputInfoSet = false;
private static Job job;
private int zoomLevel = -1;
private String table = null;
private String username = null;
private String password = null;
private String instanceName = null;
private String zooKeepers = null;
private String vizStr = null;
private ColumnVisibility colViz = null;

// do compression!!!
//  private boolean useCompression = false;
//  private CompressionCodec codec;
//  private Compressor decompressor;
private AccumuloOutputFormat _innerFormat = null;
private RecordWriter _innerRecordWriter;

public AccumuloMrsPyramidOutputFormat()
{
}


public AccumuloMrsPyramidOutputFormat(int z, ColumnVisibility cv)
{
  zoomLevel = z;
  colViz = cv;
}

public static void setJob(Job j)
{
  job = j;
}

public void setZoomLevel(int z)
{
  zoomLevel = z;
}

@Override
public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException
{

  // make sure the inner format is created
  if (_innerFormat == null)
  {
    initialize(context);
  }

  // make sure output specs are dealt with
  _innerFormat.checkOutputSpecs(context);

} // end checkOutputSpecs


@Override
public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
    InterruptedException
{
  // TODO Auto-generated method stub
  return new NullOutputFormat<TileIdWritable, RasterWritable>().getOutputCommitter(context);
} // end getOutputCommitter

/**
 * Instantiate a RecordWriter as required.  This will create an RecordWriter
 * from the internal AccumuloOutputFormat
 */
@Override
public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException,
    InterruptedException
{

  if (zoomLevel == -1)
  {
    zoomLevel = Integer.parseInt(context.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOMLEVEL));
  }

  if (_innerFormat == null)
  {
    initialize(context);
  }

  if (_innerRecordWriter == null)
  {
    _innerRecordWriter = _innerFormat.getRecordWriter(context);
  }
  String pl = context.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ);
  if (colViz == null)
  {
    colViz = new ColumnVisibility(pl);
  }
  AccumuloMrGeoRecordWriter outRW =
      new AccumuloMrGeoRecordWriter(zoomLevel, table, _innerRecordWriter, new String(colViz.getExpression()));

  return outRW;
} // end getRecordWriter

/**
 * Set all the initial parameters needed in this class for connectivity
 * out to Accumulo.
 *
 * @param context
 */
@SuppressWarnings("squid:S2696") // Don't quite know this one :(
private void initialize(JobContext context)
{//Configuration conf){

  Configuration conf = context.getConfiguration();
  try
  {
    // output zoom level
    log.info("Working from zoom level = " + zoomLevel);
    if (zoomLevel == -1)
    {
      zoomLevel = Integer.parseInt(conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOMLEVEL));
    }

    table = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_OUTPUT_TABLE);
    username = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER);
    instanceName = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_INSTANCE);
    zooKeepers = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS);

    String pl = conf.get(MrGeoConstants.MRGEO_PROTECTION_LEVEL);
    if (pl != null)
    {
      colViz = new ColumnVisibility(pl);
    }
    else if (colViz == null)
    {
      vizStr = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ);

      if (vizStr == null)
      {
        colViz = new ColumnVisibility();
      }
      else
      {
        colViz = new ColumnVisibility(vizStr);
      }
    }

    password = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD);
    String isEnc = conf.get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PWENCODED64, "false");
    if (isEnc.equalsIgnoreCase("true"))
    {
      password = Base64Utils.decodeToString(password);
    }

    if (_innerFormat != null)
    {
      return;
    }

    _innerFormat = AccumuloOutputFormat.class.newInstance();
    AuthenticationToken token = new PasswordToken(password.getBytes());
//      log.info("Setting output with: u = " + username);
//      log.info("Setting output with: p = " + password);
//      log.info("Setting output with: i = " + instanceName);
//      log.info("Setting output with: z = " + zooKeepers);

    boolean connSet = ConfiguratorBase.isConnectorInfoSet(AccumuloOutputFormat.class, conf);
    if (!connSet)
    {
      // job not always available - do it how Accumulo does it
      OutputConfigurator.setConnectorInfo(AccumuloOutputFormat.class,
          conf,
          username,
          token);
      ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(instanceName);
      cc.setProperty(ClientProperty.INSTANCE_ZK_HOST, zooKeepers);

      OutputConfigurator.setZooKeeperInstance(AccumuloOutputFormat.class, conf, cc);
      OutputConfigurator.setDefaultTableName(AccumuloOutputFormat.class, conf, table);
      OutputConfigurator.setCreateTables(AccumuloOutputFormat.class, conf, true);

      outputInfoSet = true;
    }
  }
  catch (InstantiationException | IOException | ClassNotFoundException |
      AccumuloSecurityException | IllegalAccessException e)
  {
    log.error("Exception thrown", e);
  }

} // end initialize

/**
 * The AccumuloGaSurRecordWriter wraps the AccumuloOutputFormat RecordWriter class.  When
 * writing to the class, the wrapped class write method is called.
 */
private static class AccumuloMrGeoRecordWriter extends RecordWriter<TileIdWritable, RasterWritable>
{
  private RecordWriter<Text, Mutation> _inRecordWriter = null;
  private int zoomLevel = -1;
  private String table = null;
  private Text outTable = new Text();
  private ColumnVisibility cv;

  /**
   * The constructor sets up all the needed items for putting data into AccumuloFileOutputFormat
   *
   * @param zl    output zoom level
   * @param t     table being used for writes
   * @param intRW internal RecorWriter
   */
  public AccumuloMrGeoRecordWriter(int zl, String t, RecordWriter<Text, Mutation> intRW, String pl)
  {
    zoomLevel = zl;
    table = t;
    if (pl == null)
    {
      cv = new ColumnVisibility();
    }
    else
    {
      cv = new ColumnVisibility(pl);
    }

    if (table.startsWith(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX))
    {
      table = table.replace(MrGeoAccumuloConstants.MRGEO_ACC_PREFIX, "");
    }

    outTable.set(table);
    _inRecordWriter = intRW;
  } // end constructor


  /**
   * This is needed to close out the internal RecordWriter
   */
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException
  {
    // TODO Auto-generated method stub
    _inRecordWriter.close(arg0);

  } // end close


  /**
   * The work is done here for preparing the output Mutation.  The TileIdWritable
   * and RasterWritable are transformed here.
   */
  @Override
  public void write(TileIdWritable key, RasterWritable value) throws IOException,
      InterruptedException
  {
    int zoom = zoomLevel;
    if (key instanceof TileIdZoomWritable)
    {
      zoom = ((TileIdZoomWritable) key).getZoom();
    }

    //ColumnVisibility cv = new ColumnVisibility();
    // transform the keys
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(key.get());
    Mutation m = new Mutation(new Text(buf.array()));
    // We only want the actual bytes for the value, not the full array of bytes
    Value v = new Value(value.copyBytes());
    m.put(Integer.toString(zoom), Long.toString(key.get()), cv, v);
    _inRecordWriter.write(outTable, m);

  } // end write

} // end AccumuloGaSurRecordWriter


} // end AccumuloMrsPyramidOutputFormat
