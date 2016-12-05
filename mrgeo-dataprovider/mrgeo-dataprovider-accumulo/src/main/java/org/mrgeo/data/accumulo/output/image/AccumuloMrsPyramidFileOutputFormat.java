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

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.tile.TileIdZoomWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AccumuloMrsPyramidFileOutputFormat extends FileOutputFormat<Key, Value>
{

private static final Logger log = LoggerFactory.getLogger(AccumuloMrsPyramidFileOutputFormat.class);
private String vizStr = null;
private ColumnVisibility cv = null;
private AccumuloFileOutputFormat _innerFormat = null;
private RecordWriter _innerRecordWriter = null;
private int zoomLevel = -1;

public AccumuloMrsPyramidFileOutputFormat()
{
}

public AccumuloMrsPyramidFileOutputFormat(int z, ColumnVisibility cv)
{
  zoomLevel = z;
  this.cv = cv;
  vizStr = new String(this.cv.getExpression());
  log.debug(this.getClass().getCanonicalName() + " has visibility of: " + vizStr);
} // end constructor

public void setZoomLevel(int zl)
{
  zoomLevel = zl;
}

@Override
public RecordWriter getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException
{
  // look for zoom level in the context
  // check if we have initialized the internal file format
  if (_innerFormat == null)
  {

    if (zoomLevel == -1)
    {
      // zoomLevel =
      // Integer.parseInt(context.getConfiguration().get("zoomlevel"));
      zoomLevel = Integer.parseInt(context.getConfiguration().get(
          MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOMLEVEL));
    }
    _innerFormat = new AccumuloFileOutputFormat();

  }
  String pl = context.getConfiguration().get(MrGeoConstants.MRGEO_PROTECTION_LEVEL);

  if (pl != null)
  {
    cv = new ColumnVisibility(pl);
    vizStr = pl;
  }
  if (cv == null || vizStr == null)
  {
    vizStr = context.getConfiguration().get(MrGeoAccumuloConstants.MRGEO_ACC_KEY_VIZ);
    if (vizStr == null)
    {
      cv = new ColumnVisibility();
    }
    else
    {
      cv = new ColumnVisibility(vizStr);
    }
  }

  _innerRecordWriter = _innerFormat.getRecordWriter(context);

  AccumuloMrGeoFileRecordWriter outRW = new AccumuloMrGeoFileRecordWriter(
      zoomLevel, vizStr, _innerRecordWriter);

  return outRW;
} // end getRecordWriter

/**
 *
 */
private static class AccumuloMrGeoFileRecordWriter extends RecordWriter<TileIdWritable, RasterWritable>
{
  Text zoom = new Text();
  Text cv = new Text();
  private RecordWriter<Key, Value> _inRecordWriter = null;
  private int zoomLevel = -1;


  /**
   * The constructor sets up all the needed items for putting data into AccumuloOutputFormat
   *
   * @param zl    output zoom level
   * @param intRW internal RecordWriter
   */
  public AccumuloMrGeoFileRecordWriter(int zl, String viz, RecordWriter<Key, Value> intRW)
  {
    zoomLevel = zl;
    zoom.set(Integer.toString(zoomLevel));
    _inRecordWriter = intRW;
    if (viz != null)
    {
      cv.set(viz);
    }

  } // end constructor


  /**
   * This is needed to close out the internal RecordWriter
   */
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException
  {
    _inRecordWriter.close(arg0);
  } // end close


  /**
   * The work is done here for preparing the output Key and Value.  The
   * TileIdWritable and RasterWritable are transformed here.
   */
  @Override
  public void write(TileIdWritable key, RasterWritable value) throws IOException,
      InterruptedException
  {
    int z = zoomLevel;
    if (key instanceof TileIdZoomWritable)
    {
      z = ((TileIdZoomWritable) key).getZoom();
    }

    // transform the keys
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(key.get());
    Text row = new Text(buf.array());
    Key outKey = new Key(row, new Text(Integer.toString(z)), new Text(Long.toString(key.get())), cv);
    // We only want the actual bytes for the value, not the full array of bytes
    Value v = new Value(value.copyBytes());
    _inRecordWriter.write(outKey, v);

  } // end write


} // end AccumuloMrGeoFileRecordWriter


} // end AccumuloMrsPyramidFileOutputFormat
