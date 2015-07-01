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

package org.mrgeo.format;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.Point;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Formatter;
import java.util.Locale;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class OsmContentHandlerTest 
{
  private static final Logger log = LoggerFactory.getLogger(OsmContentHandlerTest.class);

  @Before public void setUp()
  {

  }

  public void testOff()
  {
  }

  @Ignore
  @Test
  public void OfftestBenchmark() throws Exception
  {
    // @TODO this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      String input = TestUtils.composeInputDir(OsmContentHandlerTest.class);

      Configuration c = new Configuration();
      c.set("xml.content.handler", OsmContentHandler.class.getCanonicalName());
      c.set("xml.pattern", "node");
      c.set("xml.root.tag", "osm");

      fs.setConf(c);
      Path testFile = new Path(input, "sample.osm");
      testFile = fs.makeQualified(testFile);

      c.set("xml.pattern", "place");

      FileSplit split = new FileSplit(testFile, 0, 64 * 1048576, null);
      RecordReader<LongWritable, Geometry> reader = new SaxInputFormat<LongWritable, Geometry>()
          .createRecordReader(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));

      int l = 0;
      long start = new Date().getTime();
      while (reader.nextKeyValue())
      {
        l++;
      }
      long elapsed = new Date().getTime() - start;
      log.debug("ms per record: {} record count: {}", (double) elapsed / (double) l, l);
      Assert.assertEquals(1, l);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      fs.close();
    }
  }

  @Ignore
  @Test
  public void OfftestBenchmark2() throws Exception
  {
    // @TODO this class and its unit tests are a work in progress.
    try
    {

      Configuration c = new Configuration();
      c.set("xml.content.handler", OsmContentHandler.class.getCanonicalName());
      c.set("xml.pattern", "node");
      c.set("xml.root.tag", "osm");

      FileSystem fs = HadoopFileUtils.getFileSystem();
      fs.setConf(c);
      Path testFile = new Path("/user/jason.surratt/", "georgia.osm");
      testFile = fs.makeQualified(testFile);

      c.set("xml.pattern", "place");

      FileSplit split = new FileSplit(testFile, 0, fs.getFileStatus(testFile).getLen(), null);
      RecordReader<LongWritable, Geometry> reader = new SaxInputFormat<LongWritable, Geometry>()
          .createRecordReader(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));

      FileOutputStream fos = new FileOutputStream("georgia-points.txt");
      BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));

      Formatter formatter = new Formatter(wr, Locale.US);

      int l = 0;
      long start = new Date().getTime();
      while (reader.nextKeyValue() && l < 10000)
      {
        l++;
        Geometry f = reader.getCurrentValue();
        if (f instanceof Point)
        {
          Point p = (Point) f;
          formatter.format("%.7f %.7f\n", p.getX(), p.getY());
        }
      }
      
      formatter.close();
      
      long elapsed = new Date().getTime() - start;
      log.debug("ms per record: {} record count: {}", (double) elapsed / (double) l, l);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
