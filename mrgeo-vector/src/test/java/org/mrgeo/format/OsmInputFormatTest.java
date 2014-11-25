/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.format;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class OsmInputFormatTest 
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(OsmInputFormatTest.class);

  
  @Before public void setUp()
  {
  }
  
  @Test 
  @Category(UnitTest.class) 
  public void testBasics() throws Exception
  {
    // this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      String input = TestUtils.composeInputDir(OsmInputFormatTest.class);

      Configuration c = new Configuration();
      fs.setConf(c);
      Path testFile = new Path(input, "sample.osm");
      testFile = fs.makeQualified(testFile);

      c.set("xml.pattern", "place");

      FileSplit split = new FileSplit(testFile, 0, fs.getFileStatus(testFile).getLen(), null);
      OsmInputFormat.OsmRecordReader reader = new OsmInputFormat.OsmRecordReader();
      reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      int l = 0;
      while (reader.nextKeyValue() && l < 10000)
      {
        l++;
      }
      Assert.assertEquals(6, l);
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

  /**
   * This is not for normal testing, simply for debugging and development.
   * public void testBenchmark() throws Exception { // this class and its unit
   * tests are a work in progress. try {
   * 
   * Configuration c = new Configuration(); FileSystem fs =
   * HadoopUtils.getFileSystem(); fs.setConf(c); //Path testFile = new
   * Path(input, "sample.osm"); Path testFile = new Path("/user/jason.surratt/",
   * "georgia.osm"); testFile = fs.makeQualified(testFile);
   * 
   * c.set("xml.pattern", "place");
   * 
   * FileSplit split = new FileSplit(testFile, 0,
   * fs.getFileStatus(testFile).getLen(), null); OsmInputFormat.OsmRecordReader
   * reader = new OsmInputFormat.OsmRecordReader(); reader.initialize(split, new
   * TaskAttemptContext(c, new TaskAttemptID())); //FSDataInputStream is =
   * fs.open(testFile); //BufferedReader r = new BufferedReader(new
   * InputStreamReader(is)); FileOutputStream fos = new
   * FileOutputStream("georgia-points.txt"); BufferedWriter wr = new
   * BufferedWriter(new OutputStreamWriter(fos)); int l = 0; long start = new
   * Date().getTime();
   * 
   * Formatter formatter = new Formatter(wr, Locale.US); while
   * (reader.nextKeyValue() && l < 1000) //while (is.available() > 0 && l <
   * 10000) { //String line = r.readLine(); //is.read();
   * 
   * GeometryWritable g = reader.getCurrentValue();
   * //System.out.println(g.getClass().getName() + g.toString()); if
   * (g.getGeometry() instanceof Point) { Point p = (Point)g.getGeometry();
   * //wr.append(String.format("%f, %f\n", p.getX(), p.getY()));
   * formatter.format("%.7f,%.7f\n", p.getX(), p.getY());
   * //System.out.printf("%f, %f\n", p.getX(), p.getY()); }
   * //System.out.println(g.getGeometry().toString()); l++; } long elapsed = new
   * Date().getTime() - start; log.debug("ms per record: {}", (double)elapsed /
   * (double)l); } catch (Exception e) { e.printStackTrace(); throw e; } }
   */
}
