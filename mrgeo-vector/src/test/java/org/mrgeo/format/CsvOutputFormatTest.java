/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.format;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import java.io.File;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class CsvOutputFormatTest 
{
  @Test
  @Category(UnitTest.class)
  public void testBasics() throws Exception
  {
    // this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      String output = TestUtils.composeOutputDir(CsvOutputFormatTest.class);

      Configuration c = new Configuration();
      fs.setConf(c);
      Path testFile = new Path(output, "testBasics.csv");
      testFile = fs.makeQualified(testFile);
      Path columns = new Path(testFile.toString() + ".columns");

      CsvOutputFormat.CsvRecordWriter writer = new CsvOutputFormat.CsvRecordWriter(columns,
          testFile);


      WritableGeometry f = GeometryFactory.createEmptyGeometry();

      f.setAttribute("string1", "foo");
      f.setAttribute("int1", "1");
      f.setAttribute("double1", "2.0");
      writer.write(new LongWritable(0), f);

      f.setAttribute("string1", "bar");
      f.setAttribute("int1", "3");
      f.setAttribute("double1", "4.0");
      writer.write(new LongWritable(1), f);

      writer.close(null);

      String input = TestUtils.composeInputDir(CsvOutputFormatTest.class);
      {
        File csvBaselineFile = new File(input, "testBasics.csv");
        File csvOutputFile = new File(output, "testBasics.csv");
        Assert.assertTrue(String.format("The content in %s does not match %s",
              csvOutputFile.getAbsoluteFile(), csvBaselineFile.getAbsoluteFile()),
            FileUtils.contentEquals(csvBaselineFile, csvOutputFile));
      }
      {
        File columnsBaselineFile = new File(input, "testBasics.csv.columns");
        File columnsOutputFile = new File(output, "testBasics.csv.columns");
        Assert.assertTrue(String.format("The content in %s does not match %s",
              columnsOutputFile.getAbsoluteFile(), columnsBaselineFile.getAbsoluteFile()),
            FileUtils.contentEquals(columnsBaselineFile, columnsOutputFile));
      }
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
}
