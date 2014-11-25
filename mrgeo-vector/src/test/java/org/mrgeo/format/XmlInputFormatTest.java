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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class XmlInputFormatTest 
{
  @Test 
  @Category(UnitTest.class)   
  public void testBasics() throws Exception
  {
    // this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      String input = TestUtils.composeInputDir(XmlInputFormatTest.class);

      Configuration c = new Configuration();
      fs.setConf(c);
      Path testFile = new Path(input, "testBasics.xml");
      testFile = fs.makeQualified(testFile);

      c.set("xml.pattern", "node");

      FileSplit split = new FileSplit(testFile, 0, 50, null);
      XmlInputFormat.XmlRecordReader reader = new XmlInputFormat.XmlRecordReader();
      reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      int l = 0;
      while (reader.nextKeyValue())
      {
        System.out.printf("k: %s v: %s\n", reader.getCurrentKey(), reader.getCurrentValue());
        l++;
      }

      split = new FileSplit(testFile, 50, fs.getFileStatus(testFile).getLen() - 50, null);
      reader = new XmlInputFormat.XmlRecordReader();
      reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      while (reader.nextKeyValue())
      {
        System.out.printf("k: %s v: %s\n", reader.getCurrentKey(), reader.getCurrentValue());
        l++;
      }

      Assert.assertEquals(3, l);
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
