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
