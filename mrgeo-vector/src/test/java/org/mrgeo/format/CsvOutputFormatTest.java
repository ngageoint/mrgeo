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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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

        File csvBaselineFile = new File(input, "testBasics.csv");
        File csvOutputFile = new File(output, "testBasics.csv");
        TestUtils.compareTextFiles(csvBaselineFile.getAbsoluteFile(), csvOutputFile.getAbsoluteFile());

        File columnsBaselineFile = new File(input, "testBasics.csv.columns");
        File columnsOutputFile = new File(output, "testBasics.csv.columns");

        TestUtils.compareTextFiles(columnsBaselineFile.getAbsoluteFile(), columnsOutputFile.getAbsoluteFile());
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
