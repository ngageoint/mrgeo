/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.format;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.format.InlineCsvInputFormat.InlineCsvReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.Point;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class InlineCsvInputFormatTest
{
  private static final double EPSILON = 1e-10;

  @Test
  @Category(UnitTest.class)
  public void testBasics() throws Exception
  {
    // this class and its unit tests are a work in progress.
    try
    {
      String input = TestUtils.composeInputDir(InlineCsvInputFormatTest.class);

      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();

      InlineCsvInputFormat.setColumns(c, "a,b,GEOMETRY");
      InlineCsvInputFormat.setValues(c, "1,2,'POINT(3.0 4)';5,6,'POINT(7 8)'");

      List<InputSplit> splits = new InlineCsvInputFormat().getSplits(null);

      InlineCsvInputFormat.InlineCsvRecordReader reader = new InlineCsvInputFormat.InlineCsvRecordReader();
      reader.initialize(splits.get(0), HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      int l = 0;

      StringBuffer buf = new StringBuffer();

      String str;
      while (reader.nextKeyValue())
      {
        str = String.format("Feature %d\n", l);
        buf.append(str);

        TreeMap<String, String> sorted = new TreeMap<>(reader.getCurrentValue().getAllAttributes());
        for (Map.Entry attr : sorted.entrySet())
        {
          str = String.format("  %s : %s\n", attr.getKey(), attr.getValue());
          buf.append(str);
        }
        l++;
      }

      File f = new File(input + "testBasics.txt");
      byte[] buffer = new byte[(int) f.length()];
      FileInputStream fis = new FileInputStream(f);
      fis.read(buffer);
      fis.close();

      Assert.assertEquals(new String(buffer), buf.toString());
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometryNotLast() throws Exception
  {
    // this class and its unit tests are a work in progress.
    try
    {
      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();

      InlineCsvInputFormat.setColumns(c, "GEOMETRY,a,b");
      InlineCsvInputFormat.setValues(c, "'POINT(3.0 4)',1,2;'POINT(7 8)',5,6");

      List<InputSplit> splits = new InlineCsvInputFormat().getSplits(null);

      InlineCsvInputFormat.InlineCsvRecordReader reader = new InlineCsvInputFormat.InlineCsvRecordReader();
      reader.initialize(splits.get(0), HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));

      Assert.assertTrue(reader.nextKeyValue());
      Geometry feature = reader.getCurrentValue();
      Assert.assertNotNull(feature);
      // Validate the schema

      Assert.assertEquals(3, feature.getAllAttributes().size());
      Assert.assertEquals("POINT(3.0 4)", feature.getAttribute("GEOMETRY"));
      Assert.assertEquals("1", feature.getAttribute("a"));
      Assert.assertEquals("2", feature.getAttribute("b"));
      // Validate the first feature
      Assert.assertTrue(feature instanceof Point);
      Assert.assertEquals(3.0, ((Point) feature).getX(), EPSILON);
      Assert.assertEquals(4.0, ((Point) feature).getY(), EPSILON);
      Assert.assertEquals("1", feature.getAttribute("a"));
      Assert.assertEquals("2", feature.getAttribute("b"));

      // Validate the second feature
      Assert.assertTrue(reader.nextKeyValue());
      feature = reader.getCurrentValue();
      Assert.assertNotNull(feature);
      Assert.assertTrue(feature instanceof Point);
      Assert.assertEquals(7.0, ((Point) feature).getX(), EPSILON);
      Assert.assertEquals(8.0, ((Point) feature).getY(), EPSILON);
      Assert.assertEquals("5", feature.getAttribute("a"));
      Assert.assertEquals("6", feature.getAttribute("b"));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void test_setValues() throws Exception
  {
    Job j = new Job(new Configuration());
    Configuration c = j.getConfiguration();
    String inpValue = "test values - 1, 4, 5; 45% 20% #34 2-3 points ";
    InlineCsvInputFormat.setValues(c, inpValue);
    Assert.assertEquals(inpValue, InlineCsvInputFormat.getValues(c));
  }

  @Test
  @Category(UnitTest.class)
  public void test_setColumns() throws Exception
  {
    Job j = new Job(new Configuration());
    Configuration c = j.getConfiguration();
    String inpValue = "test columns - GEOMETRY#, POINT(), %LINESTRING%, CITY_LIMITS";
    InlineCsvInputFormat.setColumns(c, inpValue);
    Assert.assertEquals(inpValue, InlineCsvInputFormat.getColumns(c));
  }

  @Test
  @Category(UnitTest.class)
  public void test_getSplits() throws Exception
  {
    Job j = new Job(new Configuration());
    TaskAttemptContext tc = HadoopUtils.createTaskAttemptContext(j.getConfiguration(), new TaskAttemptID());
    List<InputSplit> outList = new InlineCsvInputFormat().getSplits(tc);
    Assert.assertEquals(1, outList.size());
    FileSplit fs = (FileSplit) outList.get(0);
    Assert.assertEquals(0, fs.getStart());
    Assert.assertEquals(0, fs.getLength());
    Assert.assertEquals(0, fs.getLocations().length);
    Assert.assertEquals("/", fs.getPath().toString());
  }

  @Test
  @Category(UnitTest.class)
  public void testRecordReader_parseColumns() throws Exception
  {
    //Job j = new Job(new Configuration());
    String[] expected = new String[] { "[x Nominal ]", "[ y Nominal ]", "[ GEOMETRY Nominal ]",
        "[PROPERTY_VALUE Nominal ]", "[INTEGER# Nominal ]", "[ DOUBLE  Nominal ]",
        "[ %PERCENT Nominal ]", "[ \"VALUES\" Nominal ]", "[ lowercase  isfloat\n Nominal ]" };

//    InlineCsvInputFormat.InlineCsvReader reader = 
//        new InlineCsvInputFormat.InlineCsvReader();
    String columns = "x, y, GEOMETRY,PROPERTY_VALUE,INTEGER#, DOUBLE , %PERCENT, \"VALUES\", lowercase  isfloat\n"
        + "";
    ColumnDefinitionFile cdf = InlineCsvReader.parseColumns(columns, ',');
    cdf.getColumns();
    for (int i = 0; i < cdf.getColumns().size(); i++)
    {
      Assert.assertEquals(expected[i], "[" + cdf.getColumns().get(i).getName() + " "
          + cdf.getColumns().get(i).getType() + " ]");
    }
  }

}
