/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.format;

import com.vividsolutions.jts.io.WKTReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.vector.WktGeometryUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class CsvInputFormatTest 
{
  private static final double EPSILON = 1e-10;

  private static String input;
  private static String output;
  
  @BeforeClass
  public static void init() throws IOException
  {
    input = TestUtils.composeInputDir(CsvInputFormatTest.class);
    output = TestUtils.composeOutputDir(CsvInputFormatTest.class);
  }
  
//  @Test
//  @Category(UnitTest.class)
//  public void fakeTest()
//  {
//    org.reflections.Reflections r = new org.reflections.Reflections("org.mrgeo");
//    java.util.Set<java.lang.reflect.Method> results = r.getMethodsAnnotatedWith(org.junit.experimental.categories.Category.class);
//    System.out.println("Got back " + results.size());
//  }

  @Test
  @Category(UnitTest.class)
  public void testBasics() throws Exception
  {
    // this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();
      fs.setConf(c);
      Path testFile = new Path(input, "testBasics.csv");
      testFile = fs.makeQualified(testFile);

      FileInputFormat.addInputPath(j, testFile);
      FileSplit split = new FileSplit(testFile, 0, 500, null);
      CsvInputFormat.CsvRecordReader reader = new CsvInputFormat.CsvRecordReader();
      reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      @SuppressWarnings("unused")
      int l = 0;

      StringBuffer buf = new StringBuffer();

      String[] base = {
          "word1:Hello word2:world number:1 ",
          "word1:foo word2:bar number:2 ",
          "word1:cat word2:dog number:3 ",
          "word1:rock word2:paper number:4 ",
          "word1:red word2:blue, number:5 ",
          "word1:,green, word2:,, number:6 ",
           };


      int index = 0;
      while (reader.nextKeyValue())
      {
        Geometry f = reader.getCurrentValue();
        String row = "";
        for (Map.Entry attr : f.getAllAttributes().entrySet())
        {
          row += attr.getKey() + ":" + attr.getValue() + " ";
        }
        Assert.assertEquals("Error in row " + index, base[index++], row);
      }

      // This hash code will tell us if anything changes then it can be manually verified.
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
  
  @Test
  @Category(UnitTest.class)
  public void testNullProcessing() throws Exception
  {
    // this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();
      fs.setConf(c);
      Path testFile = new Path(input, "testNullValues.csv");
      testFile = fs.makeQualified(testFile);

      FileInputFormat.addInputPath(j, testFile);
      FileSplit split = new FileSplit(testFile, 0, 500, null);
      CsvInputFormat.CsvRecordReader reader = new CsvInputFormat.CsvRecordReader();
      reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      @SuppressWarnings("unused")
      int l = 0;

      //StringBuffer buf = new StringBuffer();

      // Test specific rows returned to make sure the values are as expected.
      Assert.assertTrue(reader.nextKeyValue());
      Geometry f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test1", f.getAttribute("string1"));
      Assert.assertEquals(1.0, Double.parseDouble(f.getAttribute("int1")), EPSILON);
      Assert.assertEquals(1.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      // Row 2 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test2", f.getAttribute("string1"));
      Assert.assertEquals(2.0, Double.parseDouble(f.getAttribute("int1")), EPSILON);
      Assert.assertNull("Expected null value instead of: " + f.getAttribute("double1"), f.getAttribute("2"));
      // Row 3 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test3", f.getAttribute("string1"));
      Assert.assertEquals(3.0, Double.parseDouble(f.getAttribute("int1")), EPSILON);
      Assert.assertEquals(3.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      // Row 4 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test4", f.getAttribute("string1"));
      Assert.assertNull("Expected null value instead of: " + f.getAttribute("int1"), f.getAttribute("1"));
      Assert.assertEquals(4.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      // Row 5 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test5", f.getAttribute("string1"));
      Assert.assertEquals(5.0, Double.parseDouble(f.getAttribute("int1")), EPSILON);
      Assert.assertEquals(5.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      // Row 6 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test6", f.getAttribute("string1"));
      Assert.assertEquals("", f.getAttribute("int1"));
      Assert.assertEquals("", f.getAttribute("double1"));
      // Row 7 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertEquals("test7", f.getAttribute("string1"));
      Assert.assertNull("Expected null value instead of: " + f.getAttribute("int1"), f.getAttribute("int1"));
      Assert.assertNull("Expected null value instead of: " + f.getAttribute("double1"), f.getAttribute("double1"));
      Assert.assertFalse(reader.nextKeyValue());
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

  @Test
  @Category(UnitTest.class)
  public void testNullIgnore() throws Exception
  {
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      int lineCount = 0;

      // Write columns file which defines the columns title and type
      String cstr = "<?xml version='1.0' encoding='UTF-8'?>\n<AllColumns firstLineHeader='false'>\n";
      cstr += "  <Column name='name' type='Nominal'/>\n";
      cstr += "  <Column name='x' type='Numeric'/>\n";
      cstr += "  <Column name='y' type='Numeric'/>\n";
      cstr += "</AllColumns>\n";
      FileOutputStream fos = new FileOutputStream(output + "/nulXY.csv.columns");
      PrintStream ps = new PrintStream(fos);
      ps.print(cstr);
      ps.close();

      // Write csv test data
      fos = new FileOutputStream(output + "/nullXY.csv");
      ps = new PrintStream(fos);
      // populated rows
      for(int ii = 0; ii < 10; ii++)
      {
        ps.print("ASDF,1.0,1.0\n");
        lineCount++;
      }
      // empty rows
      ps.print("ASDF,,1.0\n");
      ps.print("ASDF,1.0,\n");
      ps.print("ASDF,,\n");
      lineCount += 3;
      // populated rows
      for(int ii = 0; ii < 5; ii++)
      {
        ps.print("ASDF,1.0,1.0\n");
        lineCount++;
      }
      ps.close();

      System.out.println(output + "nulXY.csv");
      
      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();
      fs.setConf(c);
      Path testFile = new Path(output, "nullXY.csv");
      testFile = fs.makeQualified(testFile);
      InputSplit split;
      long l;
      long start;

      TextInputFormat format = new TextInputFormat();
      split = new FileSplit(testFile, 0, lineCount * 1000, null);
      RecordReader<LongWritable, Text> reader2 = format.createRecordReader(split,
        HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      
      reader2.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      l = 0;
      start = System.currentTimeMillis();
      while (reader2.nextKeyValue())
      {
        reader2.getCurrentValue().toString();
        l++;
      }
      Assert.assertEquals(lineCount, l);
      System.out.printf("text line reader with null x,y ignore: %d\n", System.currentTimeMillis() - start);
      
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
  
  @Test
  @Category(UnitTest.class)
  public void testBadValues() throws Exception
  {
    // this class and its unit tests are a work in progress.
    FileSystem fs = new RawLocalFileSystem();
    try
    {
      Job j = new Job(new Configuration());
      Configuration c = j.getConfiguration();
      fs.setConf(c);
      Path testFile = new Path(input, "testErrors.csv");
      testFile = fs.makeQualified(testFile);

      FileInputFormat.addInputPath(j, testFile);
      FileSplit split = new FileSplit(testFile, 0, 500, null);
      CsvInputFormat.CsvRecordReader reader = new CsvInputFormat.CsvRecordReader();
      reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
      @SuppressWarnings("unused")
      int l = 0;

      //StringBuffer buf = new StringBuffer();

      // Test specific rows returned to make sure the values are as expected.
      Assert.assertTrue(reader.nextKeyValue());
      Geometry f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertTrue(f.isEmpty());
      Assert.assertEquals("test1,1,1.5,30.0,40.0", f.getAttribute("string1"));
      Assert.assertNull(f.getAttribute("int1"));
      Assert.assertNull(f.getAttribute("double1"));
      Assert.assertNull(f.getAttribute("x"));
      Assert.assertNull(f.getAttribute("y"));
      // Row 2 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertTrue(f.isEmpty());
      Assert.assertEquals("test2", f.getAttribute("string1"));
      Assert.assertEquals(2, Integer.parseInt(f.getAttribute("int1")));
      Assert.assertEquals("", f.getAttribute("double1"));
      Assert.assertEquals("30.abc", f.getAttribute("x"));
      Assert.assertEquals(40.0, Double.parseDouble(f.getAttribute("y")), EPSILON);
      // Row 3 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertTrue(f.isEmpty());
      Assert.assertEquals("test3", f.getAttribute("string1"));
      Assert.assertEquals(3, Integer.parseInt(f.getAttribute("int1")));
      Assert.assertEquals(3.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      Assert.assertEquals(30.0, Double.parseDouble(f.getAttribute("x")), EPSILON);
      Assert.assertEquals("40.abc", f.getAttribute("y"));
      // Row 4 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertTrue(f.isEmpty());
      Assert.assertEquals("test4", f.getAttribute("string1"));
      Assert.assertEquals("", f.getAttribute("int1"));
      Assert.assertEquals(4.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      Assert.assertEquals(30.0, Double.parseDouble(f.getAttribute("x")), EPSILON);
      Assert.assertNull(f.getAttribute("y"));
      // Row 5 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertTrue(f.isEmpty());
      Assert.assertEquals("test5", f.getAttribute("string1"));
      Assert.assertEquals(5, Integer.parseInt(f.getAttribute("int1")));
      Assert.assertEquals(5.5, Double.parseDouble(f.getAttribute("double1")), EPSILON);
      Assert.assertEquals("", f.getAttribute("x"));
      Assert.assertEquals(40.0, Double.parseDouble(f.getAttribute("y")), EPSILON);
      // Row 6 check
      Assert.assertTrue(reader.nextKeyValue());
      f = reader.getCurrentValue();
      Assert.assertNotNull(f);
      Assert.assertTrue(f.isEmpty());
      Assert.assertEquals("test6", f.getAttribute("string1"));
      Assert.assertEquals("", f.getAttribute("int1"));
      Assert.assertEquals("", f.getAttribute("double1"));
      Assert.assertEquals("", f.getAttribute("x"));
      Assert.assertEquals("", f.getAttribute("y"));

      // end
      Assert.assertFalse(reader.nextKeyValue());
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

  @Test
  @Category(UnitTest.class)
  public void testWktGeometryFixer() throws Exception
  {
    try
    {
      WKTReader wktReader = new WKTReader();
     
      //A Polygon with one exterior ring and one interior ring
      String wktGeometry = "POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))";
      boolean result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
      
      //A MultiPolygon with two Polygon values
      wktGeometry = "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))";
      result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
      
      //A LineString with four points
      wktGeometry = "LINESTRING(0 0, 10 10, 20 25, 50 60)";
      result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
      
      //A point
      wktGeometry = "POINT(15 20)";
      result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
      
      //Esri Envelope in GeoFuse tsv file
      wktGeometry = "EsriEnvelope[77.58545,54.99652,77.59644,55.0028]";
      try
      {
        wktReader.read(wktGeometry);
      }
      catch (Exception e)
      {
        result = false;
      }
      Assert.assertEquals(result, false);
      
      result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
      
      //polygon does not have the closed point
      wktGeometry = "POLYGON((102.0904541015625 14.181849032477334,102.18658447265625 14.224451017486471,102.38433837890625 14.179186142354181,102.67822265625 14.189837515179631,102.93365478515625 14.213801273022412,102.9638671875 14.160545036264903,102.919921875 14.053995128467742,102.919921875 14.000701543729708,102.81280517578125 13.92073799964004,102.744140625 13.80074084711067,102.74688720703125 13.742053062720384,102.58209228515625 13.683350573778403,102.645263671875 13.611286598054468,102.56561279296875 13.54721129739022,102.37335205078125 13.549881446917139,102.381591796875 13.386947652895737,102.381591796875 13.277372689908317,102.26898193359375 13.24261906101892,102.15362548828125 13.29875706709956,102.073974609375 13.21855594917547,102.008056640625 13.24261906101892,101.9366455078125 13.44038078817812,101.865234375 13.47777690149573,101.87896728515625 13.560561745081422,101.9366455078125 13.795406203132826,101.89544677734375 13.93406718249833))";
      try
      {
        wktReader.read(wktGeometry);
      }
      catch (Exception e)
      {
        result = false;
      }
      Assert.assertEquals(result, false);
      
      result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
      
      //A polygon points are separated by two spaces.
      wktGeometry = "POLYGON((-105.1171874958 48.768631671235  -105.46874999579 42.639763041515  -98.789062496056 42.639763041515  -105.1171874958 48.768631671235))";
      try
      {
        wktReader.read(wktGeometry);
      }
      catch (Exception e)
      {
        result = false;
      }
      Assert.assertEquals(result, false);
      
      result = WktGeometryUtils.isValidWktGeometry(wktGeometry);
      Assert.assertEquals(result, true);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
