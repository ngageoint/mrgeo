package org.mrgeo.utils;

import com.vividsolutions.jts.io.WKTReader;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryCollection;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.LineString;
import org.mrgeo.hdfs.vector.WktGeometryUtils;
import org.mrgeo.test.TestUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Map;

public class MapAlgebraTestUtils
{

  public static void compareCSV(String goldenFile, String testFile) throws FileNotFoundException, IOException
  {
    compareCSV(goldenFile, testFile, ",");
  }
  public static void compareTSV(String goldenFile, String testFile) throws FileNotFoundException, IOException
  {
    compareCSV(goldenFile, testFile, "\t");
  }
  public static void compareCSV(String goldenFile, String testFile, String seperator) throws IOException
  {
    String goldenString;

    WKTReader reader = new WKTReader();

    try
    {
      goldenString = TestUtils.readPath(new Path(goldenFile));
    }
    catch (FileNotFoundException e1)
    {
      goldenString = TestUtils.readFile(new File(goldenFile));
    }

    String testString;
    try
    {
      testString = TestUtils.readPath(new Path(testFile));
    }
    catch (FileNotFoundException e1)
    {
      testString = TestUtils.readFile(new File(testFile));
    }


    Assert.assertNotNull("Golden file null!", goldenString);
    Assert.assertTrue("Golden file empty!", goldenString.length() > 0);

    Assert.assertNotNull("Test file null!", testString);
    Assert.assertTrue("Test file empty!", testString.length() > 0);

    String[] goldenLines = goldenString.split("\n");
    String[] testLines = testString.split("\n");

    Assert.assertTrue("Golden file has no lines!", goldenLines.length > 0);
    Assert.assertTrue("tEST file has no lines!", testLines.length > 0);

    Assert.assertEquals("Number of lines mismatch", goldenLines.length, testLines.length);

    for (int line = 0; line < goldenLines.length; line++)
    {
      String[] goldenFields = goldenLines[line].split(seperator);
      String[] testFields = testLines[line].split(seperator);

      Assert.assertEquals("Number of fields mismatch! (line: " + line + ")", 
        goldenFields.length, testFields.length);

      for (int field = 0; field < goldenFields.length; field++)
      {
        String goldenVal = goldenFields[field];
        String testVal = testFields[field];
                
        try
        {
          Number golden = NumberFormat.getInstance().parse(goldenVal);
          Number test = NumberFormat.getInstance().parse(testVal);

          Assert.assertEquals("Numbers are different! (line: " + line + ")", 
            golden.doubleValue(), test.doubleValue(), 0.00000000001);

          continue;
        }
        catch (ParseException e)
        {
        }

        // is it WKT?
        if (WktGeometryUtils.isValidWktGeometry(goldenVal) &&
            WktGeometryUtils.isValidWktGeometry(testVal))
        {
          try
          {
            Geometry goldenGeom = GeometryFactory.fromJTS(reader.read(goldenVal));
            Geometry testGeom = GeometryFactory.fromJTS(reader.read(testVal));

            compareGeometry(goldenGeom, testGeom);
            continue;
          }
          catch (com.vividsolutions.jts.io.ParseException e)
          {
          }
        }
        // not numbers?, try a string compare
        Assert.assertEquals("Strings are different! (line: " + line + ")", goldenFields[field], testFields[field]);
      }
    }
  }

  public static void compareGeometry(Geometry golden, Geometry test)
  {
    Assert.assertEquals("Geometry types are different", golden.type(), test.type());
    
    // test attributes
    Map<String, String> goldenAttr = golden.getAllAttributes();
    Map<String, String> testAttr = test.getAllAttributes();
    
    for (Map.Entry<String, String> entry : goldenAttr.entrySet())
    {
      Assert.assertTrue("Missing Key: " + entry.getKey(), testAttr.containsKey(entry.getKey()));
      Assert.assertEquals("Bad value! (key: " + entry.getKey() + ")", entry.getValue(), testAttr.get(entry.getKey()));
    }
    
    switch (golden.type())
    {
    case COLLECTION:
      for (int i = 0; i < ((GeometryCollection)(golden)).getNumGeometries(); i++)
      {
        compareGeometry(((GeometryCollection)(golden)).getGeometry(i), ((GeometryCollection)(test)).getGeometry(i));
      }
      break;
    case LINESTRING:
      for (int i = 0; i < ((LineString)(golden)).getNumPoints(); i++)
      {
        compareGeometry(((LineString)(golden)).getPoint(i), ((LineString)(test)).getPoint(i));
      }
      break;
    case POINT:
      org.mrgeo.geometry.Point goldenPoint = (org.mrgeo.geometry.Point)golden;
      org.mrgeo.geometry.Point testPoint = (org.mrgeo.geometry.Point)test;
      
      Assert.assertEquals("Bad X value!", goldenPoint.getX(), testPoint.getX(), 0.000000001);
      Assert.assertEquals("Bad Y value!", goldenPoint.getY(), testPoint.getY(), 0.000000001);
      Assert.assertEquals("Bad Z value!", goldenPoint.getZ(), testPoint.getZ(), 0.000000001);
      
      break;
    case POLYGON:
      break;
    default:
      break;
    
    }
  }
}
