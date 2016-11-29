package org.mrgeo.data.vector.geowave;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.junit.UnitTest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@SuppressWarnings("all") // Test code, not included in production
public class GeoWaveVectorDataProviderTest
{
  private static final double EPSILON = 1e-5;

  @BeforeClass
  public static void setup()
  {
//    GeoWaveConnectionInfo connInfo = new GeoWaveConnectionInfo("", "", "", "", "", false);
//    GeoWaveVectorDataProvider.setConnectionInfo(connInfo);
    GeoWaveVectorDataProvider.initialized = true;
  }

  @Test
  @Category(UnitTest.class)
  public void testParseEmpty() throws IOException
  {
    Map<String, String> settings = new HashMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings("", settings);
    Assert.assertEquals(0, settings.size());
  }

  @Test
  @Category(UnitTest.class)
  public void testParseNoAssignment()
  {
    String strSettings = "abc";
    Map<String, String> settings = new HashMap<String, String>();
    try
    {
      GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
      Assert.fail("Expected an exception");;
    }
    catch(IOException e)
    {
      Assert.assertTrue("Wrong message", e.getMessage().equals("Invalid syntax. No value assignment in \"" +
                                                               strSettings + "\""));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testParseMissingValue()
  {
    String keyName = "abc";
    Map<String, String> settings = new HashMap<String, String>();
    try
    {
      GeoWaveVectorDataProvider.parseDataSourceSettings(keyName + "=", settings);
      Assert.fail("Expected an exception");;
    }
    catch(IOException e)
    {
      Assert.assertTrue("Wrong message", e.getMessage().equals("Missing value for " + keyName));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testParseSettingWithSpaces() throws IOException
  {
    String keyName = "abc";
    String value = "bad value";
    Map<String, String> settings = new HashMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(keyName + "    =    " + value + "   ", settings);
    Assert.assertEquals(1, settings.size());
    String settingName = settings.keySet().iterator().next();
    Assert.assertEquals(keyName, settingName);
    Assert.assertEquals(value, settings.get(settingName));
  }

  @Test
  @Category(UnitTest.class)
  public void testParseSettingWithoutSpaces() throws IOException
  {
    String keyName = "abc";
    String value = "bad value";
    Map<String, String> settings = new HashMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(keyName + "=" + value, settings);
    Assert.assertEquals(1, settings.size());
    String settingName = settings.keySet().iterator().next();
    Assert.assertEquals(keyName, settingName);
    Assert.assertEquals(value, settings.get(settingName));
  }

  @Test
  @Category(UnitTest.class)
  public void testParseSettingWithQuotes() throws IOException
  {
    String keyName = "abc";
    String value = "  bad value  ";
    Map<String, String> settings = new HashMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(keyName + "=\"" + value + "\"", settings);
    Assert.assertEquals(1, settings.size());
    String settingName = settings.keySet().iterator().next();
    Assert.assertEquals(keyName, settingName);
    Assert.assertEquals(value, settings.get(settingName));
  }

  @Test
  @Category(UnitTest.class)
  public void testParseSettingMissingClosingQuotes() throws IOException
  {
    String strSetting = "abc=\"bad value";
    Map<String, String> settings = new HashMap<String, String>();
    try
    {
      GeoWaveVectorDataProvider.parseDataSourceSettings(strSetting, settings);
      Assert.fail("Expected exception for missing double quote");
    }
    catch(IOException e)
    {
      Assert.assertTrue("Wrong exception message: " + e.getMessage(),
                        e.getMessage().equals(
                                "Invalid GeoWave settings string, expected ending double quote for key abc in " +
                                strSetting));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testParseSettingWithTrailingSemiColon() throws IOException
  {
    String keyName = "abc";
    String value = "bad value";
    Map<String, String> settings = new HashMap<String, String>();
    try
    {
      GeoWaveVectorDataProvider.parseDataSourceSettings(keyName + "=" + value + ";", settings);
      Assert.fail("Expected exception");
    }
    catch(IOException e)
    {
      Assert.assertTrue("Wrong message: " + e.getMessage(),
                        e.getMessage().equals("Invalid syntax. No value assignment in \"\""));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testParseTwoSettings() throws IOException
  {
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings("setting1 = value 1;   setting2=  value 2", settings);
    Assert.assertEquals(2, settings.size());
    Iterator<String> iter = settings.keySet().iterator();
    String settingName1 = iter.next();
    String settingName2 = iter.next();
    Assert.assertEquals("setting1", settingName1);
    Assert.assertEquals("setting2", settingName2);
    Assert.assertEquals("value 1", settings.get(settingName1));
    Assert.assertEquals("value 2", settings.get(settingName2));
  }

  @Test
  @Category(UnitTest.class)
  public void testParseMultiSettingsWithQuotes() throws IOException
  {
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings("setting1 = \"value 1\";setting2=\" value 2 \"", settings);
    Assert.assertEquals(2, settings.size());
    Iterator<String> iter = settings.keySet().iterator();
    String settingName1 = iter.next();
    String settingName2 = iter.next();
    Assert.assertEquals("setting1", settingName1);
    Assert.assertEquals("setting2", settingName2);
    Assert.assertEquals("value 1", settings.get(settingName1));
    Assert.assertEquals(" value 2 ", settings.get(settingName2));
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignGeometry() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "spatial=\"POLYGON((10 20, 10 30, 20 30, 20 20, 10 20))\"";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    provider.assignSettings("input", settings);
    Geometry spatial = provider.getSpatialConstraint();
    Assert.assertNull(provider.getCqlFilter());
    Assert.assertNull(provider.getStartTimeConstraint());
    Assert.assertNull(provider.getEndTimeConstraint());

    Assert.assertNotNull(spatial);
    Assert.assertTrue(spatial instanceof Polygon);
    Polygon poly = (Polygon)spatial;
    Coordinate[] coords = poly.getCoordinates();
    Assert.assertEquals(5, coords.length);
    Assert.assertEquals(10.0, coords[0].x, EPSILON);
    Assert.assertEquals(20.0, coords[0].y, EPSILON);
    Assert.assertEquals(10.0, coords[1].x, EPSILON);
    Assert.assertEquals(30.0, coords[1].y, EPSILON);
    Assert.assertEquals(20.0, coords[2].x, EPSILON);
    Assert.assertEquals(30.0, coords[2].y, EPSILON);
    Assert.assertEquals(20.0, coords[3].x, EPSILON);
    Assert.assertEquals(20.0, coords[3].y, EPSILON);
    Assert.assertEquals(10.0, coords[4].x, EPSILON);
    Assert.assertEquals(20.0, coords[4].y, EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignInvalidGeometry() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "spatial=\"POLYGON((10 20, 10 30, 20 30, 20 20))\"";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    try
    {
      provider.assignSettings("input", settings);
      Assert.fail("Expected exception for invalid polygon definition");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertEquals("Wrong message in exception: " + e.getMessage(),
                          "Points of LinearRing do not form a closed linestring", e.getMessage());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignTimeRangeWithTime() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "startTime=2013-05-05T00:00:00.000+08:00;endTime=2013-05-06T00:00:00.000+08";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    provider.assignSettings("input", settings);

    // Make sure the start time was correctly parsed and gives back the correct value.
    // Test the value using GMT so it's always consistent.
    Date startTime = provider.getStartTimeConstraint();
    final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    sdf.setTimeZone(utc);
    String gmtStartTime = sdf.format(startTime);
    Assert.assertEquals("2013-05-04T16:00:00.000 UTC", gmtStartTime);

    // Make sure the end time was correctly parsed and gives back the correct value.
    // Test the value using GMT so it's always consistent.
    Date endTime = provider.getEndTimeConstraint();
    String gmtEndTime = sdf.format(endTime);
    Assert.assertEquals("2013-05-05T16:00:00.000 UTC", gmtEndTime);

    Assert.assertNull(provider.getSpatialConstraint());
    Assert.assertNull(provider.getCqlFilter());
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignTimeRangeWithoutTime() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "startTime=2013-05-05;endTime=2013-05-06";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    provider.assignSettings("input", settings);

    // Make sure the start time was correctly parsed and gives back the correct value.
    // Test the value using GMT so it's always consistent.
    Date startTime = provider.getStartTimeConstraint();
    final String timeFormat = "yyyy-MM-dd";
    final SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
    String formattedStartTime = sdf.format(startTime);
    Assert.assertEquals("2013-05-05", formattedStartTime);

    // Make sure the end time was correctly parsed and gives back the correct value.
    // Test the value using GMT so it's always consistent.
    Date endTime = provider.getEndTimeConstraint();
    String formattedEndTime = sdf.format(endTime);
    Assert.assertEquals("2013-05-06", formattedEndTime);

    Assert.assertNull(provider.getSpatialConstraint());
    Assert.assertNull(provider.getCqlFilter());
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignInvalidStartTime() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "startTime=2013-05-05BAD00:00:00.000+08:00";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    try
    {
      provider.assignSettings("input", settings);
      Assert.fail("Expected exception for bad date format");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
                        e.getMessage().startsWith("Invalid format:"));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignTimeRangeMissingEnd() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "startTime=2013-05-05T00:00:00.000+08:00";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    try
    {
      provider.assignSettings("input", settings);
      Assert.fail("Expected an exception");
    }
    catch(IOException e)
    {
      Assert.assertTrue("Unexpected exception message: " + e.getMessage(),
                        e.getMessage().equals("When querying a GeoWave data source by time," +
                                              " both the start and the end are required."));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignTimeRangeMissingStart() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "endTime=2013-05-05T00:00:00.000+08:00";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    try
    {
      provider.assignSettings("input", settings);
      Assert.fail("Expected an exception");
    }
    catch(IOException e)
    {
      Assert.assertTrue("Unexpected exception message: " + e.getMessage(),
                        e.getMessage().equals("When querying a GeoWave data source by time," +
                                              " both the start and the end are required."));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignInvalidEndTime() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "endTime=2013-05-05BAD00:00:00.000+08:00";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", "input", (ProviderProperties)null);
    try
    {
      provider.assignSettings("input", settings);
      Assert.fail("Expected exception for bad date format");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
                        e.getMessage().startsWith("Invalid format:"));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignBadDateRange() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String strSettings = "startTime=2013-05-06T00:00:00.000+08:00;endTime=2013-05-05T00:00:00.000+08:00";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    String dataSource = "input";
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", dataSource, (ProviderProperties)null);
    try
    {
      provider.assignSettings("input", settings);
      Assert.fail("Expected exception for bad date format");
    }
    catch(IOException e)
    {
      Assert.assertEquals("Unexpected exception message: " + e.getMessage(),
                          e.getMessage(),
                          "For GeoWave data source " + dataSource + ", startDate must be after endDate");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAssignCqlFilter() throws IOException, AccumuloSecurityException, AccumuloException
  {
    String expectedCql = "MyField > 10";
    String strSettings = "cql=\"" + expectedCql + "\"";
    Map<String, String> settings = new TreeMap<String, String>();
    GeoWaveVectorDataProvider.parseDataSourceSettings(strSettings, settings);
    String dataSource = "input";
    GeoWaveVectorDataProvider provider = new GeoWaveVectorDataProvider(null, "geowave", dataSource, (ProviderProperties)null);
    provider.assignSettings("input", settings);
    String cqlFilter = provider.getCqlFilter();
    Assert.assertNotNull(cqlFilter);
    Assert.assertEquals(expectedCql, cqlFilter);
  }
}
