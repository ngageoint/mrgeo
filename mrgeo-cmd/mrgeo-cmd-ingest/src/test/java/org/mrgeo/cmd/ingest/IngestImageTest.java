/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.cmd.ingest;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.logging.LoggingUtils;
import org.mrgeo.utils.LongRectangle;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class IngestImageTest
{
  @Rule public TestName testname = new TestName();

// only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
public final static boolean GEN_BASELINE_DATA_ONLY = false;

  private static TestUtils testUtils;

  private static String input;
  private static Path inputHdfs;
  private static Path outputHdfs;

  private final static String all_ones = "all-ones";
  private String all_ones_input = Defs.INPUT + all_ones + ".tif";
  private String all_ones_output;
  private final static String aster_sample = "AsterSample";

  private static Configuration conf;
  private ProviderProperties providerProperties;
  private static String origProtectionLevelRequired;
  private static String origProtectionLevelDefault;
  private static String origProtectionLevel;

  @BeforeClass
  public static void init() throws IOException
  {
    LoggingUtils.setDefaultLogLevel(LoggingUtils.ERROR);

    Properties props = MrGeoProperties.getInstance();
    origProtectionLevelRequired = props.getProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED);
    origProtectionLevelDefault = props.getProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT);
    origProtectionLevel = props.getProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL);
    conf = HadoopUtils.createConfiguration();

    testUtils = new TestUtils(IngestImageTest.class);

    input = testUtils.getInputLocal();
    inputHdfs = testUtils.getInputHdfs();
    outputHdfs = testUtils.getOutputHdfs();

    HadoopFileUtils.delete(inputHdfs);

    // copy test files up to HDFS
    //HadoopFileUtils.copyToHdfs(input, inputHdfs, "greece.tif");

    HadoopFileUtils.copyToHdfs(input, inputHdfs, aster_sample);

  }

  @After
  public void teardown()
  {
    // Restore MrGeoProperties
    Properties props = MrGeoProperties.getInstance();
    if (origProtectionLevelRequired == null)
    {
      props.remove(MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED);
    }
    else
    {
      props.setProperty( MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED, origProtectionLevelRequired);
    }

    if (origProtectionLevelDefault == null)
    {
      props.remove(MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT);
    }
    else
    {
      props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT, origProtectionLevelDefault);
    }

    if (origProtectionLevel == null)
    {
      props.remove(MrGeoConstants.MRGEO_PROTECTION_LEVEL);
    }
    else
    {
      props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL, origProtectionLevel);
    }
  }

  @Before 
  public void setUp()
  {
    providerProperties = null;

    File file = new File(all_ones_input);
    all_ones_input = "file://" + file.getAbsolutePath();

    // tack on the test name to the output
    all_ones_output = new Path(outputHdfs, testname.getMethodName()).toString();
  }

  @Test
  @Category(IntegrationTest.class)
  public void ingestSimple() throws Exception
  {
    String[] args = { all_ones_input, "-l","-o", all_ones_output };
    int res = new IngestImage().run(args, conf, providerProperties);

    Assert.assertEquals("IngestImage command exited with error", 0, res);
    
    // now look at the files built.  We really not interested in the actual data, just that
    // things were build. (this is testing the command, not the algorithms)
    MrsPyramid pyramid = MrsPyramid.open(all_ones_output, providerProperties);
    Assert.assertNotNull("MrsPyramid not loaded", pyramid);
    
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("MrsPyramid metadata not loaded", metadata);
    Assert.assertEquals("", metadata.getProtectionLevel());
    
    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());
    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
    
    // check that we ingested the right number of tiles - in particular, that our maxTx/maxTy  
    // is inclusive  
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    long numTiles = (tb.getMaxX() - tb.getMinX() + 1) * (tb.getMaxY() - tb.getMinY() + 1);
    Assert.assertEquals("Wrong number of tiles", 12L, numTiles);

    testUtils.compareRasterToConstant(testname.getMethodName(), 1.0);
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void ingestSimpleWithDefaultProtectionLevel() throws Exception
  {
    String protectionLevel = "public";
    Properties props = MrGeoProperties.getInstance();
    props.setProperty( MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED, "true");
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT, protectionLevel);
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL, "");
    String[] args = { all_ones_input, "-l","-o", all_ones_output };
    int res = new IngestImage().run(args, conf, providerProperties);

    Assert.assertEquals("IngestImage command exited with error", 0, res);
    
    // now look at the files built.  We really not interested in the actual data, just that
    // things were build. (this is testing the command, not the algorithms)
    MrsPyramid pyramid = MrsPyramid.open(all_ones_output, providerProperties);
    Assert.assertNotNull("MrsPyramid not loaded", pyramid);
    
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("MrsPyramid metadata not loaded", metadata);
    Assert.assertEquals(protectionLevel, metadata.getProtectionLevel());
    
    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());
    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
    
    // check that we ingested the right number of tiles - in particular, that our maxTx/maxTy  
    // is inclusive  
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    long numTiles = (tb.getMaxX() - tb.getMinX() + 1) * (tb.getMaxY() - tb.getMinY() + 1);
    Assert.assertEquals("Wrong number of tiles", 12L, numTiles);

    testUtils.compareRasterToConstant(testname.getMethodName(), 1.0);

  }
  
  @Test
  @Category(IntegrationTest.class)
  public void ingestSimpleWithProtectionLevel() throws Exception
  {
    String protectionLevel = "private";
    Properties props = MrGeoProperties.getInstance();
    props.setProperty( MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED, "true");
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT, "public");
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL, "");
    String[] args = { all_ones_input, "-l","-o", all_ones_output, "-pl", protectionLevel };
    int res = new IngestImage().run(args, conf, providerProperties);

    Assert.assertEquals("IngestImage command exited with error", 0, res);
    
    // now look at the files built.  We really not interested in the actual data, just that
    // things were build. (this is testing the command, not the algorithms)
    MrsPyramid pyramid = MrsPyramid.open(all_ones_output, providerProperties);
    Assert.assertNotNull("MrsPyramid not loaded", pyramid);
    
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("MrsPyramid metadata not loaded", metadata);
    Assert.assertEquals(protectionLevel, metadata.getProtectionLevel());
    
    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());
    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
    
    // check that we ingested the right number of tiles - in particular, that our maxTx/maxTy  
    // is inclusive  
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    long numTiles = (tb.getMaxX() - tb.getMinX() + 1) * (tb.getMaxY() - tb.getMinY() + 1);
    Assert.assertEquals("Wrong number of tiles", 12L, numTiles);

    testUtils.compareRasterToConstant(testname.getMethodName(), 1.0);
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void ingestSkipPyramids() throws Exception
  {
    String[] args = { all_ones_input, "-o", all_ones_output, "-sp" };
    int res= new IngestImage().run(args, conf, providerProperties);

    Assert.assertEquals("IngestImage command exited with error", 0, res);
    
    // now look at the files built.  We really not interested in the actual data, just that
    // things were build. (this is testing the command, not the algorithms)
    MrsPyramid pyramid = MrsPyramid.open(all_ones_output, providerProperties);
    Assert.assertNotNull("MrsPyramid not loaded", pyramid);
    
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("MrsPyramid metadata not loaded", metadata);
    
    Assert.assertEquals("Wrong max zoom level", 10, metadata.getMaxZoomLevel());
    
    MrsImage image = pyramid.getImage(metadata.getMaxZoomLevel());
    Assert.assertNotNull("MrsImage image missing for level " + metadata.getMaxZoomLevel(), image);
    image.close();
    
    for (int level = metadata.getMaxZoomLevel() - 1; level >= 1; level--)
    {
      image = pyramid.getImage(level);
      Assert.assertNull("MrsImage found for level " + level, image);
    }

    testUtils.compareRasterToConstant(testname.getMethodName(), 1.0);
  }

  @Test
  @Category(IntegrationTest.class)
  public void ingestMissingDefaultProtectionLevel() throws Exception
  {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream saveOut = System.out;
    System.setOut(new PrintStream(outContent));
    Properties props = MrGeoProperties.getInstance();
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED, "true");
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT, "");
    props.setProperty(MrGeoConstants.MRGEO_PROTECTION_LEVEL, "");
    String[] args = { all_ones_input, "-o", all_ones_output, "-sp" };
    int res = new IngestImage().run(args, conf, providerProperties);
    Assert.assertEquals(-1, res);
    Assert.assertTrue("Unexpected output: " + outContent.toString(),
        outContent.toString().contains("Missing required option: pl"));

  }

  @Test
  @Category(IntegrationTest.class)
  public void ingestAster() throws Exception
  {
    int zoom = 6;

    String inputAster = new Path(inputHdfs, aster_sample).toString();
    String outputAster = new Path(outputHdfs, testname.getMethodName()).toString();
    String[] args = { inputAster, "-o", outputAster, "-sp" , "-nd" , "-32767", "-sk", "-z", Integer.toString(zoom)};
    int res= new IngestImage().run(args, conf, providerProperties);

    Assert.assertEquals("IngestImage command exited with error", 0, res);
    
    MrsPyramid pyramid = MrsPyramid.open(outputAster, providerProperties);
    Assert.assertNotNull("MrsPyramid not loaded", pyramid);
    
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("MrsPyramid metadata not loaded", metadata);
    
    Assert.assertEquals("Wrong max zoom level", zoom, metadata.getMaxZoomLevel());
    
    MrsImage image = pyramid.getImage(metadata.getMaxZoomLevel());
    Assert.assertNotNull("MrsImage image missing for level " + metadata.getMaxZoomLevel(), image);
    image.close();
    
    for (int level = metadata.getMaxZoomLevel() - 1; level >= 1; level--)
    {
      image = pyramid.getImage(level);
      Assert.assertNull("MrsImage found for level " + level, image);
    }

    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.saveBaselineTif(testname.getMethodName());
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName());
    }

  }

}
