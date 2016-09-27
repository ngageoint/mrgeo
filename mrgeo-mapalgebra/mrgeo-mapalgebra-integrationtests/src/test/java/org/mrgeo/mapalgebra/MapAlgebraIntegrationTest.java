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

package org.mrgeo.mapalgebra;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.File;
import java.io.IOException;

/**
 * @author jason.surratt
 *
 */
public class MapAlgebraIntegrationTest extends LocalRunnerTest
{
// only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
public final static boolean GEN_BASELINE_DATA_ONLY = false;

private static final String smallElevationName = "small-elevation";
private static String smallElevation = Defs.INPUT + smallElevationName;
protected static Path smallElevationPath;

private static final String greeceName = "greece";
private static String greece = Defs.INPUT + greeceName;


public static TestUtils.ValueTranslator nanTranslatorTo255 = new TestUtils.NaNTranslator(255.0f);;
protected static final String pointsName = "input1"; // .tsv
protected static String pointsPath;

private static final String allones = "all-ones";
private static Path allonesPath;
private static final String alltwos = "all-twos";
private static Path alltwosPath;
private static final String allhundreds = "all-hundreds";
private static Path allhundredsPath;
private static final String allhundredsleft = "all-hundreds-shifted-left";
private static Path allhundredsleftPath;
private static final String allhundredshalf = "all-hundreds-shifted-half";
private static Path allhundredshalfPath;
private static final String allhundredsup = "all-hundreds-shifted-up";
private static Path allhundredsupPath;
private static final String allonesnopyramids = "all-ones-no-pyramids";
private static final String allonesholes = "all-ones-with-holes";
private static final String allhundredsholes = "all-hundreds-with-holes";
// the kph-for-small-elevation overlaps small-elevation and all-ones and has some missing
// tiles so we can perform some nodata checks against it.
private static final String kphforsmallelevation = "kph-for-small-elevation";

private static String smallelevationtif = Defs.INPUT + "small-elevation.tif";

private static final String regularpoints = "regular-points";
private static Path regularpointsPath;

private static MapOpTestUtils testUtils;
// Vector private static MapOpTestVectorUtils vectorTestUtils;

private static final Logger log = LoggerFactory.getLogger(MapAlgebraIntegrationTest.class);

//  private static String factor1 = "fs_Bazaars_v2";
//  private static String factor2 = "fs_Bus_Stations_v2";
//  private static String eventsPdfs = "eventsPdfs";

private ProviderProperties providerProperties = null;

@Before
public void setup()
{
  MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, testUtils.getInputHdfs().toUri().toString());
  MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_VECTOR, testUtils.getInputHdfs().toUri().toString());
}

@BeforeClass
public static void init() throws IOException
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    log.warn("***MapAlgebraParserTest TESTS SET TO GENERATE BASELINE IMAGES ONLY***");
  }

  testUtils = new MapOpTestUtils(MapAlgebraIntegrationTest.class);
  //MapOpTestVectorUtils vectorTestUtils = new MapOpTestVectorUtils(MapAlgebraIntegrationTest.class);

  HadoopFileUtils.delete(testUtils.getInputHdfs());

  HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal() + "points"),
      testUtils.getInputHdfs(),
      pointsName + ".tsv");
  HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal() + "points"),
      testUtils.getInputHdfs(),
      pointsName + ".tsv.columns");
  pointsPath = testUtils.getInputHdfsFor(pointsName + ".tsv").toString();

  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allones);
  allonesPath = new Path(testUtils.getInputHdfs(), allones);

  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), alltwos);
  alltwosPath = new Path(testUtils.getInputHdfs(), alltwos);

  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allhundreds);
  allhundredsPath = new Path(testUtils.getInputHdfs(), allhundreds);
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allhundredsleft);
  allhundredsleftPath = new Path(testUtils.getInputHdfs(), allhundredsleft);
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allhundredshalf);
  allhundredshalfPath = new Path(testUtils.getInputHdfs(), allhundredshalf);
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allhundredsup);
  allhundredsupPath = new Path(testUtils.getInputHdfs(), allhundredsup);
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allonesholes);
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allhundredsholes);
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), kphforsmallelevation);

  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), regularpoints);
  regularpointsPath = new Path(testUtils.getInputHdfs(), regularpoints);


  HadoopFileUtils
      .copyToHdfs(new Path(Defs.INPUT), testUtils.getInputHdfs(), smallElevationName);
  smallElevationPath = new Path(testUtils.getInputHdfs(), smallElevationName);


  File file = new File(smallelevationtif);
  smallelevationtif = new Path("file://" + file.getAbsolutePath()).toString();

  file = new File(smallElevation);
  smallElevation = new Path("file://" + file.getAbsolutePath()).toString();

  file = new File(greece);
  greece = new Path("file://" + file.getAbsolutePath()).toString();

}

@Test
@Category(IntegrationTest.class)
public void add() throws Exception
{
//    java.util.Properties prop = MrGeoProperties.getInstance();
//    prop.setProperty(HadoopUtils.IMAGE_BASE, testUtils.getInputHdfs().toUri().toString());
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] + [%s]", allones, allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] + [%s]", allones, allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void addFloatConstant() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] + 3.14", allhundreds), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] + 3.14", allhundreds));

  }
}

// This test shows the limit of floating point math presision.  3,000,000,000 + 100 in floating point
// is actually 3,000,000,000.  Who knew?
@Test
@Category(IntegrationTest.class)
public void add3Billion() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] + 3000000000.0", allhundreds), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] + 3000000000.0", allhundreds));

  }
}

@Test
@Category(IntegrationTest.class)
public void addSubtractConstant() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] + [%s] - 3", allhundreds, allones), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] + [%s] - 3", allhundreds, allones));

  }
}

@Test
@Category(IntegrationTest.class)
public void addSubtractConstantAlternateSyntax() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] + [%s] + -3", allhundreds, allones), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] + [%s] + -3", allhundreds, allones));

  }
}

@Test
@Category(IntegrationTest.class)
public void aspect() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("aspect([%s], \"rad\", 0.0)", smallElevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("aspect([%s], \"rad\", 0.0)", smallElevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void aspectDefaultFlatValue() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("aspect([%s], \"rad\")", smallElevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format("aspect([%s], \"rad\")", smallElevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void aspectNaNFlatValue() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("aspect([%s], \"rad\", \"NaN\")", smallElevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format("aspect([%s], \"rad\", \"NaN\")", smallElevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void aspectDeg() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("aspect([%s], \"deg\", 0.0)", smallElevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("aspect([%s], \"deg\", 0.0)", smallElevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void aspectRad() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("aspect([%s], \"rad\", 0.0)", smallElevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("aspect([%s], \"rad\", 0.0)", smallElevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void aspectFlat() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("aspect([%s], \"rad\", 0.0)", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("aspect([%s], \"rad\", 0.0)", allones));
  }
}


@Test
@Category(IntegrationTest.class)
public void bandcombine() throws Exception
{
  String exp = String.format("bandcombine([%s], [%s])", allones, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void bandcombineAlternate() throws Exception
{
//  LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);

//  String blue = "file:///data/gis-data/images/landsat8/LC80101172015002LGN00_B2-blue.TIF";
//  String green = "file:///data/gis-data/images/landsat8/LC80101172015002LGN00_B3-green.TIF";
//  String red = "file:///data/gis-data/images/landsat8/LC80101172015002LGN00_B4-red.TIF";
//  String exp = String.format("bc(ingest('%s'), ingest('%s'), ingest('%s'))", red, green, blue);

//  String exp = String.format("bc([%s], [%s], [%s])",
//      "/mrgeo/images/landsat-red", "/mrgeo/images/landsat-green", "/mrgeo/images/landsat-blue");

  String exp = String.format("bc([%s], [%s])", allones, allhundreds);
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}


@Test
@Category(IntegrationTest.class)
public void buildpyramid() throws Exception
{
  // copy the pyramid here, in case it has been used in another buildpyramid test
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getOutputHdfs(), allonesnopyramids, true);
  Path path = new Path(testUtils.getOutputHdfs(), allonesnopyramids);

  // make sure the levels don't exist
  MrsPyramidMetadata metadata = testUtils.getImageMetadata(allonesnopyramids);
  MrsPyramidMetadata.ImageMetadata md[] =  metadata.getImageMetadata();

  for (int i = 1; i < md.length; i++)
  {
    MrsPyramidMetadata.ImageMetadata d = md[i];

    if (i != md.length - 1)
    {
      Assert.assertNull("Level name should be missing", d.name);
      Assert.assertNull("Tile Bounds should be missing", d.tileBounds);
      Assert.assertNull("Pixel Bounds should be missing", d.pixelBounds);
      Assert.assertNull("Stats should be missing", d.stats);
    }
    else
    {
      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }
  }


  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("buildPyramid([%s])", path), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("buildPyramid([%s])", path));

    // levels should exist
    metadata = testUtils.getImageMetadata(allonesnopyramids);
    md =  metadata.getImageMetadata();

    for (int i = 1; i < md.length; i++)
    {
      MrsPyramidMetadata.ImageMetadata d = md[i];

      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }

  }
}

@Test
@Category(IntegrationTest.class)
public void buildpyramidAfterSave() throws Exception
{
  // copy the pyramid here, in case it has been used in another buildpyramid test
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getOutputHdfs(), allonesnopyramids, true);
  Path path = new Path(testUtils.getOutputHdfs(), allonesnopyramids);

  // make sure the levels don't exist
  MrsPyramidMetadata metadata = testUtils.getImageMetadata(allonesnopyramids);
  MrsPyramidMetadata.ImageMetadata md[] =  metadata.getImageMetadata();

  for (int i = 1; i < md.length; i++)
  {
    MrsPyramidMetadata.ImageMetadata d = md[i];

    if (i != md.length - 1)
    {
      Assert.assertNull("Level name should be missing", d.name);
      Assert.assertNull("Tile Bounds should be missing", d.tileBounds);
      Assert.assertNull("Pixel Bounds should be missing", d.pixelBounds);
      Assert.assertNull("Stats should be missing", d.stats);
    }
    else
    {
      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }
  }


  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("buildpyramid(save([%s] + 1, \"%s\"))", path, testUtils.getOutputHdfsFor("save-test")), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("buildpyramid(save([%s] + 1, \"%s\"))", path, testUtils.getOutputHdfsFor("save-test")));

    // levels should exist for save-test
    metadata = testUtils.getImageMetadata("save-test");
    md =  metadata.getImageMetadata();

    for (int i = 1; i < md.length; i++)
    {
      MrsPyramidMetadata.ImageMetadata d = md[i];

      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }

    // but not for allones
    metadata = testUtils.getImageMetadata(allonesnopyramids);
    md =  metadata.getImageMetadata();
    for (int i = 1; i < md.length; i++)
    {
      MrsPyramidMetadata.ImageMetadata d = md[i];

      if (i != md.length - 1)
      {
        Assert.assertNull("Level name should be missing", d.name);
        Assert.assertNull("Tile Bounds should be missing", d.tileBounds);
        Assert.assertNull("Pixel Bounds should be missing", d.pixelBounds);
        Assert.assertNull("Stats should be missing", d.stats);
      }
      else
      {
        Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
        Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
        Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
        Assert.assertNotNull("Stats missing", d.stats);
      }
    }

  }
}

@Test
@Category(IntegrationTest.class)
public void buildpyramidAlternate() throws Exception
{
  // copy the pyramid here, in case it has been used in another buildpyramid test
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getOutputHdfs(), allonesnopyramids, true);
  Path path = new Path(testUtils.getOutputHdfs(), allonesnopyramids);

  // make sure the levels don't exist
  MrsPyramidMetadata metadata = testUtils.getImageMetadata(allonesnopyramids);
  MrsPyramidMetadata.ImageMetadata md[] =  metadata.getImageMetadata();

  for (int i = 1; i < md.length; i++)
  {
    MrsPyramidMetadata.ImageMetadata d = md[i];

    if (i != md.length - 1)
    {
      Assert.assertNull("Level name should be missing", d.name);
      Assert.assertNull("Tile Bounds should be missing", d.tileBounds);
      Assert.assertNull("Pixel Bounds should be missing", d.pixelBounds);
      Assert.assertNull("Stats should be missing", d.stats);
    }
    else
    {
      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }
  }


  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("bp([%s])", path), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("bp([%s])", path));

    // levels should exist
    metadata = testUtils.getImageMetadata(allonesnopyramids);
    md =  metadata.getImageMetadata();

    for (int i = 1; i < md.length; i++)
    {
      MrsPyramidMetadata.ImageMetadata d = md[i];

      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }

  }
}

@Test(expected = DataProviderNotFound.class)
@Category(IntegrationTest.class)
public void buildpyramidDoesNotExist() throws Exception
{
  // copy the pyramid here, in case it has been used in another buildpyramid test
  HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getOutputHdfs(), allonesnopyramids, true);
  Path path = new Path(testUtils.getOutputHdfs(), allonesnopyramids);

  // make sure the levels don't exist
  MrsPyramidMetadata metadata = testUtils.getImageMetadata(allonesnopyramids);
  MrsPyramidMetadata.ImageMetadata md[] =  metadata.getImageMetadata();

  for (int i = 1; i < md.length; i++)
  {
    MrsPyramidMetadata.ImageMetadata d = md[i];

    if (i != md.length - 1)
    {
      Assert.assertNull("Level name should be missing", d.name);
      Assert.assertNull("Tile Bounds should be missing", d.tileBounds);
      Assert.assertNull("Pixel Bounds should be missing", d.pixelBounds);
      Assert.assertNull("Stats should be missing", d.stats);
    }
    else
    {
      Assert.assertEquals("Level name incorrect", Integer.toString(i), d.name);
      Assert.assertNotNull("Tile Bounds missing", d.tileBounds);
      Assert.assertNotNull("Pixel Bounds missing", d.pixelBounds);
      Assert.assertNotNull("Stats missing", d.stats);
    }
  }

  testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("buildpyramid([%s] + 1)", path));
}

@Test
@Category(IntegrationTest.class)
public void changeClassification() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        "changeClassification([" + smallElevationPath + "], \"categorical\")", -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        "changeClassification([" + smallElevationPath + "], \"categorical\")");

    MrsPyramidMetadata metadata = testUtils.getImageMetadata(testname.getMethodName());
    Assert.assertEquals(MrsPyramidMetadata.Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("mean", metadata.getResamplingMethod().toLowerCase());
  }
}

@Test
@Category(IntegrationTest.class)
public void changeClassificationAggregator() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        "changeClassification([" + smallElevationPath + "], \"categorical\", \"max\")", -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        "changeClassification([" + smallElevationPath + "], \"categorical\", \"max\")");

    MrsPyramidMetadata metadata = testUtils.getImageMetadata(testname.getMethodName());
    Assert.assertEquals(MrsPyramidMetadata.Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("max", metadata.getResamplingMethod().toLowerCase());

  }
}

@Test
@Category(IntegrationTest.class)
public void complicated() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format(
            "con([%s] > 0.008, 1.0, 0.3) * pow(6, -3.5 * abs(([%s] * 5) + 0.05))",
            smallElevationPath, smallElevationPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format(
            "con([%s] > 0.008, 1.0, 0.3) * pow(6, -3.5 * abs(([%s] * 5) + 0.05))",
            smallElevationPath, smallElevationPath));
  }
}




@Test
@Category(IntegrationTest.class)
public void conAlternateFormat() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format(
            "con([%s] <= 100, [%s], [%s] > 200, [%s], [%s])", smallElevationPath, allones,
            smallElevationPath, allhundreds, alltwos), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format(
            "con([%s] <= 100, [%s], [%s] > 200, [%s], [%s])", smallElevationPath, allones,
            smallElevationPath, allhundreds, alltwos));
  }
}

@Test
@Category(IntegrationTest.class)
public void conLTE() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("con([%s] <= 100, 0, 1)", smallElevationPath), Byte.MAX_VALUE);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("con([%s] <= 100, 0, 1)", smallElevationPath));

  }
}

@Test
@Category(IntegrationTest.class)
public void conNE() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("con([%s] != 200, 2, 0)", smallElevationPath), Byte.MAX_VALUE);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("con([%s] != 200, 2, 0)", smallElevationPath));

  }
}

@Test
@Category(IntegrationTest.class)
public void conLteGte() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("([%s] <= 100) || ([%s] >= 200)", smallElevationPath, smallElevationPath), Byte.MAX_VALUE);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("([%s] <= 100) || ([%s] >= 200)", smallElevationPath, smallElevationPath));
  }

}

@Test
@Category(IntegrationTest.class)
public void conLtGt() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("([%s] < 100) || ([%s] > 200)", smallElevationPath, smallElevationPath), Byte.MAX_VALUE);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("([%s] < 100) || ([%s] > 200)", smallElevationPath, smallElevationPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void cos() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("cos([%s])", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("cos([%s])", allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void crop() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format(
            "crop([%s], 142.05, -17.75, 142.2, -17.65)", smallElevationPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format(
            "crop([%s],  142.05, -17.75, 142.2, -17.65)", smallElevationPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropFromDerivedInput() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format(
                                          "crop([%s] + 0, 142.05, -17.75, 142.2, -17.65)", smallElevationPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format(
                                          "crop([%s] + 0,  142.05, -17.75, 142.2, -17.65)", smallElevationPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropToRaster() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format(
                                          "crop([%s], [%s]);", smallElevationPath, kphforsmallelevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format(
                                          "crop([%s], [%s])", smallElevationPath, kphforsmallelevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropToRasterFromDerivedInput() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format(
                                          "crop([%s] + 0, [%s]);", smallElevationPath, kphforsmallelevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format(
                                          "crop([%s] + 0, [%s])", smallElevationPath, kphforsmallelevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropExact() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format(
            "cropExact([%s],  142.05, -17.75, 142.2, -17.65)",
            smallElevationPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format(
            "cropExact([%s],  142.05, -17.75, 142.2, -17.65)",
            smallElevationPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropExactFromDerivedInput() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format(
                                          "cropExact([%s] + 0,  142.05, -17.75, 142.2, -17.65)",
                                          smallElevationPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format(
                                          "cropExact([%s] + 0,  142.05, -17.75, 142.2, -17.65)",
                                          smallElevationPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropExactToRaster() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format(
                                          "cropExact([%s], [%s]);", smallElevationPath, kphforsmallelevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format(
                                          "cropExact([%s], [%s])", smallElevationPath, kphforsmallelevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void cropExactToRasterFromDerivedInput() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format(
                                          "cropExact([%s] + 0, [%s]);", smallElevationPath, kphforsmallelevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format(
                                          "cropExact([%s] + 0, [%s])", smallElevationPath, kphforsmallelevation));
  }
}

@Test
@Category(IntegrationTest.class)
public void divide() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] / [%s]", allones, allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] / [%s]", allones, allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void divideAddConstant() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] / [%s] + 3", allones, allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] / [%s] + 3", allones, allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void export() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("export([%s], \"%s\", \"true\")", allones, testUtils.getInputLocalFor(testname.getMethodName())), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("export([%s], \"%s\", \"true\")", allones, testUtils.getOutputLocalFor(testname.getMethodName())));

    // now check the file that was saved...
    testUtils.compareLocalRasterOutput(testname.getMethodName(), TestUtils.nanTranslatorToMinus9999);
  }
}

@Test
@Category(IntegrationTest.class)
public void exportInMemory() throws Exception
{

  ExportMapOp.inMemoryTestPath_$eq(testUtils.getOutputLocalFor(testname.getMethodName()));
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("export([%s], \"%s\", \"true\")", allones, ExportMapOp.IN_MEMORY()), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("export([%s], \"%s\", \"true\")", allones, ExportMapOp.IN_MEMORY()));


    // now check the file that was saved...
    testUtils.compareLocalRasterOutput(testname.getMethodName(), TestUtils.nanTranslatorToMinus9999);

  }
}

@Test
@Category(IntegrationTest.class)
public void expressionIncompletePathInput1() throws Exception
{
  // test expressions with file names mixed in with fullpaths
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("a = [%s]; b = 3; a / [%s] + b", allones, allonesPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("a = [%s]; b = 3; a / [%s] + b", allones, allonesPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void expressionIncompletePathInput2() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("a = [%s];\nb = 3;\na / [%s] + b", allones, allonesPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("a = [%s];\nb = 3;\na / [%s] + b", allones, allonesPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void expressionIncompletePathInput3() throws Exception
{
  // paths with file extensions
  final String fname = allonesPath.getName();
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        "a = [" + fname + "] + "
            + "[" + allonesPath.toString() + "];", -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        "a = [" + fname + "] + "
            + "[" + allonesPath.toString() + "];");
  }
}

@Test
@Category(IntegrationTest.class)
public void fillConst() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        "fill([" + allhundredsholes + "], 1)", -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        "fill([" + allhundredsholes + "], 1)");
  }
}

@Test
@Category(IntegrationTest.class)
public void fillImage() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        "fill([" + allhundredsholes + "], [" + allonesholes + "])", -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        "fill([" + allhundredsholes + "], [" + allonesholes + "])");
  }
}

// Run a fill for the upper right corner of the image. This tests
// that the fill code properly handles splits that begin with tiles
// outside of the crop bounds.
@Test
@Category(IntegrationTest.class)
public void fillBounds() throws Exception
{

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        "fillBounds([" + allhundredsholes + "], 1, 141.6, -18.6, 143.0, -17.0)", -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        "fillBounds([" + allhundredsholes + "], 1, 141.6, -18.6, 143.0, -17.0)");
  }
}

@Test
@Category(IntegrationTest.class)
public void fillToRasterBounds() throws Exception
{

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("fillBounds([%s], -1, [%s])",
                                                kphforsmallelevation, allhundreds), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format("fillBounds([%s], -1, [%s])",
                                                kphforsmallelevation, allhundreds));
    // Make sure that the output nodata value is 255 (the value
    // returned from RasterUtils.getDefaultNoDataForType with DataBuffer.TYPE_BYTE
    MrsPyramid pyramid = MrsPyramid.open((new Path(testUtils.getOutputHdfs(), testname.getMethodName())).toString(), providerProperties);
    Assert.assertNotNull("Unable to load output image", pyramid);
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Unable to load output image metadata", metadata);
    // Check the bounds in the metadata - should match allhundreds
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    Assert.assertNotNull("Unable to get tile bounds for max zoom", tb);
    Assert.assertEquals(915, tb.getMinX());
    Assert.assertEquals(917, tb.getMaxX());
    Assert.assertEquals(203, tb.getMinY());
    Assert.assertEquals(206, tb.getMaxY());
    // Make sure the output type is the same as the input type
    MrsPyramid inputPyramid = MrsPyramid.open((new Path(testUtils.getOutputHdfs(), testname.getMethodName())).toString(), providerProperties);
    Assert.assertNotNull("Unable to load input image", inputPyramid);
    MrsPyramidMetadata inputMetadata = inputPyramid.getMetadata();
    Assert.assertNotNull("Unable to load input image metadata", inputMetadata);
    Assert.assertEquals("Wrong number of bands", inputMetadata.getBands(), metadata.getBands());
    for (int b = 0; b < inputMetadata.getBands(); b++)
    {
      Assert.assertEquals("Unexpected nodata value in band " + b, inputMetadata.getDefaultValue(b),
                          metadata.getDefaultValue(b), 1e-8);
    }
    Assert.assertEquals(inputMetadata.getTileType(), metadata.getTileType());
  }
}

@Ignore
@Test
@Category(IntegrationTest.class)
public void hillshadeNonLocal() throws Exception
{
  Configuration config = HadoopUtils.createConfiguration();

  double zen = 30.0 * 0.0174532925; // sun 30 deg above the horizon
  double sunaz = 270.0 * 0.0174532925; // sun from 270 deg (west)

  double coszen = Math.cos(zen);
  double sinzen = Math.sin(zen);

  // hillshading algorithm taken from:
  // http://edndoc.esri.com/arcobjects/9.2/net/shared/geoprocessing/spatial_analyst_tools/how_hillshade_works.htm
  String exp = String.format("sl = slope([%s], \"rad\"); " +
          "as = aspect([%s], \"rad\", 0.0); " +
          "hill = 255.0 * ((%f * cos(sl)) + (%f * sin(sl) * cos(%f - as)))", smallElevation, smallElevation,
      coszen, sinzen, sunaz);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(config, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(config, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        exp);
  }
}


@Test
@Category(IntegrationTest.class)
public void hillshade() throws Exception
{
  double zen = 30.0 * 0.0174532925; // sun 30 deg above the horizon
  double sunaz = 270.0 * 0.0174532925; // sun from 270 deg (west)

  double coszen = Math.cos(zen);
  double sinzen = Math.sin(zen);

  // hillshading algorithm taken from:
  // http://edndoc.esri.com/arcobjects/9.2/net/shared/geoprocessing/spatial_analyst_tools/how_hillshade_works.htm
  String exp = String.format("sl = slope([%s], \"rad\"); " +
          "as = aspect([%s], \"rad\", 0.0); " +
          "hill = 255.0 * ((%f * cos(sl)) + (%f * sin(sl) * cos(%f - as)))", smallElevation, smallElevation,
      coszen, sinzen, sunaz);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void ingest() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("ingest(\"%s\")", smallelevationtif), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("ingest(\"%s\")", smallelevationtif));
  }
}

@Test
@Category(IntegrationTest.class)
public void isNodata() throws Exception
{
//    java.util.Properties prop = MrGeoProperties.getInstance();
//    prop.setProperty(HadoopUtils.IMAGE_BASE, testUtils.getInputHdfs().toUri().toString());
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("isNodata([%s])", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("isNodata([%s])", allones));
  }
}
@Test
@Category(IntegrationTest.class)
public void isNull() throws Exception
{
//    java.util.Properties prop = MrGeoProperties.getInstance();
//    prop.setProperty(HadoopUtils.IMAGE_BASE, testUtils.getInputHdfs().toUri().toString());
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("isNull([%s])", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("isNull([%s])", allones));
  }
}


@Test
@Category(IntegrationTest.class)
public void isNodataWithMissingTiles() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("isNodata([%s])", kphforsmallelevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format("isNodata([%s])", kphforsmallelevation));
    // Make sure that the output nodata value is 255 (the value
    // returned from RasterUtils.getDefaultNoDataForType with DataBuffer.TYPE_BYTE
    MrsPyramid pyramid = MrsPyramid.open((new Path(testUtils.getOutputHdfs(), testname.getMethodName())).toString(), providerProperties);
    Assert.assertNotNull("Unable to load output image", pyramid);
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Unable to load output image metadata", metadata);
    Assert.assertEquals("Wrong number of bands", 1, metadata.getBands());
    Assert.assertEquals("Unexpected nodata value", 255.0, metadata.getDefaultValue(0), 1e-8);
    // Check the tile bounds in the metadata
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    Assert.assertNotNull("Unable to get tile bounds for max zoom", tb);
    Assert.assertEquals(915, tb.getMinX());
    Assert.assertEquals(917, tb.getMaxX());
    Assert.assertEquals(203, tb.getMinY());
    Assert.assertEquals(205, tb.getMaxY());
    Assert.assertEquals(DataBuffer.TYPE_BYTE, metadata.getTileType());
  }
}

@Test
@Category(IntegrationTest.class)
public void isNodataWithBoundsRaster() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("isNodataBounds([%s], [%s])",
                                                kphforsmallelevation, allhundreds), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format("isNodataBounds([%s], [%s])", kphforsmallelevation, allhundreds));
    // Make sure that the output nodata value is 255 (the value
    // returned from RasterUtils.getDefaultNoDataForType with DataBuffer.TYPE_BYTE
    MrsPyramid pyramid = MrsPyramid.open((new Path(testUtils.getOutputHdfs(), testname.getMethodName())).toString(), providerProperties);
    Assert.assertNotNull("Unable to load output image", pyramid);
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Unable to load output image metadata", metadata);
    Assert.assertEquals("Wrong number of bands", 1, metadata.getBands());
    Assert.assertEquals("Unexpected nodata value", 255.0, metadata.getDefaultValue(0), 1e-8);
    // Check the bounds in the metadata - should match allhundreds
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    Assert.assertNotNull("Unable to get tile bounds for max zoom", tb);
    Assert.assertEquals(915, tb.getMinX());
    Assert.assertEquals(917, tb.getMaxX());
    Assert.assertEquals(203, tb.getMinY());
    Assert.assertEquals(206, tb.getMaxY());
    Assert.assertEquals(DataBuffer.TYPE_BYTE, metadata.getTileType());
  }
}

@Test
@Category(IntegrationTest.class)
public void isNodataWithBounds() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("isNodataBounds([%s], 141.719, -18.247, 142.617, -17.264)",
                                                kphforsmallelevation), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                  String.format("isNodataBounds([%s], 141.719, -18.247, 142.617, -17.264)",
                                                kphforsmallelevation));
    // Make sure that the output nodata value is 255 (the value
    // returned from RasterUtils.getDefaultNoDataForType with DataBuffer.TYPE_BYTE
    MrsPyramid pyramid = MrsPyramid.open((new Path(testUtils.getOutputHdfs(), testname.getMethodName())).toString(), providerProperties);
    Assert.assertNotNull("Unable to load output image", pyramid);
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Unable to load output image metadata", metadata);
    Assert.assertEquals("Wrong number of bands", 1, metadata.getBands());
    Assert.assertEquals("Unexpected nodata value", 255.0, metadata.getDefaultValue(0), 1e-8);
    // Check the bounds in the metadata - should match allhundreds
    LongRectangle tb = metadata.getTileBounds(metadata.getMaxZoomLevel());
    Assert.assertNotNull("Unable to get tile bounds for max zoom", tb);
    Assert.assertEquals(915, tb.getMinX());
    Assert.assertEquals(917, tb.getMaxX());
    Assert.assertEquals(204, tb.getMinY());
    Assert.assertEquals(206, tb.getMaxY());
    Assert.assertEquals(DataBuffer.TYPE_BYTE, metadata.getTileType());
  }
}

@Test
@Category(IntegrationTest.class)
public void kernelGaussian() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("kernel(\"gaussian\", [%s], 100.0)", regularpointsPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("kernel(\"gaussian\", [%s], 100.0)", regularpointsPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void kernelGaussian2k() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("kernel(\"gaussian\", [%s], 2000.0)", regularpointsPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("kernel(\"gaussian\", [%s], 2000.0)", regularpointsPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void kernelLaplacian() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("kernel(\"laplacian\", [%s], 100.0)", regularpointsPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("kernel(\"laplacian\", [%s], 100.0)", regularpointsPath));
  }
}


@Test
@Category(IntegrationTest.class)
public void log() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("log([%s])", alltwos), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("log([%s])", alltwos));

  }
}

@Test
@Category(IntegrationTest.class)
public void logWithBase() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("log([%s], 10)", alltwos), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("log([%s], 10)", alltwos));

  }
}

@Test
@Category(IntegrationTest.class)
public void mosaicOverlapHundredsTop() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("mosaic([%s], [%s])", allhundredsPath, alltwosPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("mosaic([%s], [%s])", allhundredsPath, alltwosPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void mosaicOverlapTwosTop() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredsPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredsPath));
  }
}
@Test
@Category(IntegrationTest.class)
public void mosaicButtedLeft() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredsleftPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredsleftPath));
  }
}
@Test
@Category(IntegrationTest.class)
public void mosaicButtedTop() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredsupPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredsupPath));
  }
}
@Test
@Category(IntegrationTest.class)
public void mosaicPartialOverlapTwosTop() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredshalfPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("mosaic([%s], [%s])", alltwosPath, allhundredshalfPath));
  }
}
@Test
@Category(IntegrationTest.class)
public void mosaicPartialOverlapHundredsTop() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("mosaic([%s], [%s])", allhundredshalfPath, alltwosPath), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("mosaic([%s], [%s])", allhundredshalfPath, alltwosPath));
  }
}

@Test
@Category(IntegrationTest.class)
public void mult() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] * 5", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] * 5", allones));

  }
}

@Test
@Category(IntegrationTest.class)
public void nestedExpression() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("pow(6, -3.5 * abs(([%s] * 5) + 0.05))", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("pow(6, -3.5 * abs(([%s] * 5) + 0.05))", allones));

  }
}

@Test
@Category(IntegrationTest.class)
public void not() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("!([%s] < 0.012)", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("!([%s] < 0.012)", allones));

  }
}

@Test
@Category(IntegrationTest.class)
public void orderOfOperations() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {

    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("[%s] + [%s] * [%s] - [%s]", allones, allones, allones, allones), -9999);
  }
  else
  {

    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("[%s] + [%s] * [%s] - [%s]", allones, allones, allones, allones));
  }
}

@Test
@Category(UnitTest.class)
public void parse1() throws Exception
{
  final String ex = String.format("[%s] + [%s]", smallElevation, smallElevation);
  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(UnitTest.class)
public void parse2() throws Exception
{
  final String ex = String.format("[%s] * 15", smallElevation);
  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(UnitTest.class)
public void parse3() throws Exception
{
  final String ex = String.format("log([%s])", smallElevation);
  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void parse4() throws Exception
{
  final String ex = String.format("log([%s/abc])", testUtils.getInputHdfs().toString());

  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void parse5() throws Exception
{
  final String ex = String.format("[%s] * 15", testUtils.getInputHdfs().toString());

  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void parseInvalidArguments1() throws Exception
{
  final String ex = String.format("[%s] + ", smallElevation);

  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void parseInvalidArguments2() throws Exception
{
  final String ex = String.format("abs [%s] [%s] ", smallElevation, smallElevation);

  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void parseInvalidArguments3() throws Exception
{
  final String ex = String.format("con[%s] + ", smallElevation);

  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void parseInvalidArguments4() throws Exception
{
  final String ex = "costDistance";
  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(UnitTest.class)
public void parseInvalidOperation() throws Exception
{
  final String ex = String.format("[%s] & [%s]", smallElevation, smallElevation);
  MapAlgebra.validateWithExceptions(ex, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(IntegrationTest.class)
public void pow() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("pow([%s], 1.2)", allhundreds), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("pow([%s], 1.2)", allhundreds));

  }
}


@Test
@Category(IntegrationTest.class)
public void save() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("save([%s], \"%s\")", allones, testUtils.getOutputHdfsFor("save-test")), -9999);

    testUtils.saveBaselineTif("save-test", -9999.0);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("save([%s], \"%s\")", allones, testUtils.getOutputHdfsFor("save-test")));

    // now check the file that was saved...
    testUtils.compareRasterOutput("save-test", TestUtils.nanTranslatorToMinus9999);
  }
}

@Test
@Category(IntegrationTest.class)
public void sin() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("sin([%s] / 0.01)", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("sin([%s] / 0.01)", allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void slope() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("slope([%s])", smallElevation), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("slope([%s])", smallElevation));

  }
}

@Test
@Category(IntegrationTest.class)
public void slopeGradient() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("slope([%s], \"gradient\")", smallElevation), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("slope([%s], \"gradient\")", smallElevation));

  }
}
@Test
@Category(IntegrationTest.class)
public void slopeRad() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("slope([%s], \"rad\")", smallElevation), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("slope([%s], \"rad\")", smallElevation));

  }
}
@Test
@Category(IntegrationTest.class)
public void slopeDeg() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("slope([%s], \"deg\")", smallElevation), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("slope([%s], \"deg\")", smallElevation));

  }
}

@Test
@Category(IntegrationTest.class)
public void slopePercent() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("slope([%s], \"percent\")", smallElevation), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("slope([%s], \"percent\")", smallElevation));

  }
}

@Test
@Category(IntegrationTest.class)
public void slope8() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("slope8([%s])", smallElevation), -9999);

  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("slope8([%s])", smallElevation));

  }
}


@Test
@Category(IntegrationTest.class)
public void statisticsCount() throws Exception
{
  String exp = String.format("statistics('count', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsMax() throws Exception
{
  String exp = String.format("statistics('max', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsMean() throws Exception
{
  String exp = String.format("statistics('mean', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsMedian() throws Exception
{
  String exp = String.format("statistics('median', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsMin() throws Exception
{
  String exp = String.format("statistics('min', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsMode() throws Exception
{
  String exp = String.format("statistics('mode', [%s], [%s], [%s], [%s])", allones, alltwos, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsSum() throws Exception
{
  String exp = String.format("statistics('sum', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsStdDev() throws Exception
{
  String exp = String.format("statistics('stddev', [%s], [%s], [%s])", allones, alltwos, allhundreds);

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void statisticsWildcard() throws Exception
{
  String exp = String.format("statistics('mean', '%s')", "all*");

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);
  }
}


@Test
@Category(IntegrationTest.class)
public void tan() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("tan([%s] / 0.01)", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("tan([%s] / 0.01)", allones));

  }
}


@Test
@Category(IntegrationTest.class)
public void variables1() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("a = [%s]; b = a; a + b * a - b", allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("a = [%s]; b = a; a + b * a - b", allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void variables2() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("\n\na = [%s];\n\nb = 3;\na \t + [%s] - b\n\n", allones, allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("\n\na = [%s];\n\nb = 3;\na \t+ [%s] - b\n\n", allones, allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void variables3() throws Exception
{
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
        String.format("a = [%s]; b = 3; a / [%s] + b", allones, allones), -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        String.format("a = [%s]; b = 3; a / [%s] + b", allones, allones));
  }
}

@Test
@Category(IntegrationTest.class)
public void testDataTypeByte() throws Exception
{
  String exp = String.format("[%s] gt 200", smallElevationPath);
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, Byte.MAX_VALUE);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        exp);

    checkDataTypes(DataBuffer.TYPE_BYTE);
  }
}


@Test
@Category(IntegrationTest.class)
public void testDataTypeFloat() throws Exception
{
  String exp = String.format("([%s] lt 200) + 500", smallElevationPath);
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
        exp);

    checkDataTypes(DataBuffer.TYPE_FLOAT);
  }
}

  @Test
  @Category(IntegrationTest.class)
  public void tpi() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("tpi([%s])", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("tpi([%s])", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatMin() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"min\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"min\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatMax() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"max\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"max\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatMean() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"mean\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"mean\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatMedian() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"median\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"median\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatRange() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"range\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"range\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatStdDev() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"stddev\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"stddev\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatVariance() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
    testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                  String.format("focalStat(\"variance\", [%s], \"3p\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"variance\", [%s], \"3p\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatSumWithNoData() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"sum\", [%s], \"3p\", \"true\")", allonesholes), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"sum\", [%s], \"3p\", \"true\")", allonesholes));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatSumWithoutIgnoreNoData() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"sum\", [%s], \"3p\", \"false\")", allonesholes), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"sum\", [%s], \"3p\", \"false\")", allonesholes));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatMinMetersEvenNeighborhood() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"min\", [%s], \"300m\", \"true\")", smallElevation), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"min\", [%s], \"300m\", \"true\")", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatSumEvenNeighborhood() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"sum\", [%s], \"4p\", \"true\")", allonesholes), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"sum\", [%s], \"4p\", \"true\")", allonesholes));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatCountEvenNeighborhood() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"count\", [%s], \"4p\", \"true\")", allonesholes), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"count\", [%s], \"4p\", \"true\")", allonesholes));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void focalStatCountEvenNeighborhoodWithoutIgnore() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
                                    String.format("focalStat(\"count\", [%s], \"4p\", \"false\")", allonesholes), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999,
                                    String.format("focalStat(\"count\", [%s], \"4p\", \"false\")", allonesholes));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void bitwiseXor() throws Exception
  {
    String stmt = String.format("convert([%s], \"byte\", \"truncate\") ^ (0x80 | 0x60)", allhundredsholes);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(), stmt, 255);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    nanTranslatorTo255, nanTranslatorTo255,
                                    stmt);
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void bitwiseAnd() throws Exception
  {
    String stmt = String.format("convert([%s], \"byte\", \"truncate\") & (0x80 | 0x60)", allhundredsholes);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(), stmt, 255);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    nanTranslatorTo255, nanTranslatorTo255,
                                    stmt);
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void bitwiseOr() throws Exception
  {
    String stmt = String.format("convert([%s], \"byte\", \"truncate\") | 0x80", allhundredsholes);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(), stmt, 255);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    nanTranslatorTo255, nanTranslatorTo255,
                                    stmt);
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void bitwiseComplement() throws Exception
  {
    String stmt = String.format("~convert([%s], \"byte\", \"truncate\")", allhundredsholes);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(), stmt, 255);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                    nanTranslatorTo255, nanTranslatorTo255,
                                    stmt);
    }
  }

  private void checkDataTypes(int type) throws IOException
{
  MrsPyramid
      pyramid = MrsPyramid.open(testUtils.getOutputHdfsFor(testname.getMethodName()).toString(), providerProperties);
  org.junit.Assert.assertNotNull("Can't load pyramid", pyramid);

  MrsPyramidMetadata metadata = pyramid.getMetadata();
  org.junit.Assert.assertNotNull("Can't load metadata", metadata);

  org.junit.Assert.assertEquals("Bad tile type", type, metadata.getTileType());

  MrsImage image = pyramid.getImage(metadata.getMaxZoomLevel());
  org.junit.Assert.assertNotNull("Can't load image", image);

  KVIterator<TileIdWritable, MrGeoRaster> iter = image.getTiles();
  while (iter.hasNext())
  {
    MrGeoRaster raster = iter.currentValue();
    org.junit.Assert.assertEquals("Bad tile type", type, raster.datatype());

    iter.next();
  }
}


}
