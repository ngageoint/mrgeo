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

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.ImageStats;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.mrgeo.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * testCostDistance gets the full area, which is 13x12 tiles 
 * testCostDistanceWithBounds gets a clipped area of 12x11 tiles, and uses boundsBuffer
 * testCostDistanceWithBoundsAndLowerZoomLevel gets a clipped area of zoom 9 - 7x6 instead of 7x7
 *  and uses boundsBuffer
 * testCostDistanceWithUniformFriction gets a clipped area of 2x2 instead of 3x4 and uses  
 *  auto bounds
 * testCostDistanceWithLeastCostPath uses auto bounds (not sure what clipped area looks like because
 *  we don't store it)
 * testNnd uses auto bounds for a clipped area of 12x11 (just as large as testCostDistanceWithBounds 
 *  but different set of 12x11 tiles)
 *
 */
@SuppressWarnings("static-method")
public class CostDistanceMapOpIntegrationTest extends LocalRunnerTest
{
  private static final Logger log = LoggerFactory.getLogger(CostDistanceMapOpIntegrationTest.class);

  private static MapOpTestUtils testUtils;

  // least cost path output is a vector, use those test utils as well...
  private static MapOpTestVectorUtils vectorutils;

  // only set this to true to generate new baseline images after correcting tests; image comparison
  // tests won't be run when is set to true
  public final static boolean GEN_BASELINE_DATA_ONLY = false;

  private static final String ALL_ONES = "all-ones";
  private static final int ALL_ONES_ZOOM = 10;

//  private static final String TOBLER_MEDIUM = "tobler-raw-4tiles";
  private static final String TOBLER_MEDIUM = "tobler-raw-medium";
//  private static final String TOBLER_MEDIUM = "tobler-raw-9tiles";
  private static final int TOBLER_MEDIUM_ZOOM = 10;
  private static String frictionSurface;

private static final String smallElevationName = "small-elevation";

  @BeforeClass
  public static void init() throws IOException
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      log.warn("*** TESTS SET TO GENERATE BASELINE IMAGES ONLY***");
    }

    vectorutils = new MapOpTestVectorUtils(CostDistanceMapOpIntegrationTest.class);
    testUtils = new MapOpTestUtils(CostDistanceMapOpIntegrationTest.class);

    HadoopFileUtils.delete(testUtils.getInputHdfs());

    HadoopFileUtils.copyToHdfs(testUtils.getInputLocal(), testUtils.getInputHdfs(), TOBLER_MEDIUM);
    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), ALL_ONES);

    frictionSurface = testUtils.getInputHdfs() + "/" +  TOBLER_MEDIUM;

    HadoopFileUtils
        .copyToHdfs(new Path(Defs.INPUT), testUtils.getInputHdfs(), smallElevationName);
  }

  @Test
  @Category(IntegrationTest.class)
  public void testCostDistance() throws Exception
  {
    // for tobler-raw-tiny
//    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(-117.5 38.5)'\");\n"
//        + "friction = [" + frictionSurface + "];\n"
//        + "result = CostDistance(src, " + TOBLER_MEDIUM_ZOOM + ", friction, \"50000.0\");";
    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(67.1875 32.38)'\");\n"
                 + "friction = [" + frictionSurface + "];\n"
                 + "result = CostDistance(src, " + TOBLER_MEDIUM_ZOOM + ", friction, \"50000.0\");";
    // Start point in tile tx=702 ty = 348, pixel px = 510 py = 0
//    String exp = "srcPoint = InlineCsv(\"GEOMETRY\", \"'POINT(9.029234 45.223345)'\");\n"
//                 + "friction = [/mrgeo/images/dave-tobler-raw-spm_nowater];\n"
//                 + "result = CostDistance(srcPoint, 9, friction, 2000000, -3.5, 36.8, 9.9, 45.6)";
    // For tobler-raw-4tiles
//    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(65.1 30.1)'\");\n"
//                 + "friction = [" + frictionSurface + "];\n"
//                 + "result = CostDistance(src, " + TOBLER_MEDIUM_ZOOM + ", friction, \"50000.0\");";

    if (GEN_BASELINE_DATA_ONLY)
    {
      long start = System.currentTimeMillis();
      testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
      long timeDelta = System.currentTimeMillis() - start;
      System.out.println("testCostDistance took " + timeDelta);
      log.error("testCostDistance took " + timeDelta);
    }
    else
    {
      long start = System.currentTimeMillis();
      testUtils.runRasterExpression(conf, testname.getMethodName(), exp);
      long timeDelta = System.currentTimeMillis() - start;
      log.error("testCostDistance took " + timeDelta);
      System.out.println("testCostDistance took " + timeDelta);

      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
          new Path(testUtils.getOutputHdfs(), testname.getMethodName()).toUri().toString(),
          AccessMode.READ, (ProviderProperties)null);

      MrsPyramidMetadata metadata = dp.getMetadataReader().read();

      ImageStats[] stats = metadata.getStats();
      ImageStats[] imageStats = metadata.getImageStats(metadata.getMaxZoomLevel());
      ImageStats bandStats = metadata.getStats(0);

      Assert.assertArrayEquals(stats, imageStats);
      Assert.assertEquals(bandStats, imageStats[0]);

      double epsilon = 0.5;
      Assert.assertEquals(0, bandStats.min, epsilon);
      Assert.assertEquals(50000, bandStats.max, epsilon);
      Assert.assertEquals(33142.46070624142, bandStats.mean, epsilon);
      Assert.assertEquals(1562052, bandStats.count);
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testCostDistanceWithUniformFriction() throws Exception
  {
    String uniformFrictionSurface = testUtils.getInputHdfs() + "/" +  ALL_ONES;
    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(142.135 -17.945)'\");\n"
        + "friction = [" + uniformFrictionSurface + "];\n"
        + "result = CostDistance(src, " + ALL_ONES_ZOOM + ", friction, \"20000.0\");";

    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
    }
    else
    {
      testUtils.runRasterExpression(conf, testname.getMethodName(), exp);

      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
          new Path(testUtils.getOutputHdfs(), testname.getMethodName()).toUri().toString(),
          AccessMode.READ, (ProviderProperties)null);

      MrsPyramidMetadata metadata = dp.getMetadataReader().read();

      ImageStats[] stats = metadata.getStats();
      ImageStats[] imageStats = metadata.getImageStats(metadata.getMaxZoomLevel());
      ImageStats bandStats = metadata.getStats(0);

      Assert.assertArrayEquals(stats, imageStats);
      Assert.assertEquals(bandStats, imageStats[0]);
    }

  }

//  @Test
//  @Category(IntegrationTest.class)
//  public void testCostDistanceWithBoundsAndDefaultZoom() throws Exception
//  {
//    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(67.1875 32.38)'\");\n"
//        + "friction = [" + frictionSurface + "];\n"
//        + "result = CostDistance(src, friction, \"50000.0\");";
//
//    if (GEN_BASELINE_DATA_ONLY)
//    {
//      testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
//    }
//    else
//    {
//      testUtils.runRasterExpression(conf, testname.getMethodName(), exp);
//    }
//  }
//
@Test
@Category(IntegrationTest.class)
public void testCostDistanceWithBoundsAndLowerZoomLevel() throws Exception
{
  final int lowerZoomLevel = TOBLER_MEDIUM_ZOOM - 1;

  String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(67.1875 32.38)'\");\n"
      + "friction = [" + frictionSurface + "];\n"
      + "result = CostDistance(src, " + lowerZoomLevel + ",friction, \"50000.0\");";

  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(conf, testname.getMethodName(), exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void directionalCostDistance() throws Exception
{
  String exp = "" +
      "sl = directionalslope([small-elevation]);\n" +
      //"cp = crop(sl, 142.1, -18.12, 142.5, -18.13);\n" +
      //"pingle = 3.6 / (112 * pow(2.718281828, -8.3 * abs(sl)));\n" +
      "tobler = 3.6 / (6 * pow(2.718281828, -3.5 * abs(sl + 0.087)));\n" +
      //"tobler = 3.6 / (6 * pow(2.718281828, -3.5 * abs(cp + 0.087)));\n" +
      "src = InlineCsv(\"GEOMETRY\", \"'POINT(142.4115 -18.1222)'\");\n" +
      //"src = InlineCsv(\"GEOMETRY\", \"'POINT(142.2 -18.0)'\");\n" +
      //"cost = CostDistance(src, pingle, 50000.0);\n" +
      "cost = CostDistance(src, tobler);\n" +
      "";
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(conf, testname.getMethodName(), exp);
  }
}

@Test
@Category(IntegrationTest.class)
public void nondirectionalCostDistance() throws Exception
{
  String exp = "" +
      "sl = slope([small-elevation]);\n" +
      //"cp = crop(sl, 142.1, -18.12, 142.5, -18.13);\n" +
      //"pingle = 3.6 / (112 * pow(2.718281828, -8.3 * abs(sl)));\n" +
      "tobler = 3.6 / (6 * pow(2.718281828, -3.5 * abs(sl + 0.087)));\n" +
      //"tobler = 3.6 / (6 * pow(2.718281828, -3.5 * abs(cp + 0.087)));\n" +
      "src = InlineCsv(\"GEOMETRY\", \"'POINT(142.4115 -18.1222)'\");\n" +
      //"src = InlineCsv(\"GEOMETRY\", \"'POINT(142.2 -18.0)'\");\n" +
      //"cost = CostDistance(src, pingle, 50000.0);\n" +
      "cost = CostDistance(src, tobler);\n" +
      "";
  if (GEN_BASELINE_DATA_ONLY)
  {
    testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
  }
  else
  {
    testUtils.runRasterExpression(conf, testname.getMethodName(), exp);
  }
}

//  @Ignore
//  @Test
//  @Category(IntegrationTest.class)
//  public void testCostDistanceWithLeastCostPath() throws Exception
//  {
//    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(67.1875 32.38)'\");\n"
//        + "friction = [" + frictionSurface + "];\n"
//        + "cost = CostDistance(src, " + TOBLER_MEDIUM_ZOOM + ", friction, \"50000.0\");\n"
//        + "destPts = InlineCsv(\"GEOMETRY\", \"'POINT(67.67 32.5)'\");\n"
//        + "result = LeastCostPath(cost, destPts);";
//
//    if (GEN_BASELINE_DATA_ONLY)
//    {
//      vectorutils.generateBaselineVector(conf, testname.getMethodName(), exp);
//    }
//    else
//    {
//      vectorutils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);
//      vectorutils.compareVectors(conf, testname.getMethodName());
//    }
//
////    LeastCostPathMapOpIntegrationTest.runLeastCostPath(HadoopUtils.createConfiguration(),
////        testUtils.getOutputHdfs(),
////        testname.getMethodName(),
////        exp,
////        false, // don't do path checks
////        41783.9f,
////        53631.8f,
////        0.8d,
////        2.1d,
////        1.34d
////    );
//  }
//
//
//  @Ignore
//  @Test
//  @Category(IntegrationTest.class)
//  public void testOpportunityVolume() throws Exception
//  {
//    String exp = "srcPt = InlineCsv(\"GEOMETRY\", \"'POINT(67.1875 32.38)'\");\n"
//        +   "destPt = InlineCsv(\"GEOMETRY\", \"'POINT(67.7 32.38)'\");\n"
//        +   "friction = [" + frictionSurface + "];\n"
//        +   "srcCost = CostDistance(srcPt, " + TOBLER_MEDIUM_ZOOM + ",friction, \"50000.0\");\n"
//        +   "destCost = CostDistance(destPt, " + TOBLER_MEDIUM_ZOOM + ",friction, \"50000.0\");\n"
//        +   "opvol = 50000 - (srcCost + destCost);\n"
//        +   "result = con(opvol < 0, 0, opvol);";
//
//    if (GEN_BASELINE_DATA_ONLY)
//    {
//      testUtils.generateBaselineTif(conf, testname.getMethodName(), exp, -9999);
//    }
//    else
//    {
//      testUtils.runRasterExpression(conf, testname.getMethodName(), exp);
//    }
//  }
}
