package org.mrgeo.mapalgebra;

import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.mrgeo.utils.HadoopUtils;
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
  @Rule
  public TestName testname = new TestName();

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
    // For tobler-raw-4tiles
//    String exp = "src = InlineCsv(\"GEOMETRY\", \"'POINT(65.1 30.1)'\");\n"
//                 + "friction = [" + frictionSurface + "];\n"
//                 + "result = CostDistance(src, " + TOBLER_MEDIUM_ZOOM + ", friction, \"50000.0\");";

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

      MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();

      ImageStats[] stats = metadata.getStats();
      ImageStats[] imageStats = metadata.getImageStats(metadata.getMaxZoomLevel());
      ImageStats bandStats = metadata.getStats(0);

      Assert.assertArrayEquals(stats, imageStats);
      Assert.assertEquals(bandStats, imageStats[0]);

      double epsilon = 0.5;
      Assert.assertEquals(0, bandStats.min, epsilon);
      Assert.assertEquals(50000, bandStats.max, epsilon);
      Assert.assertEquals(33113.35, bandStats.mean, epsilon);
      Assert.assertEquals(1852001, bandStats.count);
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

      MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();

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
