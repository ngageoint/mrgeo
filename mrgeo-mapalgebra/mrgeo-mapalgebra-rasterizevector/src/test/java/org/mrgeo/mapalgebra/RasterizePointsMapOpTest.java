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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RasterizePointsMapOpTest extends LocalRunnerTest
{
  private static MapOpTestUtils testUtils;

  // only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
  public final static boolean GEN_BASELINE_DATA_ONLY = false;

  private static final Logger log = LoggerFactory.getLogger(RasterizePointsMapOpTest.class);
  private static String shapefile = "major_road_intersections_exploded";
  private static String cropRaster = "major_road_intersections_exploded_crop_area";
  private static String hdfsShapefile;
  private static String hdfsCropRaster;
  private static String column = "FID_kabul_";

  @BeforeClass
  public static void init() throws IOException
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      log.warn("***RasterizePointsMapOpTest TESTS SET TO GENERATE BASELINE IMAGES ONLY***");
    }

    testUtils = new MapOpTestUtils(RasterizePointsMapOpTest.class);
    HadoopFileUtils.delete(testUtils.getInputHdfs());

    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
                               testUtils.getInputHdfs(), shapefile + ".shp");
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
                               testUtils.getInputHdfs(), shapefile + ".prj");
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
                               testUtils.getInputHdfs(), shapefile + ".shx");
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
                               testUtils.getInputHdfs(), shapefile + ".dbf");
    hdfsShapefile = testUtils.getInputHdfsFor(shapefile + ".shp").toString();

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), cropRaster);
    hdfsCropRaster = testUtils.getInputHdfsFor(cropRaster).toString();
  }

  @Before
  public void setup()
  {
  }

  @Test
  @Category(IntegrationTest.class)
  public void rasterizePointsMaskBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MASK\", 0.0001716614, 68.85, 34.25, 69.35, 34.75)";

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
  public void rasterizePointsMaskRasterBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MASK\", 0.0001716614, [" + hdfsCropRaster + "])";

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

  @Test(expected = Exception.class)
  @Category(IntegrationTest.class)
  public void rasterizePointsOutOfBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MASK\", 0.0001716614, \"-68.85\", 34.25, \"-69.35\", 34.75)";

    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
                                  TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);

  }

  @Test
  @Category(IntegrationTest.class)
  public void rasterizePointsMask() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MASK\", 0.0001716614)";
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
  public void rasterizePointsSum() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614)";
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
  public void rasterizePointsSumColumnNoBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\")";
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
  public void rasterizePointsSumBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 68.85, 34.25, 69.35, 34.75)";
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
  public void rasterizePointsSumRasterBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", [" + hdfsCropRaster + "])";
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
  public void rasterizePointsAverage() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"AVERAGE\", 0.0001716614, \"" + column + "\")";
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
  public void rasterizePointsMin() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MIN\", 0.0001716614, \"" + column + "\")";
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
  public void rasterizePointsMax() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MAX\", 0.0001716614, \"" + column + "\")";
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

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void maskPointsWithColumn() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"MASK\", 0.0001716614, \"" + column + "\")";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void gaussianPoints() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"GAUSSIAN\", 0.0001716614)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test
  @Category(UnitTest.class)
  public void sumPointsWithoutColumnWithBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, 68.85, 34.25, 69.35, 34.75)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test
  @Category(UnitTest.class)
  public void sumPointsWithoutColumnWithRasterBounds() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, [" + hdfsCropRaster + "])";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void sumPointsWithBadBounds3() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 68.85, 34.25, 69.35)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void sumPointsWithBadBounds2() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 34.25, 69.35)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void sumPointsWithBadBounds1() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 68.85)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void badPointsAggregationType() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], \"BAD\", 0.0001716614, \"" + column + "\", 68.85, -34.25, 69.35, -34.75)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void missingPointsQuotesAggregationType() throws Exception
  {
    String exp = "RasterizePoints([" + hdfsShapefile + "], SUM, 0.0001716614, \"" + column + "\", 68.85, -34.25, 69.35, -34.75)";
    MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
  }

  @Test
  @Category(IntegrationTest.class)
  public void variablePoints() throws Exception
  {
    String exp = String.format("a = [%s]; RasterizePoints(a, \"MAX\", 1, \"" + column + "\") ", hdfsShapefile);
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
}
