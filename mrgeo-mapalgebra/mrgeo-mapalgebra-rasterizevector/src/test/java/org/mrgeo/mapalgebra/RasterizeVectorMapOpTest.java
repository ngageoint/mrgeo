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

@SuppressWarnings("static-method")
public class RasterizeVectorMapOpTest extends LocalRunnerTest
{
private static MapOpTestUtils testUtils;

// only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
public final static boolean GEN_BASELINE_DATA_ONLY = false;

private static final Logger log = LoggerFactory.getLogger(RasterizeVectorMapOpTest.class);
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
    log.warn("***RasterizeVectorMapOpTest TESTS SET TO GENERATE BASELINE IMAGES ONLY***");
  }

  testUtils = new MapOpTestUtils(RasterizeVectorMapOpTest.class);
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
public void rasterizeMaskBounds() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, 68.85, 34.25, 69.35, 34.75)";

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
  public void rasterizeMaskRasterBounds() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, [" + hdfsCropRaster + "])";

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
public void rasterizeOutOfBounds() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, \"-68.85\", 34.25, \"-69.35\", 34.75)";

    testUtils.runRasterExpression(this.conf, testname.getMethodName(),
        TestUtils.nanTranslatorToMinus9999, TestUtils.nanTranslatorToMinus9999, exp);

}

@Test
@Category(IntegrationTest.class)
public void rasterizeMask() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614)";
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
public void rasterizeSum() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614)";
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
public void rasterizeSumColumnNoBounds() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\")";
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
public void rasterizeSumBounds() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 68.85, 34.25, 69.35, 34.75)";
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
  public void rasterizeSumRasterBounds() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", [" + hdfsCropRaster + "])";
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
public void rasterizeAverage() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"AVERAGE\", 0.0001716614, \"" + column + "\")";
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
public void rasterizeMin() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"MIN\", 0.0001716614, \"" + column + "\")";
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
public void rasterizeMax() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"MAX\", 0.0001716614, \"" + column + "\")";
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
public void maskWithColumn() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, \"" + column + "\")";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void gaussian() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"GAUSSIAN\", 0.0001716614)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(UnitTest.class)
public void sumWithoutColumnWithBounds() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, 68.85, 34.25, 69.35, 34.75)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(UnitTest.class)
public void sumWithoutColumnWithRasterBounds() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, [" + hdfsCropRaster + "])";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void sumWithBadBounds3() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 68.85, 34.25, 69.35)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void sumWithBadBounds2() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 34.25, 69.35)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void sumWithBadBounds1() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"" + column + "\", 68.85)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void badAggregationType() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], \"BAD\", 0.0001716614, \"" + column + "\", 68.85, -34.25, 69.35, -34.75)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test(expected = ParserException.class)
@Category(UnitTest.class)
public void missingQuotesAggregationType() throws Exception
{
  String exp = "RasterizeVector([" + hdfsShapefile + "], SUM, 0.0001716614, \"" + column + "\", 68.85, -34.25, 69.35, -34.75)";
  MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""));
}

@Test
@Category(IntegrationTest.class)
public void variable() throws Exception
{
  String exp = String.format("a = [%s]; RasterizeVector(a, \"MAX\", 1, \"" + column + "\") ", hdfsShapefile);
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
