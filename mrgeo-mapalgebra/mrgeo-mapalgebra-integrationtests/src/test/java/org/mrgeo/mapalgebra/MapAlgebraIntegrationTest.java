/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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
 */

package org.mrgeo.mapalgebra;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.opimage.ConstantDescriptor;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author jason.surratt
 *
 */
public class MapAlgebraIntegrationTest extends LocalRunnerTest
{
  @Rule
  public TestName testname = new TestName();

  // only set this to true to generate new baseline images after correcting tests; image comparison
  // tests won't be run when is set to true
  public final static boolean GEN_BASELINE_DATA_ONLY = false;

  private static String smallElevationName = "small-elevation";
  private static String smallElevation = Defs.INPUT + smallElevationName;
  protected static Path smallElevationPath;

  private static String greeceName = "greece";
  private static String greece = Defs.INPUT + greeceName;

  private static String majorRoadShapeName = "major_road_intersections_exploded";
  protected static Path majorRoadShapePath;


  private static String vectorName = "InputEllipse.tsv";
  private static String vector = Defs.INPUT + vectorName;

  protected static String pointsName = "input1"; // .tsv
  protected static String pointsPath;

  private static String allones = "all-ones";
  private static Path allonesPath;
  private static String alltwos = "all-twos";
  private static String allhundreds = "all-hundreds";


  protected static String _positives;
  protected static String _negatives;
  protected static String _points;

  private static MapOpTestUtils testUtils;
  // Vector private static MapOpTestVectorUtils vectorTestUtils;

  private static final Logger log = LoggerFactory.getLogger(MapAlgebraIntegrationTest.class);

  private static String factor1 = "fs_Bazaars_v2";
  private static String factor2 = "fs_Bus_Stations_v2";
  private static String eventsPdfs = "eventsPdfs";
  private Properties props = null;

  @Before
  public void setup()
  {
    MrGeoProperties.getInstance().setProperty(HadoopUtils.IMAGE_BASE, testUtils.getInputHdfs().toUri().toString());
    MrGeoProperties.getInstance().setProperty(HadoopUtils.VECTOR_BASE, testUtils.getInputHdfs().toUri().toString());
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

    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
        testUtils.getInputHdfs(),
        majorRoadShapeName + ".shp");
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
        testUtils.getInputHdfs(),
        majorRoadShapeName + ".prj");
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
        testUtils.getInputHdfs(),
        majorRoadShapeName + ".shx");
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal(), "roads"),
        testUtils.getInputHdfs(),
        majorRoadShapeName + ".dbf");
    majorRoadShapePath = testUtils.getInputHdfsFor(majorRoadShapeName + ".shp");

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allones);
    allonesPath = new Path(testUtils.getInputHdfs(), allones);

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), alltwos);
    //alltwosPath = new Path(testUtils.getInputHdfs(), allones);

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), allhundreds);
    // allhundredsPath = new Path(testUtils.getInputHdfs(), allones);

    HadoopFileUtils
        .copyToHdfs(new Path(Defs.INPUT), testUtils.getInputHdfs(), smallElevationName);
    smallElevationPath = new Path(testUtils.getInputHdfs(), smallElevationName);

    File file = new File(smallElevation);
    smallElevation = new Path("file://" + file.getAbsolutePath()).toString();

    file = new File(greece);
    greece = new Path("file://" + file.getAbsolutePath()).toString();

    vector = testUtils.getInputLocal() + "../" + vectorName;
    file = new File(vector);
    vector = new Path("file://" + file.getAbsolutePath()).toString();

    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(),
        factor1);
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(),
        factor2);
    HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(),
        eventsPdfs);
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
          String.format("[%s] + [%s]", allones, allones));

    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void addSubtractConstant() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
          String.format("[%s] + [%s] - 3", allhundreds, allones), Double.NaN);

    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("[%s] + [%s] + -3", allhundreds, allones), Double.NaN);

    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("aspect([%s])", smallElevation), Double.NaN);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format("aspect([%s])", smallElevation));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void aspectRad() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
          String.format("aspect([%s], \"rad\")", smallElevation), Double.NaN);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format("aspect([%s], \"rad\")", smallElevation));
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
          String.format("con([%s] <= 100, 0, 1)", smallElevationPath), Double.NaN);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("con([%s] != 200, 2, 0)", smallElevationPath), Double.NaN);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("([%s] <= 100) || ([%s] >= 200)", smallElevationPath, smallElevationPath), 255);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("([%s] < 100) || ([%s] > 200)", smallElevationPath, smallElevationPath), 255);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("cos([%s])", allones), Double.NaN);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
              "crop([%s], 142.05, -17.75, 142.2, -17.65);", smallElevationPath), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format(
              "crop([%s],  142.05, -17.75, 142.2, -17.65)", smallElevationPath));

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
              "crop([%s],  142.05, -17.75, 142.2, -17.65,\"EXACT\")",
              smallElevationPath), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format(
              "crop([%s],  142.05, -17.75, 142.2, -17.65,\"EXACT\")",
              smallElevationPath));
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
          String.format("[%s] / [%s] + 3", allones, allones));

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
              + "[" + allonesPath.toString() + "];");
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          "a = [" + fname + "] + "
              + "[" + allonesPath.toString() + "];");
    }
  }
  @Test
  @Category(IntegrationTest.class)
  public void fill() throws Exception
  {
    // paths with file extensions
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
          "fill([" + greece + "], 1, 22.6, 39.4, 26, 42.15, \"EXACT\")");
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          "fill([" + greece + "], 1, 22.6, 39.4, 26, 42.15, \"EXACT\")");
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
          String.format("log([%s])", alltwos));

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
          String.format("[%s] + [%s] * [%s] - [%s]", allones, allones, allones, allones));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void parse1() throws Exception
  {
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, null);
    final String ex = String.format("[%s] + [%s]", smallElevation, smallElevation);

    // expected
    final RawBinaryMathMapOp expRoot = new RawBinaryMathMapOp();
    expRoot.setFunctionName("+");

    MrsImageDataProvider elevationDataProvider = DataProviderFactory.getMrsImageDataProvider(smallElevation.toString(),
        AccessMode.READ, props);
    final MrsPyramidMapOp pyramidOp1 = new MrsPyramidMapOp();
    pyramidOp1.setDataProvider(elevationDataProvider);

    final MrsPyramidMapOp pyramidOp2 = new MrsPyramidMapOp();
    pyramidOp2.setDataProvider(elevationDataProvider);

    expRoot.addInput(pyramidOp1);
    expRoot.addInput(pyramidOp2);

    final MapOp mo = uut.parse(ex);
    assertMapOp(expRoot, mo);
  }

  @Test
  @Category(UnitTest.class)
  public void parse2() throws Exception
  {
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, null);
    final String ex = String.format("[%s] * 15", smallElevation);

    // expected
    final RawBinaryMathMapOp expRoot = new RawBinaryMathMapOp();
    expRoot.setFunctionName("*");

    MrsImageDataProvider elevationDataProvider = DataProviderFactory.getMrsImageDataProvider(smallElevation.toString(),
        AccessMode.READ, props);
    final MrsPyramidMapOp mapOp1 = new MrsPyramidMapOp();
    mapOp1.setDataProvider(elevationDataProvider);

    final RenderedImageMapOp mapOp2 = new RenderedImageMapOp();
    mapOp2.setRenderedImageFactory(new ConstantDescriptor());
    mapOp2.getParameters().add(new Double(15));

    expRoot.addInput(mapOp1);
    expRoot.addInput(mapOp2);

    final MapOp mo = uut.parse(ex);
    assertMapOp(expRoot, mo);
  }

  @Test
  @Category(UnitTest.class)
  public void parse3() throws Exception
  {
    final String ex = String.format("log([%s])", smallElevation);

    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);

    // expected
    final LogarithmMapOp expRoot = new LogarithmMapOp();
    expRoot.getParameters().add(new Double(0));

    MrsImageDataProvider elevationDataProvider = DataProviderFactory.getMrsImageDataProvider(smallElevation.toString(),
        AccessMode.READ, props);
    final MrsPyramidMapOp pyramidOp = new MrsPyramidMapOp();
    pyramidOp.setDataProvider(elevationDataProvider);

    expRoot.addInput(pyramidOp);

    final MapOp mo = uut.parse(ex);
    assertMapOp(expRoot, mo);

    // now add a search path and see if you get the same results
    final String ex1 = String.format("log([%s])", smallElevation);

    Path p = new Path(smallElevation);
    p = p.getParent();
//    uut.addPath(p.toString());

    final MapOp mo1 = uut.parse(ex1);
    assertMapOp(expRoot, mo1);
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void parse4() throws Exception
  {
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);
    final String ex = String.format("log([%s/abc])", testUtils.getInputHdfs().toString());

    uut.parse(ex);
  }

  @Test(expected = IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void parse5() throws Exception
  {
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);
    System.err.println(testUtils.getInputHdfs().toString());
    final String ex = String.format("[%s] * 15", testUtils.getInputHdfs().toString());

    uut.parse(ex);
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void parseInvalidArguments1() throws Exception
  {
    final MapAlgebraParser parser = new MapAlgebraParser(this.conf, props);
    final String ex = String.format("[%s] + ", smallElevation);

    parser.parse(ex);
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void parseInvalidArguments2() throws Exception
  {
    final MapAlgebraParser parser = new MapAlgebraParser(this.conf, props);
    final String ex = String.format("abs [%s] [%s] ", smallElevation, smallElevation);

    parser.parse(ex);
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void parseInvalidArguments3() throws Exception
  {
    final MapAlgebraParser parser = new MapAlgebraParser(this.conf, props);
    final String ex = String.format("con[%s] + ", smallElevation);

    parser.parse(ex);
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void parseInvalidArguments4() throws Exception
  {
    final MapAlgebraParser parser = new MapAlgebraParser(this.conf, props);
    final String ex = String.format("costDistance");

    parser.parse(ex);
  }

  //  @Test(expected = TokenMgrError.class)
  @Category(UnitTest.class)
  public void parseInvalidOperation() throws Exception
  {
    final MapAlgebraParser parser = new MapAlgebraParser(this.conf, props);
    // String ex = String.format("[%s] & [%s]", allones, _blur2);
    final String ex = String.format("[%s] & [%s]", smallElevation, smallElevation);

    parser.parse(ex);
  }

  @Test
  @Category(IntegrationTest.class)
  public void pow() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
          String.format("pow([%s], 1.2)", allones), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format("pow([%s], 1.2)", allones));

    }
  }

  @Test
  @Category(UnitTest.class)
  public void rasterExistsDefaultSearchPath() throws Exception
  {
    final Path p = new Path(smallElevation).getParent();
    MrGeoProperties.getInstance().setProperty("image.base", p.toString());

    final String expr = String.format("a = [%s] + [%s]", smallElevationName, smallElevationName);
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);

    // expected
    final RawBinaryMathMapOp expRoot = new RawBinaryMathMapOp();
    expRoot.setFunctionName("+");

    MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(smallElevationName,
        AccessMode.READ, props);
    final MrsPyramidMapOp mapOp1 = new MrsPyramidMapOp();
    mapOp1.setDataProvider(provider);

    final MrsPyramidMapOp mapOp2 = new MrsPyramidMapOp();
    mapOp2.setDataProvider(provider);

    expRoot.addInput(mapOp1);
    expRoot.addInput(mapOp2);

    final MapOp mo = uut.parse(expr);
    assertMapOp(expRoot, mo);
  }

  @Test
  @Category(UnitTest.class)
  public void rasterExistsFullyQualifiedPath() throws Exception
  {
    final String ex = String.format("log([%s])", smallElevation);

    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);

    // expected
    final LogarithmMapOp expRoot = new LogarithmMapOp();
    expRoot.getParameters().add(new Double(0));

    MrsImageDataProvider elevationDataProvider = DataProviderFactory.getMrsImageDataProvider(smallElevation.toString(),
        AccessMode.READ, props);
    final MrsPyramidMapOp pyramidOp = new MrsPyramidMapOp();
    pyramidOp.setDataProvider(elevationDataProvider);

    expRoot.addInput(pyramidOp);

    // add the parent path of the HDFS version to the search path, we shouldn't
    // find it...
    final Path p = smallElevationPath.getParent();
//    uut.addPath(p.toString());

    final MapOp mo1 = uut.parse(ex);
    assertMapOp(expRoot, mo1);
  }


  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void rasterNotExistsDefaultSearchPath() throws Exception
  {
    final String expr = String.format("a = ([something.tif])");
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);

    uut.parse(expr);
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void rasterNotExistsUserDefinedSearchPath() throws Exception
  {
    final String expr = String.format("a = [thingone.tif] + " + "[thingtwo.tif];");
    final MapAlgebraParser uut = new MapAlgebraParser(this.conf, props);
//    uut.addPath(testUtils.getInputHdfs().toString());
    uut.parse(expr);
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
          String.format("slope([%s], \"gradient\")", smallElevation), Double.NaN);

    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("slope([%s], \"rad\")", smallElevation), Double.NaN);

    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("slope([%s], \"deg\")", smallElevation), Double.NaN);

    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
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
          String.format("slope([%s], \"percent\")", smallElevation), Double.NaN);

    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format("slope([%s], \"percent\")", smallElevation));

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
          String.format("\n\na = [%s];\n\nb = 3;\na \t\n+ [%s] \n- b\n\n", allones, allones), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format("\n\na = [%s];\n\nb = 3;\na \t\n+ [%s] \n- b\n\n", allones, allones));
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
          String.format("a = [%s]; b = 3; a / [%s] + b", allones, allones));

    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void rasterizeVector1() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf,
          testname.getMethodName(),
          String
              .format(
                  "a = [%s]; RasterizeVector(a, \"MASK\", \"0.000092593\")",
                  majorRoadShapePath.toString()), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf,
          testname.getMethodName(),
          String
              .format(
                  "a = [%s]; RasterizeVector(a, \"MASK\", \"0.000092593\")",
                  majorRoadShapePath.toString()));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void rasterizeVector2() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils
          .generateBaselineTif(
              this.conf,
              testname.getMethodName(),
              String.format("a = [%s]; RasterizeVector(a, \"MIN\", 1, \"c\") ",
                  pointsPath.toString()), -9999);
    }
    else
    {
      testUtils
          .runRasterExpression(
              this.conf,
              testname.getMethodName(),
              String.format("a = [%s]; RasterizeVector(a, \"MIN\", 1, \"c\") ",
                  pointsPath.toString()));
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void rasterizeVector3() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(this.conf, testname.getMethodName(),
          String.format("RasterizeVector([%s], \"SUM\", 1)", pointsPath.toString()), -9999);
    }
    else
    {
      testUtils.runRasterExpression(this.conf, testname.getMethodName(),
          String.format("RasterizeVector([%s], \"SUM\", 1)", pointsPath.toString()));
    }
  }

  // asserts the expected Mapop against the actual Mapop generated when parse is
  // called
  // as more unit tests are added this method will need additional code to deal
  // with the
  // specific map ops being tested
  private void assertMapOp(final MapOp expMo, final MapOp mo)
  {
    Assert.assertEquals(expMo.getClass().getName(), mo.getClass().getName());
    if (expMo instanceof RenderedImageMapOp)
    {
      final RenderedImageMapOp rendExpMapOp = (RenderedImageMapOp) expMo;
      final RenderedImageMapOp rendMo = (RenderedImageMapOp) mo;
      Assert.assertEquals(rendExpMapOp.getRenderedImageFactory().getClass().getName(), rendMo
          .getRenderedImageFactory().getClass().getName());
      Assert.assertEquals(rendExpMapOp.getParameters().getNumParameters(), rendMo.getParameters()
          .getNumParameters());
      for (int i = 0; i < rendExpMapOp.getParameters().getNumParameters(); i++)
      {
        if (rendExpMapOp.getParameters().getObjectParameter(i) instanceof Double)
        {
          Assert.assertTrue(Double.compare(rendExpMapOp.getParameters().getDoubleParameter(i),
              rendMo.getParameters().getDoubleParameter(i)) == 0);
        }
        else
        {
          Assert.assertEquals(rendExpMapOp.getParameters().getObjectParameter(i), rendMo
              .getParameters().getObjectParameter(i));
        }
      }
    }
    else if (expMo instanceof MrsPyramidMapOp)
    {
      final MrsPyramidMapOp pyrExpMo = (MrsPyramidMapOp) expMo;
      final MrsPyramidMapOp pyrMo = (MrsPyramidMapOp) mo;
      Assert.assertEquals(pyrExpMo.getOutputName(), pyrMo.getOutputName());
    }
    Assert.assertEquals(expMo.getInputs().size(), mo.getInputs().size());
    for (int u = 0; u < expMo.getInputs().size(); u++)
    {
      assertMapOp(expMo.getInputs().get(u), mo.getInputs().get(u));
    }
  }
}
