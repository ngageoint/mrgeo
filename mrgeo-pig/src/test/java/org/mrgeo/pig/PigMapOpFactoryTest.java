/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.pig;

import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.mapalgebra.MapOpFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/**
 * Used to test and make sure that the Parser properly parses and executes
 * Legion operations.
 */
@SuppressWarnings("static-method")
public class PigMapOpFactoryTest extends LocalRunnerTest
{
  @Rule public TestName testname = new TestName();
  
  public final static boolean GEN_BASELINE_DATA_ONLY = false;

  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(PigMapOpFactoryTest.class);
  
  private static String input1;
  private static String input2;
  private static String roads;

  private static MapOpTestVectorUtils testUtils;
  
  @BeforeClass 
  public static void init()
  {
    try
    {
      testUtils = new MapOpTestVectorUtils(PigMapOpFactoryTest.class);
      
      
      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "input1.tsv");
      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "input1.tsv.columns");
      input1 = testUtils.getInputHdfsFor("input1.tsv").toString();

      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "input2.tsv");
      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "input2.tsv.columns");
      input2  = testUtils.getInputHdfsFor("input2.tsv").toString();

      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "roads.shp");
      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "roads.dbf");
      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "roads.prj");
      HadoopFileUtils.copyToHdfs(new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "roads.shx");
      roads = testUtils.getInputHdfsFor("roads.shp").toString();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @AfterClass 
  public static void cleanup()
  {
     input1 = null;
     input2 = null;
     roads = null;

     testUtils = null;
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void testBasics() throws Exception
  {
    try
    {
      ServiceLoader<MapOpFactory> loader = ServiceLoader.load(MapOpFactory.class);

      int count = 0;
      for (MapOpFactory s : loader)
      {
        System.out.println(s.getClass().getName());
        if (s.getClass().getName().equals("org.mrgeo.pig.PigMapOpFactory"))
        {
          count++;
        }
      }

      Assert.assertEquals(1, count);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

//  /**
//   * Returns 1 if the images don't match, otherwise returns 0.
//   * 
//   * @param testName
//   * @param ex
//   * @return
//   * @throws Exception 
//   */
//  private int runExpression(String testName, String ex) throws Exception
//  {
//    int result = 0;
//    log.debug(ex);
//    MapAlgebraParserv1 uut = new MapAlgebraParserv1(this.conf);
//
//    MapAlgebraExecutionerv1 mae = new MapAlgebraExecutionerv1();
//
//    MapOp mo = uut.parse(ex);
//
//    mae.setRoot(mo);
//    mae.setOutputPath(new Path(_outputHdfs, testName));
//    mae.execute(this.conf, new ProgressHierarchy());
//
//    if (TestUtils.readPath(new Path(_outputHdfs + "/" + testName + "/part-m-00000.tsv"))
//        .equals(TestUtils.readFile(new File(_input + "/" + testName))) == false)
//    {
//      log.warn("Test {} failed.", testName);
//      result = 1;
//    }
//
//    if (TestUtils.readPath(new Path(_outputHdfs + "/" + testName + ".columns")).equals(
//        TestUtils.readFile(new File(_input + "/" + testName + ".columns"))) == false)
//    {
//      log.warn("Test {} columns failed.", testName);
//      result = 1;
//    }
//
//    Assert.assertEquals(0, result);
//
//    return result;
//  }
//
//  private int runImageExpression(String testName, String ex) throws Exception
//  {
//    log.debug(ex);
//    MapAlgebraParserv1 uut = new MapAlgebraParserv1(this.conf);
//
//    MapAlgebraExecutionerv1 mae = new MapAlgebraExecutionerv1();
//
//    MapOp mo = uut.parse(ex);
//
//    mae.setRoot(mo);
//    mae.setOutputPath(new Path(_outputHdfs, testName));
//    mae.execute(this.conf, new ProgressHierarchy());
//
//    MrsPyramidv1 pyramid = MrsPyramidv1.loadPyramid(new Path(_outputHdfs, testName));
//    GeoTiffExporter.export(pyramid.getImage(0), new File(_output + testName + ".tif"));
//
//    RenderedImage test = GeoTiffDescriptor.create(_output + testName + ".tif", null);
//    RenderedImage base = GeoTiffDescriptor.create(_input + testName + ".tif", null);
//    int result = TestUtils.calculateDiffStats(base, test).diffCount == 0 ? 0 : 1;
//
//    if (result != 0)
//    {
//      log.warn("Test {} failed.", testName);
//    }
//    return result;
//  }

  @Test
  @Category(IntegrationTest.class)
  public void testExpressions1() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineVector(conf, testname.getMethodName(), String.format(
          "pig(\"result = LOAD '%s' USING MrGeoLoader;\")", input1));
    }
    else
      {
      testUtils.runVectorExpression(conf, testname.getMethodName(), String.format(
          "pig(\"result = LOAD '%s' USING MrGeoLoader;\")", input1));
      }
  }
  
  
  @Test
  @Category(IntegrationTest.class)
  public void testExpressions2() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineVector(conf, testname.getMethodName(), String.format(
          "pig(\"result = LOAD '{%%1}' USING MrGeoLoader;\", [%s])", input1));
    }
    else
      {
     testUtils.runVectorExpression(conf, testname.getMethodName(), String.format(
          "pig(\"result = LOAD '{%%1}' USING MrGeoLoader;\", [%s])", input1));
      }
  }
  
  
  // Ignored until pig & VectorBuffer is v2
  @Ignore
  @Test
  @Category(IntegrationTest.class)
  public void testExpressions3() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineVector(conf, testname.getMethodName(), String.format(
          "VectorBuffer(pig(\"result = LOAD '{%%1}' USING MrGeoLoader;\", [%s]), 1)", input2));
    }
    else
      {
      testUtils.runVectorExpression(conf, testname.getMethodName(), String.format(
          "VectorBuffer(pig(\"result = LOAD '{%%1}' USING MrGeoLoader;\", [%s]), 1)", input2));
      }
  }
    
  // Ignored until pig & VectorBuffer is v2
  @Ignore
  @Test
  @Category(IntegrationTest.class)
  public void testExpressions4() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineVector(conf, testname.getMethodName(), String.format(
          "pig(\"result = LOAD '{%%1}' USING MrGeoLoader;\", VectorBuffer([%s], 1))", input2));
    }
    else
      {
    testUtils.runVectorExpression(conf, testname.getMethodName(), String.format(
          "pig(\"result = LOAD '{%%1}' USING MrGeoLoader;\", VectorBuffer([%s], 1))", input2));
      }
  }
  
  @Ignore
  @Test
  @Category(IntegrationTest.class)
  public void testExpressions5() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineVector(conf, testname.getMethodName(), 
          String.format("pigScript(\"%stest6.pig\", [%s])", testUtils.getInputLocal(), roads));
    }
    else
      {
    testUtils.runVectorExpression(conf, testname.getMethodName(), 
        String.format("pigScript(\"%stest6.pig\", [%s])", testUtils.getInputLocal(), roads));
      }
  }
  
  // Ignored until pig & shapgrid is v2
  @Ignore
  @Test
  @Category(IntegrationTest.class)
  public void testExpressions6() throws Exception
  {
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineVector(conf, testname.getMethodName(),  String.format(
          "shapegrid(pigScript(\"%stest7.pig\", [%s]), \"MAXSPEED\", 1)", testUtils.getInputLocal(), roads));
    }
    else
      {
    testUtils.runVectorExpression(conf, testname.getMethodName(),  String.format(
          "shapegrid(pigScript(\"%stest7.pig\", [%s]), \"MAXSPEED\", 1)", testUtils.getInputLocal(), roads));
  }
  }
}
