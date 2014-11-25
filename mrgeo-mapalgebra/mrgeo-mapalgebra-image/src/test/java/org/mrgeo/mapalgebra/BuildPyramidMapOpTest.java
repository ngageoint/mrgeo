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

import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Ignore
public class BuildPyramidMapOpTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(BuildPyramidMapOpTest.class);

  @Rule
  public TestName testname = new TestName();


  private static MapOpTestUtils testUtils;

  private static String smallElevationNoPyramids = "small-elevation-nopyramids";
  private static Path smallElevationNoPyramidsPath;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new MapOpTestUtils(BuildPyramidMapOpTest.class);
  }

  @Before
  public void setup() throws IOException
  {
    Path parent = new Path(testUtils.getInputHdfs(), testname.getMethodName());
    HadoopFileUtils.copyToHdfs(Defs.INPUT, parent, smallElevationNoPyramids, true);
    smallElevationNoPyramidsPath = 
        HadoopFileUtils.unqualifyPath(new Path(parent, smallElevationNoPyramids));

  }

  @Test
  @Category(UnitTest.class)
  public void testTooFewArgs() throws Exception
  {
    String exp = String.format("BuildPyramid()");
    try
    {
      testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);
      Assert.fail("Expected IllegalArgumentException");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("Usage:"));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testTooManyArgs() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"mean\", \"bogus\")", smallElevationNoPyramidsPath);
    try
    {
      testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);
      Assert.fail("Expected IllegalArgumentException");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("Usage:"));
    }
  }

  @Test(expected = ParserException.class)
  @Category(UnitTest.class)
  public void testBadInput() throws Exception
  {
    String exp = "BuildPyramid(InlineCsv(\"NAME,GEOMETRY\",\"'Place1','POINT(69.1 34.5)';'Place2','POINT(69.25 34.55)'\"), \"mean\")";

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);
      Assert.fail("Expected IllegalArgumentException");
  }

}
