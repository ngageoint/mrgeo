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

package org.mrgeo.mapalgebra.optimizer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.MapAlgebraParser;
import org.mrgeo.mapalgebra.MapOp;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.test.LocalRunnerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizerTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(OptimizerTest.class);
  protected static Path smallElevationPath;
  private static String smallElevationName = "small-elevation";
  private static String smallElevation = Defs.CWD + "/" + Defs.INPUT + smallElevationName;
  MapAlgebraParser parser;

  @BeforeClass
  public static void init() throws URISyntaxException
  {
    smallElevationPath = new Path(new java.net.URI(smallElevation));
  }

  public void testExpression(String exp, String expected) throws ParserException,
      FileNotFoundException, IOException, URISyntaxException
  {
    MapOp mo = parser.parse(exp);

    Optimizer uut = new Optimizer(mo);
    MapOp result = uut.optimize();

    String r = MapAlgebraParser.toString(result).replace(smallElevationPath.toString(), smallElevationName);

    Assert.assertEquals("Bad optimization", expected, r);
  }

  @Before
  public void setup()
  {
    parser = new MapAlgebraParser(conf, "", null);
  }

  @Test
  @Category(IntegrationTest.class)
  public void testBasics1() throws Exception
  {
    try
    {
      testExpression("0 + 0", "0.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testBasics2() throws Exception
  {
    try
    {
      testExpression("0 + 0 + 0", "0.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics3() throws Exception
  {
    try
    {
      testExpression(String.format("0 + 0 + 3 + [%s] + 0", smallElevation),
          String.format("+\n  3.0\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics4() throws Exception
  {
    try
    {
      testExpression(String.format("0 + 2 + 3 + [%s] + 0", smallElevation),
          String.format("+\n  5.0\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics5() throws Exception
  {
    try
    {
      testExpression("1 * 1", "1.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics6() throws Exception
  {
    try
    {
      testExpression(String.format("1 * (5 + 2) - [%s]", smallElevation),
          String.format("-\n  7.0\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics7() throws Exception
  {
    try
    {
      testExpression(String.format("0 + 2 + 1 * [%s] + 0", smallElevation),
          String.format("+\n  2.0\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics8() throws Exception
  {
    try
    {
      testExpression("0 - 0", "0.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics9() throws Exception
  {
    try
    {
      testExpression("2 - 1", "1.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics10() throws Exception
  {
    try
    {
      testExpression(String.format("[%s] - 0", smallElevation), String.format("[%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics11() throws Exception
  {
    try
    {
      testExpression(String.format("[%s] - 0 - 1", smallElevation),
          String.format("-\n  [%s]\n  1.0\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics12() throws Exception
  {
    try
    {
      testExpression("1 / 1", "1.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics13() throws Exception
  {
    try
    {
      testExpression("1 / 2", "0.5\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics14() throws Exception
  {
    try
    {
      testExpression(String.format("[%s] / 1", smallElevation), String.format("[%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics15() throws Exception
  {
    try
    {
      testExpression(String.format("4 / 2 / [%s] / 6", smallElevation),
          String.format("/\n  0.3333333333333333\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics16() throws Exception
  {
    try
    {
      testExpression(String.format("0 * ([%s] + 2)", smallElevation), "0.0\n");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics17() throws Exception
  {
    try
    {
      testExpression(String.format("1 + [%s] - 2", smallElevation),
          String.format("+\n  -1.0\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics18() throws Exception
  {
    try
    {
      testExpression(String.format("1 * [%s] / 2", smallElevation),
          String.format("/\n  [%s]\n  2.0\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasics19() throws Exception
  {
    try
    {
      testExpression(String.format("(3 + 2) * [%s] / 2", smallElevation),
          String.format("*\n  2.5\n  [%s]\n", smallElevation));
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
