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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.test.LocalRunnerTest;

public class MapAlgebraParserTest extends LocalRunnerTest
{
  @Test
  @Category(UnitTest.class)
  public void testBadResourceName()
  {
    String protectionLevel = "abc";
    final MapAlgebraParser parser = new MapAlgebraParser(getConfiguration(),
        protectionLevel, null);
    String expr = "CostDistance([abc], [def]);";
    try
    {
      MapOp root = parser.parse(expr);
      Assert.assertNotNull(root);
      Assert.assertEquals(protectionLevel, root.getProtectionLevel());
    }
    catch (FileNotFoundException e)
    {
      Assert.fail("Expected a ParseException");
    }
    catch (ParserException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("Error opening"));
    }
    catch (IOException e)
    {
      Assert.fail("Expected a ParseException");
    }
  }
}
