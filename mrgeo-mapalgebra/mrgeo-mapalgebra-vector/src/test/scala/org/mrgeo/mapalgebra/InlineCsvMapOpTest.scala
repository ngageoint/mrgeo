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

package org.mrgeo.mapalgebra

import org.junit.experimental.categories.Category
import org.junit.{Assert, Test}
import org.mrgeo.data.ProviderProperties
import org.mrgeo.junit.UnitTest
import org.mrgeo.test.LocalRunnerTest
import org.scalatest.junit.AssertionsForJUnit

class InlineCsvMapOpTest extends LocalRunnerTest with AssertionsForJUnit
{
  val providerProperties = new ProviderProperties()

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def runTestGoodArgs(): Unit = {
    Assert.assertTrue(MapAlgebra.validate("InlineCsv(\"GEOMETRY\", \"'POINT(-117.5 38.5)'\")",
      providerProperties))
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def tooManyArgs(): Unit = {
    Assert.assertFalse(MapAlgebra.validate("InlineCsv(3, \"GEOMETRY\", \"'POINT(-117.5 38.5)'\")",
      providerProperties))
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def tooFewArgs(): Unit = {
    Assert.assertFalse(MapAlgebra.validate("InlineCsv(\"GEOMETRY\")",
      providerProperties))
  }
}
