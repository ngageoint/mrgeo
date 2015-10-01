package org.mrgeo.mapalgebra

import org.junit.experimental.categories.Category
import org.junit.{Assert, Test}
import org.mrgeo.data.ProviderProperties
import org.mrgeo.junit.UnitTest
import org.scalatest.junit.AssertionsForJUnit

class InlineCsvMapOpTest extends /*SparkLocalRunnerTest with*/ AssertionsForJUnit
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
