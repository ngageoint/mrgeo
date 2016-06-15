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

package org.mrgeo.mapalgebra

import java.io.{File, IOException}

import junit.framework.Assert
import org.apache.hadoop.fs.Path
import org.junit.{BeforeClass, Test}
import org.junit.experimental.categories.Category
import org.mrgeo.core.Defs
import org.mrgeo.data.{ProviderProperties, DataProviderFactory}
import org.mrgeo.junit.{IntegrationTest, UnitTest}
import org.mrgeo.mapalgebra.parser.ParserException
import org.mrgeo.test.{TestUtils, MapOpTestUtils, LocalRunnerTest}
import org.scalatest.junit.AssertionsForJUnit

object QuantilesMapOpTest
{
  def EPSILON = 1e-8
  def SAMPLED_EPSILON = 1.0
  def smallElevationName: String = "small-elevation-nopyramids"
  var smallElevation: String = Defs.INPUT + smallElevationName
  var smallElevationPath: Path = null

  var testUtils: MapOpTestUtils = null

  @BeforeClass
  def init()
  {
    testUtils = new MapOpTestUtils(classOf[QuantilesMapOpTest])

    var file = new File(smallElevation)
    smallElevation = "file://" + file.getAbsolutePath()
    smallElevationPath = new Path(smallElevation)
  }
}

class QuantilesMapOpTest extends LocalRunnerTest with AssertionsForJUnit
{
  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testNoArgs() : Unit =
  {
    val exp = s"quantiles()"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("quantiles usage"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testMissingNumQuantileArg() : Unit =
  {
    val exp = s"quantiles([${QuantilesMapOpTest.smallElevation}])"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("quantiles usage"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testMissingImageArg() : Unit =
  {
    val exp = s"quantiles(4)"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("quantiles usage"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testBadNumQuantilesValue() : Unit =
  {
    val exp = s"""quantiles([${QuantilesMapOpTest.smallElevation}], \"bad-arg\")"""
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("The value for the numQuantiles parameter must be an integer"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testBadFractionValue() : Unit =
  {
    val exp = s"""quantiles([${QuantilesMapOpTest.smallElevation}], 10, \"bad-arg\")"""
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("The value for the fraction parameter must be a number"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testBadImageArg() : Unit =
  {
    val exp = "quantiles(\"abc\", 4)"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue("Unexpected message: " + e.getMessage,
          e.getMessage.contains("is not a raster input"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testNumQuantilesTooBig() : Unit =
  {
    try {
      val exp = s"quantiles([${QuantilesMapOpTest.smallElevation}], 2000000)"
      QuantilesMapOpTest.testUtils.runMapAlgebraExpression(conf,
        testname.getMethodName, exp)
    } catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue("Unexpected message: " + e.getMessage,
          e.getMessage.contains("Unable to compute quantiles because there are only"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testFractionNumQuantilesTooBig() : Unit =
  {
    try {
      val exp = s"quantiles([${QuantilesMapOpTest.smallElevation}], 20000, 0.01)"
      QuantilesMapOpTest.testUtils.runMapAlgebraExpression(conf,
        testname.getMethodName, exp)
    } catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue("Unexpected message: " + e.getMessage,
          e.getMessage.contains("Unable to compute quantiles because there are only"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testAllPixelsQuartiles() : Unit =
  {
    QuantilesMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("quantiles([%s], 4)", QuantilesMapOpTest.smallElevation))
    // Validate the quantiles in the resulting metadata
    val output = new Path(QuantilesMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    val quantiles = metadata.getQuantiles(0)
    Assert.assertEquals(67.483246, quantiles(0), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(87.27524, quantiles(1), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(110.95636, quantiles(2), QuantilesMapOpTest.EPSILON)
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testAllPixelsDeciles() : Unit =
  {
    QuantilesMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("quantiles([%s], 10)", QuantilesMapOpTest.smallElevation))
    // Validate the quantiles in the resulting metadata
    val output = new Path(QuantilesMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    val quantiles = metadata.getQuantiles(0)
    Assert.assertEquals(56.122494, quantiles(0), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(63.68695, quantiles(1), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(71.33959, quantiles(2), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(79.16016, quantiles(3), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(87.27524, quantiles(4), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(95.93457, quantiles(5), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(105.90043, quantiles(6), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(118.21525, quantiles(7), QuantilesMapOpTest.EPSILON)
    Assert.assertEquals(156.05188, quantiles(8), QuantilesMapOpTest.EPSILON)
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testRandomPixelsQuartiles() : Unit =
  {
    QuantilesMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("quantiles([%s], 4, 0.5)", QuantilesMapOpTest.smallElevation))
    // Validate the quantiles in the resulting metadata
    val output = new Path(QuantilesMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    val quantiles = metadata.getQuantiles(0)
    // Make sure each quantile value is different than what would be expected
    // from the quantile computation based on the full set of data. But they should
    // at least be within a close range of those values.
    // NOTE: theoretically, the SAMPLED_EPSILON should be calculated using some
    // statistical principals.
    validateQuantileEstimate(67.483246, 0, quantiles)
    Assert.assertEquals(67.483246, quantiles(0), QuantilesMapOpTest.SAMPLED_EPSILON)
    validateQuantileEstimate(87.27524, 1, quantiles)
    Assert.assertEquals(87.27524, quantiles(1), QuantilesMapOpTest.SAMPLED_EPSILON)
    validateQuantileEstimate(110.95636, 2, quantiles)
    Assert.assertEquals(110.95636, quantiles(2), QuantilesMapOpTest.SAMPLED_EPSILON)
  }

  private def validateQuantileEstimate(exactValue: Double, quantile:Int, quantiles: Array[Double]): Unit = {
    Assert.assertTrue(s"Quantile ${quantile + 1} should not match the exact quantile value",
      ((quantiles(quantile) < (exactValue - QuantilesMapOpTest.EPSILON)) ||
        (quantiles(quantile) > (exactValue + QuantilesMapOpTest.EPSILON))))
  }
}
