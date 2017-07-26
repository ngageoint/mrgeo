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

import java.awt.image.DataBuffer
import java.io.File

import junit.framework.Assert
import org.apache.hadoop.fs.Path
import org.junit.experimental.categories.Category
import org.junit.{BeforeClass, Test}
import org.mrgeo.core.Defs
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.junit.{IntegrationTest, UnitTest}
import org.mrgeo.mapalgebra.parser.ParserException
import org.mrgeo.test.{LocalRunnerTest, MapOpTestUtils}
import org.scalatest.junit.AssertionsForJUnit

@SuppressWarnings(Array("all")) // Test code, not included in production
object ConvertMapOpTest {
  var allHundreds:String = Defs.INPUT + allHundredsName
  var allHundredsPath:Path = _
  var testUtils:MapOpTestUtils = _

  def EPSILON = 1e-8

  def SAMPLED_EPSILON = 1.0

  def allHundredsName = "all-hundreds"

  @BeforeClass
  def init() {
    testUtils = new MapOpTestUtils(classOf[ConvertMapOpTest])

    val file = new File(allHundreds)
    allHundreds = "file://" + file.getAbsolutePath()
    allHundredsPath = new Path(allHundreds)
  }
}

class ConvertMapOpTest extends LocalRunnerTest with AssertionsForJUnit {
  @Test
  @Category(Array[Class[_]] {
    classOf[UnitTest]
  })
  def testNoArgs():Unit = {
    val exp = s"convert()"
    try {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e:ParserException =>
        // Verify the content of the error message
        Assert.assertTrue("Got unexpected exception message: " + e.getMessage,
          e.getMessage.contains("convert usage"))
    }
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[UnitTest]
  })
  def testMissingImage():Unit = {
    val exp = "convert(\"float32\", \"truncate\")"
    try {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e:ParserException =>
        // Verify the content of the error message
        Assert.assertTrue("Got unexpected exception message: " + e.getMessage,
          e.getMessage.contains("is not a raster input"))
    }
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[UnitTest]
  })
  def testMissingType():Unit = {
    val exp = s"convert([%s])"
    try {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e:ParserException =>
        // Verify the content of the error message
        Assert.assertTrue("Got unexpected exception message: " + e.getMessage,
          e.getMessage.contains("convert usage"))
    }
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToByteWithMod():Unit = {
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"byte\", \"mod\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_BYTE, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 3100000000.0 % 254.0, stats.min)
    Assert.assertEquals("Unexpected max value ", 3100000000.0 % 254.0, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToByteWithTruncateMax():Unit = {
    // Start with a float32 layer with very large values outside the range of byte and
    // make sure it truncates to the max byte value properly.
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"byte\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_BYTE, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 254.0, stats.min)
    Assert.assertEquals("Unexpected max value ", 254.0, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToByteWithTruncateMin():Unit = {
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * -1000000.0 - 3000000000.0, \"byte\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_BYTE, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 0.0, stats.min)
    Assert.assertEquals("Unexpected max value ", 0.0, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToShortWithMod():Unit = {
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"short\", \"mod\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_SHORT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 3100000000.0 % Short.MaxValue, stats.min)
    Assert.assertEquals("Unexpected max value ", 3100000000.0 % Short.MaxValue, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToShortWithTruncateMax():Unit = {
    // Start with a float32 layer with very large values outside the range of Short and
    // make sure it truncates to the max Short value properly.
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"short\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_SHORT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", Short.MaxValue.toDouble, stats.min)
    Assert.assertEquals("Unexpected max value ", Short.MaxValue.toDouble, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToShortWithTruncateMin():Unit = {
    // Start with a float32 layer with very small values outside the range of Short and
    // make sure it truncates to the min Short value properly.
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * -1000000.0 - 3000000000.0, \"short\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_SHORT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", (Short.MinValue + 1).toDouble, stats.min)
    Assert.assertEquals("Unexpected max value ", (Short.MinValue + 1).toDouble, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToIntWithMod():Unit = {
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"int\", \"mod\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_INT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 3100000000.0 % Int.MaxValue, stats.min)
    Assert.assertEquals("Unexpected max value ", 3100000000.0 % Int.MaxValue, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToIntWithTruncateMax():Unit = {
    // Start with a float32 layer with very large values outside the range of Int and
    // make sure it truncates to the max Int value properly.
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"int\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_INT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", Int.MaxValue.toDouble, stats.min)
    Assert.assertEquals("Unexpected max value ", Int.MaxValue.toDouble, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToIntWithTruncateMin():Unit = {
    // Start with a float32 layer with very small values outside the range of Int and
    // make sure it truncates to the min Int value properly.
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * -1000000.0 - 3000000000.0, \"int\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_INT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", (Int.MinValue + 1).toDouble, stats.min)
    Assert.assertEquals("Unexpected max value ", (Int.MinValue + 1).toDouble, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToFloat64WithMod():Unit = {
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String.format("convert([%s] * 1000000.0 + 3000000000.0, \"float64\", \"truncate\")",
        ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_DOUBLE, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 3100000000.0, stats.min)
    Assert.assertEquals("Unexpected max value ", 3100000000.0, stats.max)
  }

  @Test
  @Category(Array[Class[_]] {
    classOf[IntegrationTest]
  })
  def testFloat32ToFloat64AndBackWithMod():Unit = {
    ConvertMapOpTest.testUtils.runMapAlgebraExpression(this.conf, testname.getMethodName,
      String
          .format("convert(convert([%s] * 1000000.0 + 3000000000.0, \"float64\", \"truncate\"), \"float32\", \"mod\")",
            ConvertMapOpTest.allHundreds))
    val output = new Path(ConvertMapOpTest.testUtils.getOutputHdfs, testname.getMethodName).toUri.toString
    val dataProvider = DataProviderFactory.getMrsImageDataProvider(output, DataProviderFactory.AccessMode.READ,
      new ProviderProperties())
    Assert.assertNotNull("Unable to get data provider", dataProvider)
    val metadataReader = dataProvider.getMetadataReader
    Assert.assertNotNull("Unable to get metadataReader", metadataReader)
    val metadata = metadataReader.read()
    Assert.assertNotNull("Unable to read metadata", metadata)
    Assert.assertEquals("Unexpected image data type", DataBuffer.TYPE_FLOAT, metadata.getTileType)
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 3100000000.0, stats.min)
    Assert.assertEquals("Unexpected max value ", 3100000000.0, stats.max)
  }
}
