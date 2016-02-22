package org.mrgeo.mapalgebra

import java.io.File

import junit.framework.Assert
import org.apache.hadoop.fs.Path
import org.junit.experimental.categories.Category
import org.junit.{Test, BeforeClass}
import org.mrgeo.core.Defs
import org.mrgeo.data.{ProviderProperties, DataProviderFactory}
import org.mrgeo.junit.IntegrationTest
import org.mrgeo.test.{MapOpTestUtils, LocalRunnerTest}
import org.scalatest.junit.AssertionsForJUnit

object ConvertMapOpTest
{
  def EPSILON = 1e-8
  def SAMPLED_EPSILON = 1.0
  def allHundredsName: String = "all-hundreds"
  var allHundreds: String = Defs.INPUT + allHundredsName
  var allHundredsPath: Path = null

  var testUtils: MapOpTestUtils = null

  @BeforeClass
  def init()
  {
    testUtils = new MapOpTestUtils(classOf[ConvertMapOpTest])

    var file = new File(allHundreds)
    allHundreds = "file://" + file.getAbsolutePath()
    allHundredsPath = new Path(allHundreds)
  }
}

class ConvertMapOpTest extends LocalRunnerTest with AssertionsForJUnit
{
  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testFloatToByte() : Unit =
  {
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
    val stats = metadata.getImageStats(metadata.getMaxZoomLevel, 0)
    Assert.assertEquals("Unexpected min value ", 3100000000.0, stats.min)
    Assert.assertEquals("Unexpected min value ", 3100000000.0, stats.max)
  }
}
