package org.mrgeo.mapalgebra

import java.io.File

import junit.framework.Assert
import org.junit._
import org.junit.experimental.categories.Category
import org.mrgeo.core.Defs
import org.mrgeo.data.ProviderProperties
import org.mrgeo.junit.{IntegrationTest, UnitTest}
import org.mrgeo.mapalgebra.parser.ParserException
import org.mrgeo.test.{LocalRunnerTest, MapOpTestUtils}
import org.scalatest.junit.AssertionsForJUnit

object BandExtractMapOpTest
{
  def all_ones = "all-ones"
  def small_3band = "small-3band"
  var all_ones_input = Defs.INPUT + all_ones
  var small_3band_input = Defs.INPUT + small_3band

  var testUtils: MapOpTestUtils = null

  @BeforeClass
  def init()
  {
    testUtils = new MapOpTestUtils(classOf[BandExtractMapOpTest])

    var file = new File(all_ones_input)
    all_ones_input = "file://" + file.getAbsolutePath()
    file = new File(small_3band_input)
    small_3band_input = "file://" + file.getAbsolutePath()
  }
}

class BandExtractMapOpTest extends LocalRunnerTest with AssertionsForJUnit
{
  // only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
  def GEN_BASELINE_DATA_ONLY = false

//  private static final Logger log = LoggerFactory.getLogger(BandExtractMapOpTest.class)

  @Before
  def setup(): Unit =
  {
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testMissingAllArgs() : Unit =
  {
    val exp = s"BandExtract()"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("requires two arguments"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testMissingBandArg() : Unit =
  {
    val exp = s"BandExtract([$BandExtractMapOpTest.all_ones_input])"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("requires two arguments"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testMissingImageArg() : Unit =
  {
    val exp = s"BandExtract(0)"
    try
    {
      MapAlgebra.validateWithExceptions(exp, ProviderProperties.fromDelimitedString(""))
      Assert.fail("Should have gotten a ParserException")
    }
    catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue(e.getMessage.contains("requires two arguments"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testBadImageArg() : Unit =
  {
    val exp = "BandExtract(\"abc\", 0)"
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
  @Category(Array[Class[_]] { classOf[UnitTest] })
  def testBadImageArg2() : Unit =
  {
    val exp = "BandExtract([abc], 0)"
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
  def testBandTooLarge() : Unit =
  {
    try {
      val exp = s"BandExtract([${BandExtractMapOpTest.all_ones_input}], 10)"
      if (GEN_BASELINE_DATA_ONLY) {
        BandExtractMapOpTest.testUtils.generateBaselineTif(conf,
          testname.getMethodName, exp, -9999)
      }
      else {
        BandExtractMapOpTest.testUtils.runRasterExpression(conf,
          testname.getMethodName, exp)
      }
    } catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue("Unexpected message: " + e.getMessage,
          e.getMessage.contains("Cannot extract band"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testFromSingleBandImage() : Unit =
  {
    try {
      val exp = s"BandExtract([${BandExtractMapOpTest.all_ones_input}], 1)"
      if (GEN_BASELINE_DATA_ONLY) {
        BandExtractMapOpTest.testUtils.generateBaselineTif(conf,
          testname.getMethodName, exp, -9999)
      }
      else {
        BandExtractMapOpTest.testUtils.runRasterExpression(conf,
          testname.getMethodName, exp)
      }
    } catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue("Unexpected message: " + e.getMessage,
          e.getMessage.contains("Cannot extract band"))
      }
    }
  }

  @Test
  @Category(Array[Class[_]] { classOf[IntegrationTest] })
  def testFromMultiBandImage() : Unit =
  {
    try {
      val exp = s"BandExtract([${BandExtractMapOpTest.small_3band_input}], 3 )"
      if (GEN_BASELINE_DATA_ONLY) {
        BandExtractMapOpTest.testUtils.generateBaselineTif(conf,
          testname.getMethodName, exp, -9999)
      }
      else {
        BandExtractMapOpTest.testUtils.runRasterExpression(conf,
          testname.getMethodName, exp)
      }
    } catch {
      case e: ParserException => {
        // Verify the content of the error message
        Assert.assertTrue("Unexpected message: " + e.getMessage,
          e.getMessage.contains("Cannot extract band"))
      }
    }
  }
}
