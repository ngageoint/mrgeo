package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.utils.RasterMapOpTestVerifySupport
import org.mrgeo.utils.tms.Bounds
import org.scalatest.{BeforeAndAfter, FlatSpec}

@SuppressWarnings(Array("all")) // Test code, not included in production
class CropExactMapOpTest extends FlatSpec with BeforeAndAfter with RasterMapOpTestVerifySupport {

  private val tileIds = Array(11l, 12l, 19l, 20l)

  private var inputRaster:RasterMapOp = _
  private var subject:RasterMapOp = _
  private var zoomLevel = 3
  private var tileSize = 512
  private var initialData = 1.0
  private var expectedData = Array(initialData)

  before {
    inputRaster = createRasterMapOp(tileIds = tileIds, zoomLevel = zoomLevel, tileSize = tileSize,
      imageInitialData = Some(expectedData))
  }

  after {
    stopSparkContext
  }

  behavior of "CropExactMapOp"

  it should "keep all tiles when the bounds includes all tiles" in {
    val (left, bottom, right, top) = (-44.0, -44.0, 44.0, 44.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(4) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(11, 12, 19, 20), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top, Some(expectedData))
  }

  it should "keep the top two tiles when the bounds excludes the bottom two" in {
    val (left, bottom, right, top) = (-44.0, 1.0, 44.0, 44.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(19, 20), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  it should "keep the right two tiles when the bounds excludes the left two" in {
    val (left, bottom, right, top) = (1.0, -44.0, 44.0, 44.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(12, 20), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  it should "keep the left two tiles when the bounds excludes the right two" in {
    val (left, bottom, right, top) = (-44.0, -44.0, -1.0, 44.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(11, 19), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  it should "keep the bottom two tiles when the bounds excludes the top two" in {
    val (left, bottom, right, top) = (-44.0, -44.0, 44.0, -1.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(11, 12), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  it should "keep the top right tiles when the bounds excludes the other 3" in {
    val (left, bottom, right, top) = (1.0, 1.0, 44.0, 44.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(1) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(20), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  // TODO unignore this test once the issue with empty result images is addressed
  ignore should "keep no tiles when the bounds excludes all of them" in {
    subject = CropExactMapOp.create(inputRaster, 46.0, -44.0, 90.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(0) {
      transformedRDD.count
    }
  }

  it should
  "set the bounds on the mapop and expand the image to fill the bounds when the crop bounds are larger then the tile bounds" in
  {
    val (left, bottom, right, top) = (-89.0, -44.0, 44.0, 44.0)
    subject = CropExactMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(4) {
      transformedRDD.count
    }
    assertResult(left) {
      subject.metadata().get.getBounds.w
    }
    //    verifyRastersAreUnchanged(transformedRDD, Array(11, 12, 19, 20))
    verifyRastersNoData(transformedRDD, Array(11, 12, 19, 20), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  it should "be able to use the bounds of another Raster as the bounds for cropping" in {
    val (left, bottom, right, top) = (1.0, 1.0, 44.0, 44.0)
    // Create a new raster map op.  Make sure to reuse the current context since you can only have one context active
    // in a JVM
    val rasterMapOpForBounds = createRasterMapOpWithBounds(tileIds = tileIds, zoomLevel = 3, tileSize = 512,
      bounds = new Bounds(left, bottom, right, top))
    subject = CropExactMapOp.create(inputRaster, rasterMapOpForBounds).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(1) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(20), tileSize, zoomLevel,
      inputRaster.metadata().get.getDefaultValues, left, right, bottom, top)
  }

  it should "fill all bands of the image outside of the bounds with nodata" in {
    val (left, bottom, right, top) = (-44.0, -44.0, 44.0, 44.0)
    val imageData = Array(1.0, 2.0, 3.0)
    val rasterMapOpWithBands = createRasterMapOp(tileIds = tileIds, zoomLevel = 3, tileSize = 512,
      imageInitialData = Some(imageData))
    subject = CropExactMapOp.create(rasterMapOpWithBands, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(4) {
      transformedRDD.count
    }
    verifyRastersNoData(transformedRDD, Array(11, 12, 19, 20), tileSize, zoomLevel,
      rasterMapOpWithBands.metadata().get.getDefaultValues, left, right, bottom, top, Some(imageData))
  }
}
