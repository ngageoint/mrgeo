package org.mrgeo.mapalgebra

import java.awt.image.{DataBuffer, Raster}
import java.net.URL

import org.mrgeo.data.raster.RasterUtils
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.utils.{RasterMapOpTestVerifySupport}
import org.mrgeo.utils.tms.Bounds
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Created by ericwood on 7/19/16.
  */
class CropMapOpTest extends FlatSpec with BeforeAndAfter with RasterMapOpTestVerifySupport {

  private val tileIds: Array[Long] = Array(11, 12, 19, 20)

  private var inputRaster: RasterMapOp = _
  private var subject: RasterMapOp = _

  before {
    val zoomLevel = 3
    val tileSize = 512
    inputRaster = createRasterMapOp(tileIds, zoomLevel, tileSize)
  }

  after {
    stopSparkContext
  }

  behavior of "CropMapOp"

  it should "keep all tiles when the bounds includes all tiles" in {
    subject = CropMapOp.create(inputRaster, -44.0, -44.0, 44.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(4) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, tileIds)
  }

  it should "keep the top two tiles when the bounds excludes the bottom two" in {
    subject = CropMapOp.create(inputRaster, -44.0, 1.0, 44.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, Array(19, 20))
  }

  it should "keep the right two tiles when the bounds excludes the left two" in {
    subject = CropMapOp.create(inputRaster, 1.0, -44.0, 44.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, Array(12, 20))
  }

  it should "keep the left two tiles when the bounds excludes the right two" in {
    subject = CropMapOp.create(inputRaster, -44.0, -44.0, -1.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, Array(11, 19))
  }

  it should "keep the bottom two tiles when the bounds excludes the top two" in {
    subject = CropMapOp.create(inputRaster, -44.0, -44.0, 44.0, -1.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(2) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, Array(11, 12))
  }

  it should "keep the top right tiles when the bounds excludes the other 3" in {
    subject = CropMapOp.create(inputRaster, 1.0, 1, 44.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(1) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, Array(20))
  }

  // TODO unignore this test once the issue with empty result images is addressed
  ignore should "keep no tiles when the bounds excludes all of them" in {
    subject = CropMapOp.create(inputRaster, 46.0, -44.0, 90.0, 44.0).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(0) {
      transformedRDD.count
    }
  }

  it should "set the bounds on the mapop to the crop bounds when the crop bounds are larger then the tile bounds" in {
    val (left, bottom, right, top) = (-89.0, -44.0, 44.0, 44.0)
    subject = CropMapOp.create(inputRaster, left, bottom, right, top).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(4) {
      transformedRDD.count
    }
    assertResult(-90.0) {
      subject.metadata().get.getBounds.w
    }
    verifyRastersAreUnchanged(transformedRDD, Array(11, 12, 19, 20))
  }

  it should "be able to use the bounds of another Raster as the bounds for cropping" in {
    // Create a new raster map op.  Make sure to reuse the current context since you can only have one context active
    // in a JVM
    val rasterMapOpForBounds = createRasterMapOpWithBounds(tileIds = tileIds, zoomLevel = 3, tileSize = 512,
                                                           bounds = new Bounds(10.0, 10.0, 35.0, 35.0))
    subject = CropMapOp.create(inputRaster, rasterMapOpForBounds).asInstanceOf[RasterMapOp]
    subject.execute(subject.context())
    val transformedRDD = subject.rdd().get
    assertResult(1) {
      transformedRDD.count
    }
    verifyRastersAreUnchanged(transformedRDD, Array(20))
  }
}
