package org.mrgeo.mapalgebra.utils


import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.utils.tms.TMSUtils
import org.scalatest.Assertions

/**
  * Created by ericwood on 8/2/16.
  */
@SuppressWarnings(Array("all")) // test code, not included in production
trait RasterMapOpTestVerifySupport extends RasterMapOpTestSupport {

  type RasterVerifier = (MrGeoRaster) => Unit
  type RasterVerifiers = Map[Long, RasterVerifier]

  def Assertions = new Object with Assertions

  // Verify the rasters in the RDD using the verifiers.  If a verifier is associated with a key not found in
  // the RDD, then the method should fail the test if failOnMissingKey is true
  def verifyRasters(rdd: RDD[_], verifiers: RasterVerifiers, failOnMissingKey: Boolean = true): Unit = {
    var idsToVerify = verifiers.keySet
    rdd.toLocalIterator.foreach {
      case (tileId: TileIdWritable, raster: RasterWritable) => verifiers.get(tileId.get()) match {
      case Some(verifier) =>
        verifier(RasterWritable.toMrGeoRaster(raster))
        // Record the key
        idsToVerify = idsToVerify - tileId.get()
      }
      case None =>
    }
    if (failOnMissingKey && idsToVerify.size != 0) Assertions.fail("Keys missing from RDD: " + idsToVerify.mkString)
  }

  def verifyRastersAreUnchanged(rdd: RDD[_], tileIds: Array[Long]): Unit = {
    val verifier: RasterVerifier = verifyRastersAreTheSame(generatedRasters.values.head) _
    val verifiers = tileIds.map(t => (t, verifier)).toMap
    verifyRasters(rdd, verifiers)
  }

  /**
    * Verifies the rasters in the RDD do not have data outside of the specified bounds, optionally verifying the
    * expectedData inside the bounds
    *
    * @param rdd
    * @param tileIds
    * @param nodatas
    * @param left
    * @param right
    * @param top
    * @param bottom
    * @param expectedData
    */
  def verifyRastersNoData(rdd: RDD[_], tileIds: Array[Long], tileSize: Int, zoomLevel: Int, nodatas: Array[Double],
      left: Double, right: Double, top: Double, bottom: Double,
      expectedData: Option[Array[Double]] = None) = {
    val verifier = (tid: Long, r: MrGeoRaster) =>
      verifyRasterNoData(tid, zoomLevel, tileSize, r, nodatas, left, right, top, bottom, expectedData)
    val verifiers = tileIds.map(t => (t, (r: MrGeoRaster) => verifier(t, r))).toMap
    verifyRasters(rdd, verifiers)
  }

  def verifyRastersAreTheSame(expected: MrGeoRaster)(actual: MrGeoRaster): Unit = {
    // Assert rasters are the same size
    val width = expected.width()
    val height = expected.height()
    val bands = expected.bands()

    Assertions.assertResult(width, "Raster width") {actual.width()}
    Assertions.assertResult(height, "Raster height") {actual.height()}
    Assertions.assertResult(bands, "Raster number of bands") {actual.bands()}

    forEachSampleInRaster(actual, (b, x, y, sample) => {
      Assertions.assertResult(expected.getPixelDouble(x, y, b), s"Sample at x: $x y: $y band: $b") {sample}
    })
  }

  /**
    * Verifies that the data outside the the specified bounds is nodata, optionally verifying that all other samples
    * are in accordance with
    *
    * @param raster
    * @param nodatas
    * @param left
    * @param right
    * @param top
    * @param bottom
    */
  def verifyRasterNoData(tileId: Long, zoomLevel: Int, tileSize: Int, raster: MrGeoRaster, nodatas: Array[Double],
      left: Double, right: Double, top: Double, bottom: Double, expectedData: Option[Array[Double]]) = {

    // Get the tile for the id and zoom level
    val tile = TMSUtils.tileid(tileId, zoomLevel)
    // Convert lat lon bounds to tile pixel bounds
    val pixelLeftBottom = TMSUtils.latLonToTilePixelUL(bottom, left, tile.tx, tile.ty, zoomLevel, tileSize)
    val pixelRightTop = TMSUtils.latLonToTilePixelUL(top, right, tile.tx, tile.ty, zoomLevel, tileSize)
    println(s"For Tile $tileId LeftBottom bounds pixel: ${pixelLeftBottom.px}, ${pixelLeftBottom.py}. RightTop bounds pixel: ${pixelRightTop.px}, ${pixelRightTop.py}")

    val verifyExpectedData = expectedData match {
    case Some(dataArray) => (b: Int, x: Int, y: Int, sample: Double) => Unit
      if (b < dataArray.size) Assertions.assertResult(dataArray(b), s"Sample at x: $x y: $y band: $b") {
        sample
      }
    case None => (b: Int, x: Int, y: Int, sample: Double) => Unit
    }

    def verifySample(b: Int, x: Int, y: Int, sample: Double): Unit = {
      (x, y) match {
      case (x, y) if x < pixelLeftBottom.px || x > pixelRightTop.px ||
          y < pixelLeftBottom.py || y > pixelRightTop.py =>
        Assertions.assertResult(nodatas(b), s"Sample at x: $x y: $y band: $b") {sample}
      case _ => verifyExpectedData(b, x, y, sample)
      }
    }

    forEachSampleInRaster(raster, verifySample)
  }

  /**
    * Executes f for each sample in the raster
    *
    * @param raster
    * @param f function that takes band, x, y, and sample
    */
  def forEachSampleInRaster(raster: MrGeoRaster, f: (Int, Int, Int, Double) => Unit) = {
    val width = raster.width()
    val height = raster.height()
    val bands = raster.bands()
    println(s"Raster height: $height width: $width bands: $bands")

    // Loop over every sample.
    for {
      b <- 0 to bands - 1
      y <- 0 to height - 1
      x <- 0 to width - 1} {
      f(b, x, y, raster.getPixelDouble(x, y, b))
    }
  }
}
