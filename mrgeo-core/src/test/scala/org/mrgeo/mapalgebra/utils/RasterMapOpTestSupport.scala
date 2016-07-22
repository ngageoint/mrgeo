package org.mrgeo.mapalgebra.utils

import java.awt.image.{DataBuffer, Raster}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.Bounds
import org.scalatest.Assertions

/**
  * Created by ericwood on 7/20/16.
  */
trait RasterMapOpTestSupport extends Assertions {
  protected var rasterMapOpBuilder: RasterMapOpBuilder = _
  protected var sparkContext: Option[SparkContext] = None

  protected var generatedRasters = Map[Long,Raster]()

  /**
    * function that takes a tileId, tileSize, and zoomLevel and returns A Raster
    */
  type RasterGenerator = (Long, Int, Int) => Raster
  type RasterVerifier = (Raster) => Unit
  type RasterVerifiers = Map[Long, RasterVerifier]


  protected def _createRasterMapOp(tileIds: Array[Long], zoomLevel: Int = 1, tileSize: Int = 512,
                        imageNoData: Array[Double] = Array(), bounds: Option[Bounds],
                        rasterGenerator: RasterGenerator = createRaster): RasterMapOp = {
    // Local function to create builder without context
    def createWithoutContext():Unit = {
      rasterMapOpBuilder = RasterMapOpBuilder()
      sparkContext = Some(rasterMapOpBuilder.context)
    }
    sparkContext match {
      case None => createWithoutContext()
      case Some(sc) if (sc.isStopped) => createWithoutContext()
      case Some(sc) => {rasterMapOpBuilder = RasterMapOpBuilder(sc)}
    }

    tileIds.foreach(t => {
      val raster = rasterGenerator(t, tileSize, zoomLevel)
      // Store generated raster for later comparison
      generatedRasters = generatedRasters + (t -> raster)
      rasterMapOpBuilder.raster(t, raster)
    })
    if (bounds.isDefined) rasterMapOpBuilder.bounds(bounds.get)

    rasterMapOpBuilder
      .zoomLevel(zoomLevel)
      .tileSize(tileSize)
      // If no defaults were specified, use 0.0 for all bands
      .imageNoData(if (!imageNoData.isEmpty) imageNoData
                   else Array.fill[Double](generatedRasters.values.head.getNumBands)(0.0))
    .build
  }

  /**
    * Create a new RasterMapOp containing rasters for the specified tiles generated using the specified RasterGenerator.
    *
    * This method will create a new SparkContext if needed, or will reuse the existing SparkContext if it exists and is
    * not closed.
    *
    * @param tileIds
    * @param rasterGenerator
    * @return
    */
  def createRasterMapOp(tileIds: Array[Long], zoomLevel: Int = 1, tileSize: Int = 512,
                                  imageNoData: Array[Double] = Array(),
                                  rasterGenerator: RasterGenerator = createRaster): RasterMapOp = {
    _createRasterMapOp(tileIds, zoomLevel, tileSize, imageNoData, None, rasterGenerator)
  }

  /**
    * Create a new RasterMapOp with the specified bounds containing rasters for the specified tiles generated using the
    * specified RasterGenerator.
    *
    * This method will create a new SparkContext if needed, or will reuse the existing SparkContext if it exists and is
    * not closed.
    *
    * @param tileIds
    * @param rasterGenerator
    * @return
    */
  def createRasterMapOpWithBounds(tileIds: Array[Long], zoomLevel: Int = 1, tileSize: Int = 512,
                        imageNoData: Array[Double] = Array(), bounds: Bounds,
                        rasterGenerator: RasterGenerator = createRaster): RasterMapOp = {
    _createRasterMapOp(tileIds, zoomLevel, tileSize, imageNoData, Some(bounds), rasterGenerator)
  }

  def createRaster(tileId: Long, tileSize: Int, zoomLevel:Int): Raster = {
    // default implementation doesn't use tileId.  It's there for a generator that might.
    val raster = RasterUtils.createEmptyRaster(tileSize, tileSize, zoomLevel, DataBuffer.TYPE_BYTE)
    RasterUtils.fillWithNodata(raster,1.0)
    raster
  }

  // Verify the raters in the RDD using the verifiers.  If a verifier is associated with a key not found in
  // the RDD, then the method should fail the test if failOnMissingKey is true
  def verifyRasters(rdd: RDD[_], verifiers: RasterVerifiers, failOnMissingKey: Boolean = true): Unit = {
    var idsToVerify = verifiers.keySet
    rdd.toLocalIterator.foreach(element => {
      element match {
        case (tileId: TileIdWritable, raster: RasterWritable) => verifiers.get(tileId.get()) match {
          case Some(verifier) => {
            verifier(RasterWritable.toRaster(raster))
            // Record the key
            idsToVerify = idsToVerify - tileId.get()
          }
        }
      }
    })
    if (failOnMissingKey && idsToVerify.size != 0) fail("Keys missing from RDD: " + idsToVerify.mkString)
  }

  def verifyRastersAreUnchanged(rdd: RDD[_], tileIds: Array[Long]):Unit = {
//    verifyRastersAreTheSame(generatedRasters.values.head)(generatedRasters.values.head)
    val verifier: RasterVerifier = verifyRastersAreTheSame(generatedRasters.values.head)_
    val verifiers = tileIds.map(t => (t, verifier)).toMap
    verifyRasters(rdd, verifiers)
  }

  def verifyRastersAreTheSame(expected: Raster)(actual: Raster):Unit = {
    // Assert rasters are the same size
    val width = expected.getWidth();
    val height = expected.getHeight();
    val bands = expected.getNumBands();

    assertResult(width, "Raster width") {actual.getWidth()}
    assertResult(height, "Raster height") {actual.getHeight()}
    assertResult(bands, "Raster number of bands") {actual.getNumBands()}

    // Loop over every sample.
    for {
      b <- 0 to bands - 1
      y <- 0 to height - 1
      x <- 0 to width - 1} {
        // Compare the pixels for equality.
        assertResult(expected.getSample(x, y, b), s"Sample at x: $x y: $y band: $b") {actual.getSample(x, y, b)}
    }
  }

}
