package org.mrgeo.mapalgebra.utils


import java.awt.image.Raster

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.Bounds

/**
  * This exists to allow non-Scala code (i.e. Python) to use the methods in the RasterMapOpTestSupport trait
  */
class StandaloneRasterMapOpTestSupport {
  object RasterMapOpTestSupport$ extends RasterMapOpTestSupport {}

  def useSparkContext(context: SparkContext) {
    RasterMapOpTestSupport$.useSparkContext(context)
  }

  def stopSparkContext(): Unit = {
    RasterMapOpTestSupport$.stopSparkContext
  }

  def createRasterMapOp(tileIds: Array[Long], zoomLevel: Int = 1, tileSize: Int = 512,
                        imageNoData: Array[Double] = Array(),
                        imageInitialData: Array[Double] = Array()): RasterMapOp = {
    getImageInitialData(imageInitialData)
    RasterMapOpTestSupport$.createRasterMapOp(tileIds, zoomLevel, tileSize, imageNoData, getImageInitialData(imageInitialData))
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
  def createRasterMapOpWithBounds(tileIds: Array[Long], zoomLevel: Int = 1, tileSize: Int = 512, bounds: Bounds,
                                  imageNoData: Array[Double] = Array(),
                                  imageInitialData: Array[Double] = Array()): RasterMapOp = {
    RasterMapOpTestSupport$.createRasterMapOpWithBounds(tileIds, zoomLevel, tileSize, bounds, imageNoData,
                                      getImageInitialData(imageInitialData))
  }

  def getRDD(mapOp: RasterMapOp): RDD[_] = {
    mapOp.rdd().getOrElse(mapOp.context().emptyRDD)
  }

  def verifySample(raster: Raster, x: Int, y:Int, b: Int, expectedValue: AnyVal): Boolean = {
    raster.getSample(x, y, b) == expectedValue
  }

  def verifySamples(raster: Raster, x: Int, y:Int, widthRange: Int, heightRange: Int, b: Int,
                    expectedValue: AnyVal): Boolean = {
    val samples = raster.getSamples(x, y, widthRange, heightRange, b, null.asInstanceOf[Array[Double]])
    samples.forall(_ == expectedValue)
  }

  protected def getImageInitialData(imageInitialData: Array[Double]): Option[Array[Double]] = {
    val dataArray: Option[Array[Double]] = imageInitialData match {
      case Array() => Some(Array(1.0))
      case _ => Some(imageInitialData)
    }
    dataArray
  }


}
