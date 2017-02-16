package org.mrgeo.mapalgebra.utils


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.MrGeoRaster
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.FloatUtils
import org.mrgeo.utils.tms.Bounds

/**
  * This exists to allow non-Scala code (i.e. Python) to use the methods in the RasterMapOpTestSupport trait
  */
@SuppressWarnings(Array("all")) // test code, not included in production
class StandaloneRasterMapOpTestSupport {

  def useSparkContext(context:SparkContext) {
    RasterMapOpTestSupport$.useSparkContext(context)
  }

  def stopSparkContext():Unit = {
    RasterMapOpTestSupport$.stopSparkContext
  }

  def createRasterMapOp(tileIds:Array[Long], zoomLevel:Int = 1, tileSize:Int = 512, name:String = "",
                        imageNoData:Array[Double] = Array(),
                        imageInitialData:Array[Double] = Array()):RasterMapOp = {
    getImageInitialData(imageInitialData)
    RasterMapOpTestSupport$
        .createRasterMapOp(tileIds, zoomLevel, tileSize, name, imageNoData, getImageInitialData(imageInitialData))
  }

  /**
    * Create a new RasterMapOp with the specified bounds containing rasters for the specified tiles generated using the
    * specified RasterGenerator.
    *
    * This method will create a new SparkContext if needed, or will reuse the existing SparkContext if it exists and is
    * not closed.
    *
    */
  def createRasterMapOpWithBounds(tileIds:Array[Long], zoomLevel:Int = 1, tileSize:Int = 512, name:String = "",
                                  bounds:Bounds, imageNoData:Array[Double] = Array(),
                                  imageInitialData:Array[Double] = Array()):RasterMapOp = {
    RasterMapOpTestSupport$.createRasterMapOpWithBounds(tileIds, zoomLevel, tileSize, bounds, name, imageNoData,
      getImageInitialData(imageInitialData))
  }

  def getRDD(mapOp:RasterMapOp):RDD[_] = {
    mapOp.rdd().getOrElse(mapOp.context().emptyRDD)
  }

  def verifySample(raster:MrGeoRaster, x:Int, y:Int, b:Int, expectedValue:AnyVal):Boolean = {
    raster.getPixelDouble(x, y, b) == expectedValue
  }

  def verifySamples(raster:MrGeoRaster, x:Int, y:Int, widthRange:Int, heightRange:Int, b:Int,
                    expectedValue:AnyVal):Boolean = {

    val expected = expectedValue match {
      case b:Byte => b.toDouble
      case c:Char => c.toDouble
      case s:Short => s.toDouble
      case i:Int => i.toDouble
      case f:Float => f.toDouble
      case d:Double => d.toDouble
    }

    var xx = x
    var yy = y
    while (yy < heightRange) {
      while (xx < widthRange) {
        if (!FloatUtils.isEqual(raster.getPixelDouble(xx, yy, b), expected)) {
          println("px: " + xx + " py: " + yy + " b: " + b + " expected: " + expected + " actual: " +
                  raster.getPixelDouble(x, y, b))
          return false
        }
        xx += 1
      }
      yy += 1
    }
    true
  }

  protected def getImageInitialData(imageInitialData:Array[Double]):Option[Array[Double]] = {
    val dataArray:Option[Array[Double]] = imageInitialData match {
      case Array() => Some(Array(1.0))
      case _ => Some(imageInitialData)
    }
    dataArray
  }

  object RasterMapOpTestSupport$ extends RasterMapOpTestSupport {}


}
