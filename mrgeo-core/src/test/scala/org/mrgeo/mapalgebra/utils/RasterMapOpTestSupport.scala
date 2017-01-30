package org.mrgeo.mapalgebra.utils

import java.awt.image.DataBuffer

import org.apache.spark.SparkContext
import org.mrgeo.data.raster.MrGeoRaster
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.Bounds

/**
  * Created by ericwood on 7/20/16.
  */
@SuppressWarnings(Array("all")) // test code, not included in production
trait RasterMapOpTestSupport {
  /**
    * function that takes a tileId, tileSize, and zoomLevel and returns A Raster
    */
  type ImageDataArray = Array[Double]
  type RasterGenerator = (Long, Int, Int, Option[ImageDataArray]) => MrGeoRaster
  protected var rasterMapOpBuilder:RasterMapOpBuilder = _
  protected var sparkContext:Option[SparkContext] = None
  protected var generatedRasters = Map[Long, MrGeoRaster]()

  def useSparkContext(context:SparkContext) {
    this.sparkContext = Some(context)
  }

  def stopSparkContext:Unit = {
    // Stop the context if it is defined
    sparkContext.foreach(_.stop)
    sparkContext = None
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
  def createRasterMapOp(tileIds:Array[Long], zoomLevel:Int = 1, tileSize:Int = 512, imageName:String = "",
                        imageNoData:Array[Double] = Array(),
                        imageInitialData:Option[ImageDataArray] = Some(Array(1.0)),
                        rasterGenerator:RasterGenerator = createRaster):RasterMapOp = {
    _createRasterMapOp(tileIds, zoomLevel, tileSize, None, imageName, imageNoData, imageInitialData, rasterGenerator)
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
  def createRasterMapOpWithBounds(tileIds:Array[Long], zoomLevel:Int = 1, tileSize:Int = 512, bounds:Bounds,
                                  imageName:String = "", imageNoData:Array[Double] = Array(),
                                  imageInitialData:Option[ImageDataArray] = Some(Array(1.0)),
                                  rasterGenerator:RasterGenerator = createRaster):RasterMapOp = {
    _createRasterMapOp(tileIds, zoomLevel, tileSize, Some(bounds), imageName, imageNoData, imageInitialData,
      rasterGenerator)
  }

  def createRaster(tileId:Long, tileSize:Int, zoomLevel:Int,
                   imageInitialData:Option[ImageDataArray]):MrGeoRaster = imageInitialData match {
    // default implementation doesn't use tileId.  It's there for a generator that might.
    case Some(dataArray) =>
      val raster = MrGeoRaster.createEmptyRaster(tileSize, tileSize, dataArray.length, DataBuffer.TYPE_DOUBLE)
      raster.fill(dataArray)

      raster
    case None =>
      MrGeoRaster.createEmptyRaster(tileSize, tileSize, 1, DataBuffer.TYPE_DOUBLE)
  }

  protected def _createRasterMapOp(tileIds:Array[Long], zoomLevel:Int = 1, tileSize:Int = 512,
                                   bounds:Option[Bounds], imageName:String, imageNoData:Array[Double] = Array(),
                                   imageInitialData:Option[ImageDataArray],
                                   rasterGenerator:RasterGenerator = createRaster):RasterMapOp = {
    // Local function to create builder without context
    def createWithoutContext():Unit = {
      rasterMapOpBuilder = RasterMapOpBuilder()
      sparkContext = Some(rasterMapOpBuilder.context)
    }

    sparkContext match {
      case None => createWithoutContext()
      case Some(sc) => {
        rasterMapOpBuilder = RasterMapOpBuilder(sc)
      }
    }

    tileIds.foreach(t => {
      val raster = rasterGenerator(t, tileSize, zoomLevel, imageInitialData)
      // Store generated raster for later comparison
      generatedRasters = generatedRasters + (t -> raster)
      rasterMapOpBuilder.raster(t, raster)
    })
    if (bounds.isDefined) {
      rasterMapOpBuilder.bounds(bounds.get)
    }

    rasterMapOpBuilder
        .zoomLevel(zoomLevel)
        .tileSize(tileSize)
        // If no defaults were specified, use 0.0 for all bands
        .imageNoData(if (!imageNoData.isEmpty) {
      imageNoData
    }
    else {
      Array.fill[Double](generatedRasters.values.head.bands())(0.0)
    })
        .imageName(imageName)
        .build
  }

}


