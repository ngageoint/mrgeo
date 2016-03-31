package org.mrgeo.mapalgebra.raster

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.utils.{SparkUtils, TMSUtils}

abstract class RawFocalMapOp extends RasterMapOp with Externalizable {

  private[mapalgebra] var inputMapOp: Option[RasterMapOp] = None
  private var rasterRDD:Option[RasterRDD] = None

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean =
  {
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    beforeExecute(meta)
    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val tb = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(meta.getBounds), zoom, tilesize)

    val nodatas = Array.ofDim[Number](meta.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = meta.getDefaultValue(i)
    }

    val neighborhoodInfo = getNeighborhoodInfo
    val neighborhoodWidth = neighborhoodInfo._1
    val neighborhoodHeight = neighborhoodInfo._2
    val bufferX = (neighborhoodWidth / 2).toInt
    val bufferY = (neighborhoodHeight / 2).toInt

    val tiles = FocalBuilder.create(rdd, bufferX, bufferY, meta.getBounds, zoom, nodatas, context)

    rasterRDD =
      Some(RasterRDD(calculate(tiles, neighborhoodWidth, neighborhoodHeight, nodatas, zoom, tilesize)))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, Array.fill[Number](meta.getBands)(getOutputNoData),
      bounds = meta.getBounds, calcStats = false))

    true
  }

  /**
    * Compute and return the value to be assigned to the pixel at processX, processY in the
    * source raster. It is guaranteed that the value of the pixel being processed is not nodata,
    * but there is no guarantee for its neighborhood pixels.
    *
    * Note that the neighborhood width and height can be either odd or even, meaning that the pixel
    * being processed can be either in the center of the neighborhood or slightly left and/or
    * above center. For example, if the neighborhoodWidth is 3, then the processing pixel will
    * be in the middle of the neighborhood. If the neighborhoodWidth is 4, it will be the second pixel
    * from the left (e.g. xLeftOffset will be 1).
    *
    * @param rasterValues An array of the source raster including the neighborhood buffer.
    * @param notnodata An array of booleans indicating whether each pixel value in
    *                  the source raster is nodata or not. Using this array improves
    *                  performance during neighborhood calculations because the "is nodata"
    *                  checks are expensive when repeatedly run for the same pixel.
    * @param processX The x pixel coordinate in the source raster of the pixel to process
    * @param processY The y pixel coordinate in the source raster of the pixel to process
    * @param xLeftOffset Defines the left boundary of the neighborhood. This is the number of pixels
    *                    to the left of the pixel being processed.
    * @param neighborhoodWidth The width of the neighborhood in pixels.
    * @param yAboveOffset Defines the top boundary of the neighborhood. This is the number of pixels
    *                     above the pixel being processed.
    * @param neighborhoodHeight The height of the neighborhood in pixels.
    * @return
    */
  protected def computePixelValue(rasterValues: Array[Double], notnodata: Array[Boolean],
                                  rasterWidth: Int,
                                  processX: Int, processY: Int,
                                  xLeftOffset: Int, neighborhoodWidth: Int,
                                  yAboveOffset: Int, neighborhoodHeight: Int, tileId: Long): Double

  /**
    * This method is called at the start of the "execute" method, giving sub-classes an
    * opportunity to perform some processing or initialization prior to executing the
    * map op.
    *
    * @param meta
    */
  protected def beforeExecute(meta: MrsPyramidMetadata): Unit = {}

  private def isNoData(value: Double, nodata: Double): Boolean =
  {
    if (nodata.isNaN) {
      value.isNaN
    }
    else {
      (value == nodata)
    }
  }

  /**
    * Returns 2 values about the neighborhood to use (neighborhood width, neighborhood height).
    *
    * This method is called at the start of the execution of this map op.
    *
    * @return
    */
  protected def getNeighborhoodInfo: (Int, Int)

  protected def getOutputTileType: Int = {
    DataBuffer.TYPE_FLOAT
  }

  protected def getOutputNoData: Double = {
    Double.NaN
  }

  protected def calculateRasterIndex(rasterWidth: Int, x: Int, y:Int): Int =
  {
    y * rasterWidth + x
  }

  private def calculate(tiles:RDD[(TileIdWritable, RasterWritable)],
                        neighborhoodWidth: Int, neighborhoodHeight: Int,
                        nodatas:Array[Number], zoom:Int, tilesize:Int) =
  {
    val outputNoData = getOutputNoData
    tiles.map(tile => {

      val raster = RasterWritable.toRaster(tile._2)
      val answer = RasterUtils.createEmptyRaster(tilesize, tilesize, raster.getNumBands,
        getOutputTileType) // , Float.NaN)

      // If neighborhoodWidth is an odd value, then the neighborhood has the same number of pixels to the left
      // and right of the source pixel. If even, then it has one fewer pixels to the left of the
      // source value than to the right.
      val xLeftOffset = if ((neighborhoodWidth % 2) == 0) {
        (neighborhoodWidth / 2).toInt - 1
      }
      else {
        (neighborhoodWidth / 2).toInt
      }
      // If neighborhoodHeight is an odd value, then the neighborhood has the same number of pixels above and
      // below the source pixel. If even, then it has one fewer pixel above than below.
      val yAboveOffset = if ((neighborhoodHeight % 2) == 0) {
        (neighborhoodHeight / 2).toInt - 1
      }
      else {
        (neighborhoodHeight / 2).toInt
      }
      val rasterWidth = raster.getWidth
      var band: Int = 0
      while (band < raster.getNumBands) {
        val rasterValues = raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
          raster.getHeight, band, null.asInstanceOf[Array[Double]])
        // For performance, construct an array of booleans indicating whether or not each
        // pixel value in the source raster is nodata or not
        val notnodata = new Array[Boolean](rasterWidth * raster.getHeight)
        var i: Int = 0
        while (i < rasterValues.length) {
          notnodata(i) = !isNoData(rasterValues(i), nodatas(band).doubleValue())
          i += 1
        }
        var py = 0
        var px = 0
        while (py < raster.getHeight) {
          while (px < rasterWidth) {
            val index = calculateRasterIndex(rasterWidth, px, py)
            val v = raster.getSampleDouble(px, py, band)
            rasterValues(index) = v
            if (!isNoData(v, nodatas(band).doubleValue())) {
              notnodata(index) = true
            }
            else {
              notnodata(index) = false
            }
            px += 1
          }
          py += 1
        }
        var y: Int = 0
        var x: Int = 0
        while (y < tilesize) {
          x = 0
          while (x < tilesize) {
            val srcX = x + xLeftOffset
            val srcY = y + yAboveOffset
            // If the source pixel is nodata, skip it
            if (notnodata(calculateRasterIndex(rasterWidth, srcX, srcY))) {
              answer.setSample(x, y, band,
                computePixelValue(rasterValues, notnodata, rasterWidth, srcX, srcY, xLeftOffset, neighborhoodWidth,
                  yAboveOffset, neighborhoodHeight, tile._1.get()))
            }
            else {
              answer.setSample(x, y, band, outputNoData)
            }
            x += 1
          }
          y += 1
        }
        band += 1
      }
      (new TileIdWritable(tile._1), RasterWritable.toWritable(answer))
    })
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
  }

  override def writeExternal(out: ObjectOutput): Unit = {
  }
}
