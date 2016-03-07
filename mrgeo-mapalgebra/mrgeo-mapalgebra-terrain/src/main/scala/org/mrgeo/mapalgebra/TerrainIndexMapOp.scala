package org.mrgeo.mapalgebra

import java.awt.image.{WritableRaster, Raster, DataBuffer}
import java.io.{IOException, ObjectOutput, ObjectInput, Externalizable}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.utils.{SparkUtils, TMSUtils}

abstract class TerrainIndexMapOp extends RasterMapOp with Externalizable {

  private[mapalgebra] var inputMapOp: Option[RasterMapOp] = None
  private var rasterRDD:Option[RasterRDD] = None

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean =
  {
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val tb = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(meta.getBounds), zoom, tilesize)

    val nodatas = Array.ofDim[Number](meta.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = meta.getDefaultValue(i)
    }

    val kernelInfo = getKernelInfo
    val kernelWidth = kernelInfo._1
    val kernelHeight = kernelInfo._2
    val bufferX = (kernelWidth / 2).toInt
    val bufferY = (kernelHeight / 2).toInt

    val tiles = FocalBuilder.create(rdd, bufferX, bufferY, meta.getBounds, zoom, nodatas, context)

    rasterRDD =
      Some(RasterRDD(calculate(tiles, kernelWidth, kernelHeight, nodatas(0).doubleValue(), zoom, tilesize)))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, Array[Number](Float.NaN),
      bounds = meta.getBounds, calcStats = false))

    true
  }

  /**
    * Compute and return the value to be assigned to the pixel at processX, processY in the
    * source raster. It is guaranteed that the value of the pixel being processed is not nodata,
    * but there is no guarantee for its neighborhood pixels.
    *
    * Note that the kernel width and height can be either odd or even, meaning that the pixel
    * being processed can be either in the center of the kernel or slightly left and/or
    * above center. For example, if the kernelWidth is 3, then the processing pixel will
    * be in the middle of the kernel. If the kernelWidth is 4, it will be the second pixel
    * from the left (e.g. xLeftOffset will be 1).
    *
    * @param rasterValues An array of the source raster including the neighborhood buffer.
    * @param notnodata An array of booleans indicating whether each pixel value in
    *                  the source raster is nodata or not. Using this array improves
    *                  performance during neighborhood calculations because the "is nodata"
    *                  checks are expensive when repeatedly run for the same pixel.
    * @param processX The x pixel coordinate in the source raster of the pixel to process
    * @param processY The y pixel coordinate in the source raster of the pixel to process
    * @param xLeftOffset Defines the left boundary of the kernel. This is the number of pixels
    *                    to the left of the pixel being processed.
    * @param kernelWidth The width of the kernel in pixels.
    * @param yAboveOffset Defines the top boundary of the kernel. This is the number of pixels
    *                     above the pixel being processed.
    * @param kernelHeight The height of the kernel in pixels.
    * @return
    */
  protected def computePixelValue(rasterValues: Array[Double], notnodata: Array[Boolean],
                                  rasterWidth: Int,
                                  processX: Int, processY: Int,
                                  xLeftOffset: Int, kernelWidth: Int,
                                  yAboveOffset: Int, kernelHeight: Int, tileId: Long): Double

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
    * Returns 2 values about the kernel to use (kernel width, kernel height).
    *
    * This method is called at the start of the execution of this map op.
    *
    * @return
    */
  protected def getKernelInfo: (Int, Int)

  protected def calculateRasterIndex(rasterWidth: Int, x: Int, y:Int): Int =
  {
    y * rasterWidth + x
  }

  private def calculate(tiles:RDD[(TileIdWritable, RasterWritable)],
                        kernelWidth: Int, kernelHeight: Int,
                        nodata:Double, zoom:Int, tilesize:Int) =
  {
    tiles.map(tile => {

      val raster = RasterWritable.toRaster(tile._2)
      val answer = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT) // , Float.NaN)

      // If kernelWidth is an odd value, then the kernel has the same number of pixels to the left
      // and right of the source pixel. If even, then it has one fewer pixels to the left of the
      // source value than to the right.
      val xLeftOffset = if ((kernelWidth % 2) == 0) {
        (kernelWidth / 2).toInt - 1
      }
      else {
        (kernelWidth / 2).toInt
      }
      // If kernelHeight is an odd value, then the kernel has the same number of pixels above and
      // below the source pixel. If even, then it has one fewer pixel above than below.
      val yAboveOffset = if ((kernelHeight % 2) == 0) {
        (kernelHeight / 2).toInt - 1
      }
      else {
        (kernelHeight / 2).toInt
      }
      val rasterWidth = raster.getWidth
      val rasterValues = raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
        raster.getHeight, 0, null.asInstanceOf[Array[Double]])
      // For performance, construct an array of booleans indicating whether or not each
      // pixel value in the source raster is nodata or not
      val notnodata = new Array[Boolean](rasterWidth * raster.getHeight)
      var i: Int = 0
      while (i < rasterValues.length) {
        notnodata(i) = !isNoData(rasterValues(i), nodata)
        i += 1
      }
      var py = 0
      var px = 0
      while (py < raster.getHeight) {
        while (px < rasterWidth) {
          val index = calculateRasterIndex(rasterWidth, px, py)
          val v = raster.getSampleDouble(px, py, 0)
          rasterValues(index) = v
          if (!isNoData(v, nodata)) {
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
            answer.setSample(x, y, 0,
              computePixelValue(rasterValues, notnodata, rasterWidth, srcX, srcY, xLeftOffset, kernelWidth,
                yAboveOffset, kernelHeight, tile._1.get()))
          }
          else
          {
            answer.setSample(x, y, 0, Float.NaN)
          }
          x += 1
        }
        y += 1
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
