package org.mrgeo.mapalgebra

import java.awt.image.{DataBuffer, Raster}
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp

import scala.language.existentials


object QuantileMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("quantiles")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new QuantileMapOp(node, true, variables)
}

class QuantileMapOp extends RasterMapOp with Externalizable {
  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None

  def this(node: ParserNode, isSlope: Boolean, variables: String => Option[ParserNode]) {
    this()
    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
  }

  override def rdd(): Option[RasterRDD] = {
    rasterRDD
  }

  override def registerClasses(): Array[Class[_]] = {
    Array[Class[_]](classOf[Array[Double]],
      classOf[Array[Float]],
      classOf[Array[Int]],
      classOf[Array[Short]],
      classOf[Array[Byte]],
      classOf[Array[Object]]
    )
  }

  override def execute(context: SparkContext): Boolean = {

    implicit val doubleOrdering = new Ordering[Double] {
      override def compare(x: Double, y: Double): Int = x.compareTo(y)
    }

    implicit val floatOrdering = new Ordering[Float] {
      override def compare(x: Float, y: Float): Int = x.compareTo(y)
    }

    implicit val intOrdering = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x.compareTo(y)
    }

    implicit val shortOrdering = new Ordering[Short] {
      override def compare(x: Short, y: Short): Int = x.compareTo(y)
    }

    implicit val byteOrdering = new Ordering[Byte] {
      override def compare(x: Byte, y: Byte): Int = x.compareTo(y)
    }

    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    rasterRDD = input.rdd()
    val rdd = rasterRDD getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))
    // No reason to calculate metadata like raster map ops that actually compute a raster. This
    // map op does not compute the raster output, it just uses the input raster. All we need to
    // do is update the metadata already computed for the input raster map op.
//    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, meta.getDefaultValues,
//      bounds = meta.getBounds, calcStats = false))

    val quantiles = Array[Float](0.25f, 0.5f, 0.75f)
    var b: Int = 0
    val dt = meta.getTileType
    while (b < meta.getBands) {
      val nodata = meta.getDefaultValue(b)
      val sortedPixelValues = meta.getTileType match {
        case DataBuffer.TYPE_DOUBLE => {
          val pixelValues = rdd.flatMap(U => {
            getDoublePixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            !RasterMapOp.isNodata(value, nodata)
          })
          pixelValues.sortBy(x => x)
        }
        case DataBuffer.TYPE_FLOAT => {
          val pixelValues = rdd.flatMap(U => {
            getFloatPixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            !RasterMapOp.isNodata(value, nodata)
          })
          pixelValues.sortBy(x => x)
        }
        case (DataBuffer.TYPE_INT | DataBuffer.TYPE_USHORT) => {
          val pixelValues = rdd.flatMap(U => {
            getIntPixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            value != nodata.toInt
          })
          pixelValues.sortBy(x => x)
        }
        case DataBuffer.TYPE_SHORT => {
          val pixelValues = rdd.flatMap(U => {
            getShortPixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            value != nodata.toShort
          })
          pixelValues.sortBy(x => x)
        }
        case DataBuffer.TYPE_BYTE => {
          val pixelValues = rdd.flatMap(U => {
            getBytePixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            value != nodata.toByte
          })
          pixelValues.sortBy(x => x)
        }
      }
      val count = sortedPixelValues.count()
      if (count >= quantiles.length) {
        log.info("value count is " + count)
        // Add an index to the sorted pixel values, but we want it as the key instead
        // of the value.
        val sortedWithIndexKey = sortedPixelValues.zipWithIndex().map(_.swap)
        val quantileValues = new Array[Double](quantiles.length)
        quantiles.zipWithIndex.foreach(q => {
          val quantileKey: Long = (q._1 * count).ceil.toLong
          val quantileValue = sortedWithIndexKey.lookup(quantileKey).head.toString.toDouble
          log.info("quantile " + q._1 + " is at index " + quantileKey + " and has value " + quantileValue)
          quantileValues(q._2) = quantileValue
        })
        log.error("Setting quantiles for band " + b + " to:")
        quantileValues.foreach(v => {
          log.error("  " + v)
        })
        meta.setQuantiles(b, quantileValues)
      }
      else {
        log.warn("Unable to compute quantiles because there are only " + count + " values")
      }
      b += 1
    }
    metadata(meta)
    true
  }

  def getDoublePixelValues(raster: Raster, band: Int): Array[Double] = {
    raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
      raster.getHeight, band, null.asInstanceOf[Array[Double]])
  }

  def getFloatPixelValues(raster: Raster, band: Int): Array[Float] = {
    raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
      raster.getHeight, band, null.asInstanceOf[Array[Float]])
  }

  def getIntPixelValues(raster: Raster, band: Int): Array[Int] = {
    raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
      raster.getHeight, band, null.asInstanceOf[Array[Int]])
  }

  def getShortPixelValues(raster: Raster, band: Int): Array[Short] = {
    val intValues = raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
      raster.getHeight, band, null.asInstanceOf[Array[Int]])
    intValues.map(U => {
      U.toShort
    })
  }

  def getBytePixelValues(raster: Raster, band: Int): Array[Byte] = {
    val intValues = raster.getSamples(raster.getMinX, raster.getMinY, raster.getMinX + raster.getWidth,
      raster.getHeight, band, null.asInstanceOf[Array[Int]])
    intValues.map(U => {
      U.toByte
    })
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}