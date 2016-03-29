package org.mrgeo.quantiles

import java.awt.image.{DataBuffer, Raster}
import java.io.{Externalizable, ObjectInput, ObjectOutput, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data
import org.mrgeo.data.ProviderProperties
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Quantiles extends MrGeoDriver with Externalizable {
  final private val Input = "input"
  final private val Output = "output"
  final private val NumQuantiles = "num.quantiles"
  final private val Fraction = "fraction"
  final private val ProviderProperties = "provider.properties"

  def compute(input: String, output: String, numQuantiles: Int,
              conf: Configuration, providerProperties: ProviderProperties): Boolean = {
    val name = "Quantiles"

    val args = setupArguments(input, output, numQuantiles, None, providerProperties)

    run(name, classOf[Quantiles].getName, args.toMap, conf)

    true
  }

  def compute(input: String, output: String, numQuantiles: Int, fraction: Float,
              conf: Configuration, providerProperties: ProviderProperties): Boolean = {
    val name = "Quantiles"

    val args = setupArguments(input, output, numQuantiles, Some(fraction), providerProperties)

    run(name, classOf[Quantiles].getName, args.toMap, conf)

    true
  }

  private def setupArguments(input: String, output: String,
                             numQuantiles: Int,
                             fraction: Option[Float],
                             providerProperties: ProviderProperties):mutable.Map[String, String] = {
    val args = mutable.Map[String, String]()

    args += Input -> input
    args += Output -> output
    args += NumQuantiles -> numQuantiles.toString
    if (fraction.isDefined) {
      args += Fraction -> fraction.get.toString
    }

    if (providerProperties != null)
    {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
    }
    else
    {
      args += ProviderProperties -> ""
    }

    args
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

  def compute(rdd: RasterRDD, numberOfQuantiles: Int, fraction: Option[Float], meta: MrsPyramidMetadata) = {
    val quantiles = new Array[Float](numberOfQuantiles - 1)
    for (i <- quantiles.indices) {
      quantiles(i) = 1.0f / numberOfQuantiles.toFloat * (i + 1).toFloat
    }

    var b: Int = 0
    val dt = meta.getTileType
    var result = new ListBuffer[Array[Double]]()
    while (b < meta.getBands) {
      val nodata = meta.getDefaultValue(b)
      val sortedPixelValues = meta.getTileType match {
        case DataBuffer.TYPE_DOUBLE => {
          var pixelValues = rdd.flatMap(U => {
            getDoublePixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            !RasterMapOp.isNodata(value, nodata)
          })
          if (fraction.isDefined && fraction.get < 1.0f) {
            pixelValues = pixelValues.sample(false, fraction.get)
          }
          pixelValues.sortBy(x => x)
        }
        case DataBuffer.TYPE_FLOAT => {
          var pixelValues = rdd.flatMap(U => {
            getFloatPixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            !RasterMapOp.isNodata(value, nodata)
          })
          if (fraction.isDefined && fraction.get < 1.0f) {
            pixelValues = pixelValues.sample(false, fraction.get)
          }
          pixelValues.sortBy(x => x)
        }
        case (DataBuffer.TYPE_INT | DataBuffer.TYPE_USHORT) => {
          var pixelValues = rdd.flatMap(U => {
            getIntPixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            value != nodata.toInt
          })
          if (fraction.isDefined && fraction.get < 1.0f) {
            pixelValues = pixelValues.sample(false, fraction.get)
          }
          pixelValues.sortBy(x => x)
        }
        case DataBuffer.TYPE_SHORT => {
          var pixelValues = rdd.flatMap(U => {
            getShortPixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            value != nodata.toShort
          })
          if (fraction.isDefined && fraction.get < 1.0f) {
            pixelValues = pixelValues.sample(false, fraction.get)
          }
          pixelValues.sortBy(x => x)
        }
        case DataBuffer.TYPE_BYTE => {
          var pixelValues = rdd.flatMap(U => {
            getBytePixelValues(RasterWritable.toRaster(U._2), b)
          }).filter(value => {
            value != nodata.toByte
          })
          if (fraction.isDefined && fraction.get < 1.0f) {
            pixelValues = pixelValues.sample(false, fraction.get)
          }
          pixelValues.sortBy(x => x)
        }
      }
      val count = sortedPixelValues.count()
      if (count >= quantiles.length) {
//        log.info("value count is " + count)
        // Add an index to the sorted pixel values, but we want it as the key instead
        // of the value.
        val sortedWithIndexKey = sortedPixelValues.zipWithIndex().map(_.swap)
        val quantileValues = new Array[Double](quantiles.length)
        quantiles.zipWithIndex.foreach(q => {
          val quantileKey: Long = (q._1 * count).ceil.toLong
          val quantileValue = sortedWithIndexKey.lookup(quantileKey).head.toString.toDouble
//          log.info("quantile " + q._1 + " is at index " + quantileKey + " and has value " + quantileValue)
          quantileValues(q._2) = quantileValue
        })
//        if (log.isInfoEnabled) {
//          log.info("Setting quantiles for band " + b + " to:")
//          quantileValues.foreach(v => {
//            log.info("  " + v)
//          })
//        }
        result += quantileValues
      }
//      else {
//        log.warn("Unable to compute quantiles because there are only " + count + " values")
//      }
      b += 1
    }
    result.toList
  }

  override def setup(job: JobArguments): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
  }

  override def writeExternal(out: ObjectOutput): Unit = {
  }
}

class Quantiles extends MrGeoJob with Externalizable {
  private var input: String = null
  private var output: String = null
  private var numQuantiles: Int = 0.toInt
  private var fraction: Option[Float] = None
  var providerproperties:ProviderProperties = null

  private[quantiles] def this(input: String, output: String,
                                 numQuantiles: Int,
                                 providerProperties: ProviderProperties) = {
    this()

    this.input = input
    this.output = output
    this.numQuantiles = numQuantiles
    this.providerproperties = providerproperties
  }

  private[quantiles] def this(input: String, output: String,
                                 numQuantiles: Int, fraction: Float,
                                 providerProperties: ProviderProperties) = {
    this()

    this.input = input
    this.output = output
    this.numQuantiles = numQuantiles
    this.fraction = Some(fraction)
    this.providerproperties = providerproperties
  }

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[Array[Float]]
    classes += classOf[Array[Object]]

    classes.result()
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    input = job.getSetting(Quantiles.Input)
    output = job.getSetting(Quantiles.Output)
    numQuantiles = job.getSetting(Quantiles.NumQuantiles).toInt
    fraction = if (job.hasSetting(Quantiles.Fraction)) {
      Some(job.getSetting(Quantiles.Fraction).toFloat)
    }
    else {
      None
    }
    providerproperties = ProviderProperties.fromDelimitedString(
      job.getSetting(Quantiles.ProviderProperties))

    true
  }

  override def execute(context: SparkContext): Boolean = {
    val imagePair = SparkUtils.loadMrsPyramidAndMetadata(input, context)
    val result = org.mrgeo.quantiles.Quantiles.compute(imagePair._1, numQuantiles, fraction, imagePair._2)
    // Write the quantiles to the specified HDFS output file
    val p = new Path(output)
    val fs = HadoopFileUtils.getFileSystem(context.hadoopConfiguration, p)
    val os = fs.create(p, true)
    val pw = new PrintWriter(os)
    try {
      if (result.length == 1) {
        result(0).foreach(v => {
          pw.println("" + v)
        })
      }
      else {
        result.zipWithIndex.foreach(v => {
          pw.println("Band " + (v._2 + 1))
          v._1.foreach(q => {
            pw.println("  " + q)
          })
        })
      }
      true
    }
    finally {
      pw.close()
    }
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    input = in.readUTF()
    output = in.readUTF()
    numQuantiles = in.readInt()
    val hasFraction = in.readBoolean()
    if (hasFraction) {
      fraction = Some(in.readFloat())
    }
    val hasProviderProperties = in.readBoolean()
    if (hasProviderProperties) {
      providerproperties = ProviderProperties.fromDelimitedString(in.readUTF())
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(input)
    out.writeUTF(output)
    out.writeInt(numQuantiles)
    out.writeBoolean(fraction.isDefined)
    if (fraction.isDefined) {
      out.writeFloat(fraction.get)
    }
    out.writeBoolean(providerproperties != null)
    if (providerproperties != null) {
      out.writeUTF(ProviderProperties.toDelimitedString(providerproperties))
    }
  }
}
