/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.quantiles

import java.awt.image.DataBuffer
import java.io.{Externalizable, ObjectInput, ObjectOutput, PrintWriter}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data
import org.mrgeo.data.ProviderProperties
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

@SuppressFBWarnings(value = Array("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION"),
  justification = "object has no constructor, empty Externalizable prevents object serialization")
@SuppressFBWarnings(value = Array("UPM_UNCALLED_PRIVATE_METHOD"), justification = "Scala constant")
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
      providerProperties: ProviderProperties): mutable.Map[String, String] = {
    val args = mutable.Map[String, String]()

    args += Input -> input
    args += Output -> output
    args += NumQuantiles -> numQuantiles.toString
    if (fraction.isDefined) {
      args += Fraction -> fraction.get.toString
    }

    if (providerProperties != null) {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
    }
    else {
      args += ProviderProperties -> ""
    }

    args
  }

  def getDoublePixelValues(raster: MrGeoRaster, band: Int): Array[Double] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = Array.ofDim[Double](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        data(cnt) = raster.getPixelDouble(x, y, band)
        cnt += 1

        x += 1
      }
      y += 1
    }

    data
  }

  def getFloatPixelValues(raster: MrGeoRaster, band: Int): Array[Float] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = Array.ofDim[Float](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        data(cnt) = raster.getPixelFloat(x, y, band)

        cnt += 1

        x += 1
      }
      y += 1
    }

    data
  }

  def getIntPixelValues(raster: MrGeoRaster, band: Int): Array[Int] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = Array.ofDim[Int](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        data(cnt) = raster.getPixelInt(x, y, band)
        cnt += 1

        x += 1
      }
      y += 1
    }

    data
  }

  def getShortPixelValues(raster: MrGeoRaster, band: Int): Array[Short] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = Array.ofDim[Short](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        data(cnt) = raster.getPixelShort(x, y, band)
        cnt += 1

        x += 1
      }
      y += 1
    }

    data
  }

  def getBytePixelValues(raster: MrGeoRaster, band: Int): Array[Byte] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = Array.ofDim[Byte](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        data(cnt) = raster.getPixelByte(x, y, band)
        cnt += 1

        x += 1
      }
      y += 1
    }

    data
  }

  def compute(rdd: RasterRDD, numberOfQuantiles: Int, fraction: Option[Float], meta: MrsPyramidMetadata) = {
    var b: Int = 0
    //val dt = meta.getTileType
    var result = new ListBuffer[Array[Double]]()
    while (b < meta.getBands) {
      val nodata = meta.getDefaultValue(b)
      val sortedPixelValues: RDD[AnyVal] = meta.getTileType match {
      case DataBuffer.TYPE_DOUBLE =>
        var pixelValues = rdd.flatMap(U => {
          getDoublePixelValues(RasterWritable.toMrGeoRaster(U._2), b)
        }).filter(value => {
          !RasterMapOp.isNodata(value, nodata)
        })
        if (fraction.isDefined && fraction.get < 1.0f) {
          pixelValues = pixelValues.sample(false, fraction.get)
        }
        pixelValues.sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case DataBuffer.TYPE_FLOAT =>
        var pixelValues = rdd.flatMap(U => {
          getFloatPixelValues(RasterWritable.toMrGeoRaster(U._2), b)
        }).filter(value => {
          !RasterMapOp.isNodata(value, nodata)
        })
        println(pixelValues.count())
        if (fraction.isDefined && fraction.get < 1.0f) {
          pixelValues = pixelValues.sample(false, fraction.get)
        }
        pixelValues.sortBy(x => x).asInstanceOf[RDD[AnyVal]]

      case (DataBuffer.TYPE_INT | DataBuffer.TYPE_USHORT) =>
        var pixelValues = rdd.flatMap(U => {
          getIntPixelValues(RasterWritable.toMrGeoRaster(U._2), b)
        }).filter(value => {
          value != nodata.toInt
        })
        if (fraction.isDefined && fraction.get < 1.0f) {
          pixelValues = pixelValues.sample(false, fraction.get)
        }
        pixelValues.sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case DataBuffer.TYPE_SHORT =>
        var pixelValues = rdd.flatMap(U => {
          getShortPixelValues(RasterWritable.toMrGeoRaster(U._2), b)
        }).filter(value => {
          value != nodata.toShort
        })
        if (fraction.isDefined && fraction.get < 1.0f) {
          pixelValues = pixelValues.sample(false, fraction.get)
        }
        pixelValues.sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case DataBuffer.TYPE_BYTE =>
        var pixelValues = rdd.flatMap(U => {
          getBytePixelValues(RasterWritable.toMrGeoRaster(U._2), b)
        }).filter(value => {
          value != nodata.toByte
        })
        if (fraction.isDefined && fraction.get < 1.0f) {
          pixelValues = pixelValues.sample(false, fraction.get)
        }
        pixelValues.sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      }
      val count = sortedPixelValues.count()
      // Build an RDD containing an entry for each individual quantile to compute.
      // The key is the index into the set of sorted pixel values corresponding to
      // the quantile. The value is the number of the quantile itself.
      val quantiles = new Array[(Long, Int)](numberOfQuantiles - 1)
      for (i <- quantiles.indices) {
        val qFraction = 1.0f / numberOfQuantiles.toFloat * (i + 1).toFloat
        quantiles(i) = ((qFraction * count).ceil.toLong, i)
      }
      val quantilesRdd = new PairRDDFunctions(rdd.context.parallelize(quantiles))
      if (count >= quantiles.length) {
        //        log.info("value count is " + count)
        // Add an index as the key to the sorted pixel values so we can join on that key.
        val sortedWithIndexKey = sortedPixelValues.zipWithIndex().map(_.swap)
        val quantileValues = new Array[Double](quantiles.length)
        // Join the two RDD's and the results contain an entry for each quantile
        // with the quantile number and the pixel value corresponding to that quantile.
        val joined = quantilesRdd.join(sortedWithIndexKey)
        val localJoined = joined.collect()
        localJoined.foreach(q => {
          quantileValues(q._2._1) = q._2._2.toString.toDouble
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
  private var input: String = _
  private var output: String = _
  private var numQuantiles: Int = 0
  private var fraction: Option[Float] = None
  var providerproperties: ProviderProperties = _

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
        result.head.foreach(v => {
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
