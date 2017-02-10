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
import java.util.Random

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
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
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

@SuppressFBWarnings(value = Array("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION"),
  justification = "object has no constructor, empty Externalizable prevents object serialization")
@SuppressFBWarnings(value = Array("UPM_UNCALLED_PRIVATE_METHOD"), justification = "Scala constant")
@SuppressFBWarnings(value = Array("PREDICTABLE_RANDOM"),
  justification = "Use of Random has no impact on security")
object Quantiles extends MrGeoDriver with Externalizable {
  final private val Input = "input"
  final private val Output = "output"
  final private val NumQuantiles = "num.quantiles"
  final private val Fraction = "fraction"
  final private val ProviderProperties = "provider.properties"

  def compute(input:String, output:String, numQuantiles:Int,
              conf:Configuration, providerProperties:ProviderProperties):Boolean = {
    val name = "Quantiles"

    val args = setupArguments(input, output, numQuantiles, None, providerProperties)

    run(name, classOf[Quantiles].getName, args.toMap, conf)

    true
  }

  def compute(input:String, output:String, numQuantiles:Int, fraction:Float,
              conf:Configuration, providerProperties:ProviderProperties):Boolean = {
    val name = "Quantiles"

    val args = setupArguments(input, output, numQuantiles, Some(fraction), providerProperties)

    run(name, classOf[Quantiles].getName, args.toMap, conf)

    true
  }

  def getDoublePixelValues(raster: MrGeoRaster, band: Int, nodata: Double,
                           fraction: Option[Float]): ArrayBuffer[Double] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = new ArrayBuffer[Double](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        val pixelValue = raster.getPixelDouble(x, y, band)
        if (!RasterMapOp.isNodata(pixelValue, nodata)) {
          data += pixelValue
          cnt += 1
        }

        x += 1
      }
      y += 1
    }
    // If a fraction is specified, then randomly sample the data without replacement
    if (fraction.isDefined && fraction.get < 1.0f) {
      val n = data.length
      val m = math.floor(fraction.get * n).toInt
      if (m < cnt) {
        var j: Int = 0
        val r = new Random()
        while (j < m) {
          val k = r.nextInt(n - j) + j
          val keep = data(j)
          data(j) = data(k)
          data(k) = keep
          j += 1
        }
        data.reduceToSize(m)
      }
      else {
        // There aren't enough non-nodata pixels for the requested sample, so take what we get.
        data.reduceToSize(cnt)
      }
    }
    else {
      // Use all of the non-nodata pixel values
      data.reduceToSize(cnt)
    }
    data
  }

  def getFloatPixelValues(raster: MrGeoRaster, band: Int, nodata: Double, fraction: Option[Float]): ArrayBuffer[Float] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = new ArrayBuffer[Float](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        val pixelValue = raster.getPixelFloat(x, y, band)
        if (!RasterMapOp.isNodata(pixelValue, nodata)) {
          data += pixelValue
          cnt += 1
        }

        x += 1
      }
      y += 1
    }
    // If a fraction is specified, then randomly sample the data without replacement
    if (fraction.isDefined && fraction.get < 1.0f) {
      val n = data.length
      val m = math.floor(fraction.get * n).toInt
      if (m < cnt) {
        var j: Int = 0
        val r = new Random()
        while (j < m) {
          val k = r.nextInt(n - j) + j
          val keep = data(j)
          data(j) = data(k)
          data(k) = keep
          j += 1
        }
        data.reduceToSize(m)
      }
      else {
        // There aren't enough non-nodata pixels for the requested sample, so take what we get.
        data.reduceToSize(cnt)
      }
    }
    else {
      // Use all of the non-nodata pixel values
      data.reduceToSize(cnt)
    }
    data
  }

  def getIntPixelValues(raster: MrGeoRaster, band: Int, nodata: Double,
                        fraction: Option[Float]): ArrayBuffer[Int] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = new ArrayBuffer[Int](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        val pixelValue = raster.getPixelInt(x, y, band)
        if (!RasterMapOp.isNodata(pixelValue, nodata)) {
          data += pixelValue
          cnt += 1
        }

        x += 1
      }
      y += 1
    }
    // If a fraction is specified, then randomly sample the data without replacement
    if (fraction.isDefined && fraction.get < 1.0f) {
      val n = data.length
      val m = math.floor(fraction.get * n).toInt
      if (m < cnt) {
        var j: Int = 0
        val r = new Random()
        while (j < m) {
          val k = r.nextInt(n - j) + j
          val keep = data(j)
          data(j) = data(k)
          data(k) = keep
          j += 1
        }
        data.reduceToSize(m)
      }
      else {
        // There aren't enough non-nodata pixels for the requested sample, so take what we get.
        data.reduceToSize(cnt)
      }
    }
    else {
      // Use all of the non-nodata pixel values
      data.reduceToSize(cnt)
    }
    data
  }

  def getShortPixelValues(raster: MrGeoRaster, band: Int, nodata: Double,
                          fraction: Option[Float]): ArrayBuffer[Short] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = new ArrayBuffer[Short](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        val pixelValue = raster.getPixelShort(x, y, band)
        if (!RasterMapOp.isNodata(pixelValue, nodata)) {
          data += pixelValue
          cnt += 1
        }

        x += 1
      }
      y += 1
    }
    // If a fraction is specified, then randomly sample the data without replacement
    if (fraction.isDefined && fraction.get < 1.0f) {
      val n = data.length
      val m = math.floor(fraction.get * n).toInt
      if (m < cnt) {
        var j: Int = 0
        val r = new Random()
        while (j < m) {
          val k = r.nextInt(n - j) + j
          val keep = data(j)
          data(j) = data(k)
          data(k) = keep
          j += 1
        }
        data.reduceToSize(m)
      }
      else {
        // There aren't enough non-nodata pixels for the requested sample, so take what we get.
        data.reduceToSize(cnt)
      }
    }
    else {
      // Use all of the non-nodata pixel values
      data.reduceToSize(cnt)
    }
    data
  }

  def getBytePixelValues(raster: MrGeoRaster, band: Int, nodata: Double,
                         fraction: Option[Float]): ArrayBuffer[Byte] = {
    var x = 0
    var y = 0

    val width = raster.width()
    val height = raster.height()

    val data = new ArrayBuffer[Byte](width * height)

    var cnt = 0
    while (y < height) {
      x = 0
      while (x < width) {
        val pixelValue = raster.getPixelByte(x, y, band)
        if (!RasterMapOp.isNodata(pixelValue, nodata)) {
          data += pixelValue
          cnt += 1
        }

        x += 1
      }
      y += 1
    }
    // If a fraction is specified, then randomly sample the data without replacement
    if (fraction.isDefined && fraction.get < 1.0f) {
      val n = data.length
      val m = math.floor(fraction.get * n).toInt
      if (m < cnt) {
        var j: Int = 0
        val r = new Random()
        while (j < m) {
          val k = r.nextInt(n - j) + j
          val keep = data(j)
          data(j) = data(k)
          data(k) = keep
          j += 1
        }
        data.reduceToSize(m)
      }
      else {
        // There aren't enough non-nodata pixels for the requested sample, so take what we get.
        data.reduceToSize(cnt)
      }
    }
    else {
      // Use all of the non-nodata pixel values
      data.reduceToSize(cnt)
    }
    data
  }

  def compute(rdd:RasterRDD, numberOfQuantiles:Int, fraction:Option[Float], meta:MrsPyramidMetadata) = {
    var b:Int = 0
    //val dt = meta.getTileType
    var result = new ListBuffer[Array[Double]]()
    while (b < meta.getBands) {
      val nodata = meta.getDefaultValue(b)
      val sortedPixelValues: RDD[AnyVal] = meta.getTileType match {
      case DataBuffer.TYPE_DOUBLE =>
        var pixelValues = rdd.flatMap(U => {
          getDoublePixelValues(RasterWritable.toMrGeoRaster(U._2), b, nodata, fraction)
        })
        pixelValues.persist(StorageLevel.MEMORY_AND_DISK).sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case DataBuffer.TYPE_FLOAT =>
        var pixelValues = rdd.flatMap(U => {
          getFloatPixelValues(RasterWritable.toMrGeoRaster(U._2), b, nodata, fraction)
        })
        pixelValues.persist(StorageLevel.MEMORY_AND_DISK).sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case (DataBuffer.TYPE_INT | DataBuffer.TYPE_USHORT) =>
        var pixelValues = rdd.flatMap(U => {
          getIntPixelValues(RasterWritable.toMrGeoRaster(U._2), b, nodata, fraction)
        })
        pixelValues.persist(StorageLevel.MEMORY_AND_DISK).sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case DataBuffer.TYPE_SHORT =>
        var pixelValues = rdd.flatMap(U => {
          getShortPixelValues(RasterWritable.toMrGeoRaster(U._2), b, nodata, fraction)
        })
        pixelValues.persist(StorageLevel.MEMORY_AND_DISK).sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      case DataBuffer.TYPE_BYTE =>
        var pixelValues = rdd.flatMap(U => {
          getBytePixelValues(RasterWritable.toMrGeoRaster(U._2), b, nodata, fraction)
        })
        pixelValues.persist(StorageLevel.MEMORY_AND_DISK).sortBy(x => x).asInstanceOf[RDD[AnyVal]]
      }
      try {
        val count = sortedPixelValues.count()
        // Build an RDD containing an entry for each individual quantile to compute.
        // The key is the index into the set of sorted pixel values corresponding to
        // the quantile. The value is the number of the quantile itself.
        val quantileIndices = new Array[Long](numberOfQuantiles - 1)
        for (i <- quantileIndices.indices) {
          val qFraction = 1.0f / numberOfQuantiles.toFloat * (i + 1).toFloat
          quantileIndices(i) = (qFraction * count).ceil.toLong
        }
        if (count >= quantileIndices.length) {
          //        log.info("value count is " + count)
          // Add an index as the key to the sorted pixel values so we can join on that key.
          val sortedWithIndexKey = sortedPixelValues.zipWithIndex().map(_.swap)
          val count = sortedWithIndexKey.count
          val quantileValues = sortedWithIndexKey.filter(U => {
            quantileIndices.contains(U._1)
          }) //.sortByKey()
          result += quantileValues.collect().map(U => {
            U._2.toString.toDouble
          }).sorted
        }
      }
      finally {
        sortedPixelValues.unpersist()
      }
      //      else {
      //        log.warn("Unable to compute quantiles because there are only " + count + " values")
      //      }
      b += 1
    }
    result.toList
  }

  override def setup(job:JobArguments):Boolean = {
    true
  }

  override def readExternal(in:ObjectInput):Unit = {
  }

  override def writeExternal(out:ObjectOutput):Unit = {
  }

  private def setupArguments(input:String, output:String,
                             numQuantiles:Int,
                             fraction:Option[Float],
                             providerProperties:ProviderProperties):mutable.Map[String, String] = {
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
}

class Quantiles extends MrGeoJob with Externalizable {
  var providerproperties:ProviderProperties = _
  private var input:String = _
  private var output:String = _
  private var numQuantiles:Int = 0
  private var fraction:Option[Float] = None

  override def registerClasses():Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[Array[Float]]
    classes += classOf[Array[Object]]

    classes.result()
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
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

  override def execute(context:SparkContext):Boolean = {
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

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def readExternal(in:ObjectInput):Unit = {
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

  override def writeExternal(out:ObjectOutput):Unit = {
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

  private[quantiles] def this(input:String, output:String,
                              numQuantiles:Int,
                              providerProperties:ProviderProperties) = {
    this()

    this.input = input
    this.output = output
    this.numQuantiles = numQuantiles
    this.providerproperties = providerproperties
  }

  private[quantiles] def this(input:String, output:String,
                              numQuantiles:Int, fraction:Float,
                              providerProperties:ProviderProperties) = {
    this()

    this.input = input
    this.output = output
    this.numQuantiles = numQuantiles
    this.fraction = Some(fraction)
    this.providerproperties = providerproperties
  }
}
