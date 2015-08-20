/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.opchain

import java.awt.image._
import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import javax.media.jai.{PlanarImage, RenderedOp}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{ProviderProperties, DataProviderFactory, ProtectionLevelUtils}
import org.mrgeo.ingest.IngestImageSpark
import org.mrgeo.mapalgebra.{RasterMapOp, RenderedImageMapOp}
import org.mrgeo.opimage.MrsPyramidOpImage
import org.mrgeo.progress.Progress
import org.mrgeo.rasterops.{OpImageRegistrar, OpImageUtils}
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OpChainDriver extends MrGeoDriver with Externalizable {
  final private val Inputs = "inputs"
  final private val Output = "output"
  final private val OpChain = "opchain"
  final private val ImageBounds = "bounds"
  final private val Zoom = "zoom"
  final private val Protection = "protection"
  final private val ProviderProperties = "provider.properties"

  def opchain(rimop: RenderedImageMapOp, inputs: java.util.Set[String], output: String, zoom: Int, userConf: Configuration,
      progress: Progress, protectionLevel: String, providerProperties: ProviderProperties):Unit = {
    opchain(rimop, inputs, output, zoom, Bounds.world, userConf, progress, protectionLevel, providerProperties)
  }

  final def opchain(rimop: RenderedImageMapOp, inputs: java.util.Set[String], output: String, zoom: Int, bounds: Bounds,
      userConf: Configuration, progress: Progress, protectionLevel: String, providerProperties: ProviderProperties):Unit = {

    val rop: RenderedImage = rimop.asInstanceOf[RasterMapOp].getRasterOutput
    opchain(rop, inputs, output, zoom, bounds, userConf, progress, protectionLevel, providerProperties)
  }

  final def opchain(rop: RenderedImage, inputs: java.util.Set[String], output: String, zoom: Int, bounds: Bounds,
      conf: Configuration, progress: Progress, protectionLevel: String, providerProperties: ProviderProperties):Unit = {

    val now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date())
    val name = "OpChain-" + now + "-" + HadoopUtils.createRandomString(10)

    val args =  mutable.Map[String, String]()

    args += OpChainDriver.Inputs -> inputs.mkString(",")
    args += OpChainDriver.Output -> output
    args += OpChainDriver.OpChain -> Base64Utils.encodeObject(rop)

    if (bounds != null) {
      args += OpChainDriver.ImageBounds -> bounds.toDelimitedString
    }

    args += Zoom -> zoom.toString

    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(output,
      AccessMode.OVERWRITE, providerProperties)

    args += Protection -> ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel)
    args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)

    run(name, classOf[OpChainDriver].getName, args.toMap, conf)
  }

  override def setup(job: JobArguments): Boolean = {true}

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}


class OpChainDriver extends MrGeoJob with Externalizable {
  var inputs: Array[String] = null
  var output:String = null
  var zoom:Int = 0
  var optree: RenderedOp = null
  var bounds:Bounds = null
  var providerproperties:ProviderProperties = null
  var protectionlevel:String = null

  var inputOps:Array[Array[MrsPyramidOpImage]] = null

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes.result()
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    OpImageRegistrar.registerMrGeoOps()

    conf.set("spark.storage.memoryFraction", "0.2") // set the storage amount lower...
    conf.set("spark.shuffle.memoryFraction", "0.3") // set the shuffle higher

    val in = job.getSetting(OpChainDriver.Inputs)
    inputs = in.split(",")
    output = job.getSetting(OpChainDriver.Output)

    val op = job.getSetting(OpChainDriver.OpChain)
    optree = Base64Utils.decodeToObject(op).asInstanceOf[RenderedOp]

    // Force the OpChain tree to be created - invokes create method on each of the descriptors
    // in the op chain tree.
    optree.getMaxX

    zoom = job.getSetting(OpChainDriver.Zoom).toInt

    val boundstr = job.getSetting(OpChainDriver.ImageBounds, null)
    if (boundstr != null) {
      bounds = Bounds.fromDelimitedString(boundstr)
    }

    protectionlevel = job.getSetting(OpChainDriver.Protection)
    if (protectionlevel == null)
    {
      protectionlevel = ""
    }

    providerproperties = ProviderProperties.fromDelimitedString(job.getSetting(OpChainDriver.ProviderProperties))

    inputOps = findMrsPyramidOpImages()


    true
  }

  override def execute(context: SparkContext): Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    logInfo("OpChainDriver.execute")

    val pyramids = Array.ofDim[RDD[(TileIdWritable, RasterWritable)]](inputs.length)

    // loop through the inputs and load the pyramid RDDs and metadata
    for (i <- inputs.indices) {

      logInfo("Loading pyramid: " + inputs(i))
      try {
        val pyramid = SparkUtils.loadMrsPyramidAndMetadata(inputs(i), context)
        pyramids(i) = pyramid._1
      }
    }

    val tiles = if (pyramids.length == 1) {
      pyramids(0)
    }
    else {
      var maxpartitions = 0
      val partitions = pyramids.foreach(p => {
        if (p.partitions.length > maxpartitions) {
          maxpartitions = p.partitions.length
        }
      })
      new CoGroupedRDD(pyramids, new HashPartitioner(maxpartitions))
    }


    val answer = tiles.map(tile =>  {
      val key = tile._1
      val group:Seq[_] = tile._2 match {
      case rw: RasterWritable => Array(rw)
      case s: Seq[_] => s
      case a: Array[_] => a
      }

      for (i <- group.indices) {
        for (op <- inputOps(i)) {
          val rw = group(i) match {
          case s: Seq[RasterWritable] => s.head
          case r: RasterWritable => r
          }

          op.setInputRaster(key.get, RasterWritable.toRaster(rw))
        }
      }

      val sm: SampleModel = optree.getSampleModel
      val noData: Double = OpImageUtils.getNoData(optree, Double.NaN)
      val output: WritableRaster = RasterUtils
          .createEmptyRaster(sm.getWidth, sm.getHeight, sm.getNumBands, sm.getDataType, noData)
      optree.copyData(output)

//      val t = TMSUtils.tileid(key.get(), zoom)
//      val fn = "/data/export/small-elevation/tile-" + key.get() + "-" + t.ty + "-" + t.tx + ".tif"
//      GDALUtils.saveRaster(output, fn, t.tx, t.ty, zoom, output.getWidth, noData)

      (new TileIdWritable(key), RasterWritable.toWritable(output))

    })

    val dp = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.WRITE, providerproperties)

    val tile = answer.first()
    val raster = RasterWritable.toRaster(tile._2)

    SparkUtils.saveMrsPyramid(answer, dp, output, zoom, raster.getWidth, Array[Double](Double.NaN),
      context.hadoopConfiguration, DataBuffer.TYPE_FLOAT, bounds, 1, protectionlevel, providerproperties)

    true
  }

  private def walkOpTree(op: RenderedImage, sources:mutable.Map[String, ListBuffer[MrsPyramidOpImage]]):Unit = {
    // walk the sources
//    var out = ""
//    for (i <- 0 until depth) {
//      out += " "
//    }
//    out += op.getClass.getSimpleName
//    println(out)

    if (op.getSources != null) {
      for (obj <- op.getSources) {
        obj match {
        case child: RenderedImage =>
          walkOpTree(child, sources)
        case _ =>
        }
      }
    }

    op match {
    case ro: RenderedOp =>
      val image: PlanarImage = ro.getCurrentRendering
      image match {
      case mrsimage: MrsPyramidOpImage =>
        mrsimage.setZoomlevel(zoom)
        val name: String = mrsimage.getDataProvider.getResourceName
        val lb = sources.getOrElseUpdate(name, ListBuffer.empty[MrsPyramidOpImage])
        lb += mrsimage
      case _ =>
      }
      walkOpTree(image, sources)
    case _ =>
    }
  }

  private def findMrsPyramidOpImages() = {
    val sources = mutable.HashMap.empty[String, ListBuffer[MrsPyramidOpImage]]
    val opmap = walkOpTree(optree, sources)

    val ops = Array.ofDim[Array[MrsPyramidOpImage]](inputs.length)
    for (i <- inputs.indices) {
      val list = sources.getOrElse(inputs(i), null)
      if (list != null) {
        ops(i) = Array.ofDim[MrsPyramidOpImage](list.size)
        for (j <- ops(i).indices) {
          ops(i)(j) = list.get(j)
        }
      }
    }

    ops
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    OpImageRegistrar.registerMrGeoOps()

    inputs = in.readUTF().split(",")
    output = in.readUTF()
    zoom = in.readInt()

    val len = in.readInt()
    val bytes = Array.ofDim[Byte](len)
    in.readFully(bytes)

    // TODO:  This would make a great broadcast variable...
    optree = ObjectUtils.decodeObject(bytes).asInstanceOf[RenderedOp]

    // force creation of the optree
    optree.getMaxX

    inputOps = findMrsPyramidOpImages()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(inputs.mkString(","))
    out.writeUTF(output)
    out.writeInt(zoom)

    val bytes = ObjectUtils.encodeObject(optree)
    out.writeInt(bytes.length)
    out.write(bytes)
  }
}
