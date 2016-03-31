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

package org.mrgeo.mapalgebra

import java.awt.image.{Raster, DataBuffer}
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.kernel.{Kernel, LaplacianGeographicKernel, GaussianGeographicKernel}
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.utils._

import org.mrgeo.utils.MrGeoImplicits._


object KernelMapOp extends MapOpRegistrar {
  val MaxLatitude: Double = 60.0

  val Gaussian: String = "gaussian"
  val Laplacian: String = "laplacian"

  def create(raster:RasterMapOp, method:String, sigma:Double):MapOp =
    new KernelMapOp(Some(raster), method, sigma)

  override def register: Array[String] = {
    Array[String]("kernel")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new KernelMapOp(node, variables)
}

class KernelMapOp extends RasterMapOp with Externalizable {
  private var rasterRDD:Option[RasterRDD] = None

  private var method:String = null
  private var sigma:Double = 0
  private var inputMapOp:Option[RasterMapOp] = None

  private[mapalgebra] def this(raster:Option[RasterMapOp], method:String, sigma:Double) = {
    this()
    inputMapOp = raster
    this.sigma = sigma

    method.toLowerCase match {
    case KernelMapOp.Gaussian =>
    case KernelMapOp.Laplacian =>
    case _ => throw new ParserException("Bad kernel method: " + method)
    }
  }

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 3) {
      throw new ParserException("Usage: kernel(<method>, <raster>, <params ...>)")
    }

    method = MapOp.decodeString(node.getChild(0), variables) match  {
    case Some(s) => s.toLowerCase
    case _ => throw new ParserException("Error decoding string")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(1), variables)


    method match {
    case KernelMapOp.Gaussian =>
      if (node.getNumChildren != 3) {
        throw new ParserException(
          method + " takes two additional arguments. (source raster, and sigma (in meters))")
      }
      sigma = MapOp.decodeDouble(node.getChild(2), variables) match  {
      case Some(d) => d
      case _ => throw new ParserException("Error decoding double")
      }
    case KernelMapOp.Laplacian =>
      if (node.getNumChildren != 3) {
        throw new ParserException(
          method + " takes two additional arguments. (source raster, and sigma (in meters))")
      }
      sigma = MapOp.decodeDouble(node.getChild(2), variables) match  {
      case Some(d) => d
      case _ => throw new ParserException("Error decoding double")
      }
    }
  }

  override def registerClasses(): Array[Class[_]] = {
    Array[Class[_]](classOf[Array[Float]])
  }

  override def rdd(): Option[RasterRDD] = rasterRDD
  override def execute(context: SparkContext): Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val nodatas = Array.ofDim[Number](meta.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = meta.getDefaultValue(i)
    }


    val kernel = method match {
    case KernelMapOp.Gaussian =>
      new GaussianGeographicKernel(sigma, zoom, tilesize)
    case KernelMapOp.Laplacian =>
      new LaplacianGeographicKernel(sigma, zoom, tilesize)
    }

    val halfKernelW = kernel.getWidth / 2 + 1
    val halfKernelH = kernel.getHeight / 2 + 1

    val focal = FocalBuilder.create(rdd, halfKernelW - 1, halfKernelH - 1,
      meta.getBounds, zoom, nodatas, context)

    rasterRDD = Some(RasterRDD(kernel.getKernel match {
    case Some(kernelData) =>
      naiveKernel(focal, kernel, nodatas, context)
    case _ =>
      val metersPerPixel = TMSUtils.resolution(zoom, tilesize) * LatLng.METERS_PER_DEGREE

      focal.flatMap(tile => {
        kernel.calculate(tile._1.get(), RasterWritable.toRaster(tile._2), nodatas) match {
        case Some(r:Raster) => Array((tile._1, RasterWritable.toWritable(r))).iterator
        case _ => Array.empty[(TileIdWritable, RasterWritable)].iterator
        }
      })
    }))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, Array.fill[Number](1)(Float.NaN),
      bounds = meta.getBounds, calcStats = false))

    true
  }

  def naiveKernel(focal:RDD[(TileIdWritable, RasterWritable)], kernel:Kernel, nodatas:Array[Float],
      context: SparkContext):RDD[(TileIdWritable, RasterWritable)] = {

    val weights = context.broadcast(kernel.getKernel.get)

    val kernelW = kernel.getWidth
    val kernelH = kernel.getHeight

    if (log.isDebugEnabled()) {
      //          val localWeights = kernel.getKernel
      //          log.info("Kernel w, h " + kernelW + ", " + kernelH)
      //          for (ky <- 0 until kernelH) {
      //            // log.info(ky + ": ")
      //            val sb = new StringBuffer()
      //            for (kx <- 0 until kernelW) {
      //              sb.append(localWeights(ky * kernelW + kx) + "     ")
      //            }
      //            log.info(sb.toString)
      //          }
    }

    val halfKernelW = kernelW / 2 + 1
    val halfKernelH = kernelH / 2 + 1

    //val resolution = TMSUtils.resolution(zoom, tilesize)

    val startmaptime = System.currentTimeMillis()

    focal.map(tile => {
      val startTime = System.currentTimeMillis()

      val logging = log.isInfoEnabled

      val nodata = nodatas(0).doubleValue()
      def isNodata(value: Double): Boolean = {
        if (nodata.isNaN) {
          value.isNaN
        }
        else {
          value == nodata
        }
      }

      val src = RasterWritable.toRaster(tile._2)
      val tileWidth = src.getWidth

      val tilesize = tileWidth - kernelW + 1

      val dst = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT)
      val dstValues = Array.fill[Float](tilesize * tilesize)(Float.NaN)

      val useWeights = weights.value

      val srcValues = src.getSamples(0, 0, src.getWidth, src.getHeight, 0, null.asInstanceOf[Array[Float]])
      val notNodataValues = new Array[Boolean](srcValues.length)


      var i: Int = 0
      while (i < srcValues.length) {
        if (isNodata(srcValues(i))) {
          srcValues(i) = 0
          notNodataValues(i) = false
        }
        else {
          notNodataValues(i) = true
        }
        i += 1
      }
      val tileStart = System.currentTimeMillis()

      var loopMin: Long = Long.MaxValue
      var loopMax: Long = Long.MinValue
      var loopTot: Long = 0
      var loopRuns: Long = 0

      var y: Int = 0
      var x: Int = 0
      var result: Float = 0.0f
      var weight: Float = 0.0f
      var kx: Int = 0
      var ky: Int = 0
      while (y < tilesize) {
        x = 0
        val off = (y + halfKernelH) * tileWidth + halfKernelW
        while (x < tilesize) {
          if (notNodataValues(off + x)) {
            val loopStart = System.currentTimeMillis()

            result = 0.0f
            weight = 0.0f

            ky = 0
            while (ky < kernelH) {
              kx = 0
              val off2 = (y + ky) * tileWidth + x
              val off3 = ky * kernelW
              while (kx < kernelW) {
                val w = useWeights(off3 + kx)
                if (w != 0.0) {
                  weight += w
                  result += srcValues(off2 + kx) * w
                }
                kx += 1
              }
              ky += 1
            }
            if (logging) {
              val loopTime = System.currentTimeMillis() - loopStart
              loopMin = Math.min(loopMin, loopTime)
              loopMax = Math.max(loopMax, loopTime)
              loopTot += loopTime
              loopRuns += 1
            }
            if (weight != 0.0) {
              dstValues(y * tilesize + x) = result / weight
            }
          }
          x += 1
        }
        y += 1
      }

      dst.setSamples(0, 0, tilesize, tilesize, 0, dstValues)

      if (log.isDebugEnabled()) {
        val endTime = System.currentTimeMillis()
        logDebug("Time to process tile " + tile._1.get + " is " + (endTime - startTime))
        logDebug("  prep " + (tileStart - startTime))
        logDebug("  loop " + loopTot)
        logDebug("  tile " + (endTime - tileStart - loopTot))
        logDebug("  loopMin = " + loopMin + ", loopMax = " + loopMax +
            ", loopAvg = " + (loopTot.toDouble / loopRuns) + ", loopRuns = " + loopRuns)
      }

      (tile._1, RasterWritable.toWritable(dst))
    })
  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = true
  override def teardown(job: JobArguments, conf:SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
    method = in.readUTF()
    sigma = in.readDouble()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(method)
    out.writeDouble(sigma)
  }

}
