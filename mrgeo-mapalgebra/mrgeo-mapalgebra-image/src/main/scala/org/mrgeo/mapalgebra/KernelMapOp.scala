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

package org.mrgeo.mapalgebra

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.kernel.{GaussianGeographicKernel, LaplacianGeographicKernel}
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.utils.{LatLng, SparkUtils, TMSUtils}

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
      new GaussianGeographicKernel(sigma)
    case KernelMapOp.Laplacian =>
      new LaplacianGeographicKernel(sigma)
    }

    val res = TMSUtils.resolution(zoom, tilesize)
    val metersPerPixel = res * LatLng.METERS_PER_DEGREE
    val weights = context.broadcast(kernel.createKernel(metersPerPixel, metersPerPixel))

    val localWeights = kernel.createKernel(metersPerPixel, metersPerPixel)
    val kernelW: Int = kernel.getWidth
    val kernelH: Int = kernel.getHeight
    log.info("Kernel w, h " + kernelW + ", " + kernelH)

    for (ky <- 0 until kernelH) {
      log.info(ky + ": " )
      val sb = new StringBuffer()
      for (kx <- 0 until kernelW) {
        sb.append(localWeights(ky * kernelW + kx) + "     ")
      }
      log.info(sb.toString)
    }

    val halfKernelW = kernelW / 2 + 1
    val halfKernelH = kernelH / 2 + 1

    val resolution = TMSUtils.resolution(zoom, tilesize)
    val focal = FocalBuilder.create(rdd, halfKernelW - 1, halfKernelH - 1,
      meta.getBounds, zoom, nodatas, context)

    rasterRDD = Some(RasterRDD(focal.map(tile => {

      val startTime = System.currentTimeMillis()
      val nodata = nodatas(0).doubleValue()
      def isNodata(value:Double):Boolean = {
        if (nodata.isNaN) {
          value.isNaN
        }
        else {
          value == nodata
        }
      }

      val t = TMSUtils.tileid(tile._1.get(), zoom)
      val bounds = TMSUtils.tileBounds(t, zoom, tilesize)

      val ul = TMSUtils.latLonToPixelsUL(bounds.n, bounds.w, zoom, tilesize)
      val src = RasterWritable.toRaster(tile._2)
      // tileWidth is the pixel width of the source tile including the neighborhood.
      val tileWidth = src.getWidth
      val dst = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT)

      val useWeights = weights.value
      val srcValues = src.getSamples(0, 0, src.getWidth, src.getHeight, 0, null.asInstanceOf[Array[Double]])
      var pixelHasValue = new Array[Boolean](srcValues.length)
      var i: Int = 0
      while (i < srcValues.length) {
        pixelHasValue(i) = !isNodata(srcValues(i))
        i += 1
      }
//      var loopMin: Long = Long.MaxValue
//      var loopMax: Long = Long.MinValue
      val tileStart = System.currentTimeMillis()
      var ky: Int = 0
      var kx: Int = 0
      var result: Double = 0.0f
      var weight: Double = 0.0f
      var x: Int = 0
      var y: Int = 0
      while (y < tilesize) {
        x = 0
        while (x < tilesize) {
            if (pixelHasValue((y + halfKernelH) * tileWidth + x + halfKernelW)) {
              result = 0.0f
              weight = 0.0f

//              val loopStart = System.currentTimeMillis()
              ky = 0
              while (ky <  kernelH) {
                kx = 0
                while (kx < kernelW) {
                  val index = (y + ky) * tileWidth + x + kx
                  if (pixelHasValue(index)) {
                    val w: Double = useWeights(ky * kernelW + kx)
                    weight += w
                    result += srcValues(index) * w
                  }
                  kx += 1
                }
                ky += 1
              }
//              val loopTime = System.currentTimeMillis() - loopStart
//              loopMin = Math.min(loopMin, loopTime)
//              loopMax = Math.max(loopMax, loopTime)
              if (weight == 0.0f) {
                dst.setSample(x, y, 0, Float.NaN)
              }
              else {
                dst.setSample(x, y, 0, result / weight)
              }
            }
            else {
              dst.setSample(x, y, 0, Float.NaN)
            }
          x += 1
          }
        y += 1
      }

      log.info("Time to process tile " + tile._1.get + " is " + (System.currentTimeMillis() - startTime))
      log.info("  prep " + tile._1.get + " is " + (tileStart - startTime))
      log.info("  just tile " + tile._1.get + " is " + (System.currentTimeMillis() - tileStart))
//      log.info("  loopMin = " + loopMin + ", loopMax = " + loopMax)
      (new TileIdWritable(tile._1), RasterWritable.toWritable(dst))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, Array.fill[Number](1)(Float.NaN),
      bounds = meta.getBounds, calcStats = false))

    true
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
