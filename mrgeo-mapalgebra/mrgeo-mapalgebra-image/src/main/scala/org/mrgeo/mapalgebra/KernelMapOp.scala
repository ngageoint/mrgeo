package org.mrgeo.mapalgebra

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.kernel.{LaplacianGeographicKernel, GaussianGeographicKernel}
import org.mrgeo.mapalgebra.old.KernelMapOpHadoop
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.{SparkUtils, TMSUtils}

object KernelMapOp extends MapOpRegistrar {
  val MaxLatitude: Double = 60.0

  val Gaussian: String = "gaussian"
  val Laplacian: String = "laplacian"

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

  override def rdd(): Option[RasterRDD] = rasterRDD
  override def execute(context: SparkContext): Boolean = {

    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    // copy this here to avoid serializing the whole mapop

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val nodatas = Array.ofDim[Number](meta.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = meta.getDefaultValue(i)
    }

    val kernel = method match {
    case KernelMapOpHadoop.Gaussian =>
      new GaussianGeographicKernel(sigma)
    case KernelMapOpHadoop.Laplacian =>
      new LaplacianGeographicKernel(sigma)
    }

    val weights = kernel.createMaxSizeKernel(zoom, tilesize)

    val kernelW: Int = kernel.getWidth
    val kernelH: Int = kernel.getHeight

    val halfKernelW = kernelW / 2
    val halfKernelH = kernelH / 2

    val resolution = TMSUtils.resolution(zoom, tilesize)
    val focal = FocalBuilder.create(rdd, halfKernelW, halfKernelH,
      meta.getBounds, zoom, nodatas, context)

    rasterRDD = Some(RasterRDD(focal.map(tile => {

      val nodata = nodatas(0).doubleValue()
      def isNodata(value:Double):Boolean = {
        if (nodata.isNaN) {
          value.isNaN
        }
        else {
          value == nodata
        }
      }

      // kernel is not serializable, so we need to create a new one each time.
      val kernel = method match {
      case KernelMapOpHadoop.Gaussian =>
        new GaussianGeographicKernel(sigma)
      case KernelMapOpHadoop.Laplacian =>
        new LaplacianGeographicKernel(sigma)
      }

      val t = TMSUtils.tileid(tile._1.get(), zoom)
      val bounds = TMSUtils.tileBounds(t, zoom, tilesize)

      val ul = TMSUtils.latLonToPixelsUL(bounds.n, bounds.w, zoom, tilesize)
      val src = RasterWritable.toRaster(tile._2)
      val dst = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT)

      for (y <- 0 until tilesize) {
        val ll = TMSUtils.pixelToLatLonUL(ul.px, ul.py + y + kernelH, zoom, tilesize)

        if (Math.abs(ll.lat) > KernelMapOp.MaxLatitude) {
          for (x <- 0 until tilesize) {
            dst.setSample(x, y, 0, Float.NaN)
          }
        }
        else {
          //          val weights = kernel.createKernel(ll.lat, resolution, resolution)
          //
          //          val kernelW: Int = kernel.getWidth
          //          val kernelH: Int = kernel.getHeight

          for (x <- 0 until tilesize) {

            if (!isNodata(src.getSampleDouble(x + halfKernelW, y + halfKernelH, 0))) {
              var result: Double = 0.0f
              var weight: Double = 0.0f

              for (ky <- 0 until kernelH) {
                for (kx <- 0 until kernelW) {
                  val w: Double = weights(ky * kernelW + kx)
                  //println("x: " + (x + kx - halfKernelW) + " y: " + (y + ky - halfKernelH))
                  //val v: Double = src.getSampleDouble(x + kx - halfKernelW, y + ky - halfKernelH, 0)
                  val v: Double = src.getSampleDouble(x + kx, y + ky, 0)
                  if (!isNodata(v)) {
                    weight += w
                    result += v * w
                  }
                }
              }
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
          }
        }
      }

      (new TileIdWritable(tile._1), RasterWritable.toWritable(dst))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, nodatas(0).doubleValue()))

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
