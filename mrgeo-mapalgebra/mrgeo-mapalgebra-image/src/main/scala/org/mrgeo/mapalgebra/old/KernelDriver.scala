package org.mrgeo.mapalgebra.old

import java.awt.image.DataBuffer
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{DataProviderFactory, ProtectionLevelUtils, ProviderProperties}
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.kernel.{LaplacianGeographicKernel, GeographicKernel, GaussianGeographicKernel}
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils.{SparkUtils, TMSUtils}

import scala.collection.mutable

object KernelDriver extends MrGeoDriver with Externalizable {
  private val MaxLatitude: Double = 60.0

  final val Input = "input"
  final val Output = "output"
  final val Method = "method"
  final val Sigma = "sigma"
  final val ProviderProperties = "providerproperties"
  final val Protection = "protectionlevel"

  def gaussian(input:String, output:String, sigma: Double, protectionLevel: String, providerProperties: ProviderProperties, conf:Configuration) = {

    val args =  mutable.Map[String, String]()

    val name = "Kernel (" + KernelMapOpHadoop.Gaussian + ", " + input + ")"

    args += Method -> KernelMapOpHadoop.Gaussian
    args += Input -> input
    args += Output -> output
    args += Sigma -> sigma.toString

    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(output,
      AccessMode.OVERWRITE, providerProperties)

    args += Protection -> ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel)
    args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)

    run(name, classOf[KernelDriver].getName, args.toMap, conf)
  }

  def laplacian(input:String, output:String, sigma:Double, protectionLevel: String, providerProperties: ProviderProperties, conf:Configuration) = {
    val args =  mutable.Map[String, String]()

    val name = "Kernel (" + KernelMapOpHadoop.Laplacian + ", " + input + ")"

    args += Method -> KernelMapOpHadoop.Laplacian
    args += Input -> input
    args += Output -> output
    args += Sigma -> sigma.toString

    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(output,
      AccessMode.OVERWRITE, providerProperties)

    args += Protection -> ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel)
    args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)

    run(name, classOf[KernelDriver].getName, args.toMap, conf)
  }

  override def setup(job: JobArguments): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}

class KernelDriver extends MrGeoJob with Externalizable {
  var input:String = null
  var output:String = null

  var method:String = null
  var sigma:Double = 0

  var geographicKernel:GeographicKernel = null

  var providerproperties:ProviderProperties = null
  var protectionlevel:String = null

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes.result()
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    input = job.getSetting(KernelDriver.Input)
    output = job.getSetting(KernelDriver.Output)

    method = job.getSetting(KernelDriver.Method)

    method match {
    case KernelMapOpHadoop.Gaussian | KernelMapOpHadoop.Laplacian =>
      sigma = job.getSetting(KernelDriver.Sigma).toDouble
    }

    protectionlevel = job.getSetting(KernelDriver.Protection)
    if (protectionlevel == null)
    {
      protectionlevel = ""
    }

    providerproperties = ProviderProperties.fromDelimitedString(job.getSetting(KernelDriver.ProviderProperties))

    true
  }

  override def execute(context: SparkContext): Boolean = {

    val ip = DataProviderFactory.getMrsImageDataProvider(input, AccessMode.READ, providerproperties)

    val metadata: MrsImagePyramidMetadata = ip.getMetadataReader.read
    val zoom = metadata.getMaxZoomLevel
    val tilesize = metadata.getTilesize

    val nodatas = Array.ofDim[Number](metadata.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = metadata.getDefaultValue(i)
    }

    val pyramid = SparkUtils.loadMrsPyramidRDD(ip, zoom, context)

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
    val focal = FocalBuilder.create(pyramid, halfKernelW, halfKernelH,
      metadata.getBounds, zoom, nodatas, context)

    val answer = focal.map(tile => {

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

        if (Math.abs(ll.lat) > KernelDriver.MaxLatitude) {
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
    })

    val op = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.WRITE, providerproperties)

    SparkUtils.saveMrsPyramidRDD(answer, op, zoom, tilesize, Array[Double](Float.NaN),
      context.hadoopConfiguration, DataBuffer.TYPE_FLOAT, metadata.getBounds, bands = 1,
      protectionlevel = metadata.getProtectionLevel)

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    method = in.readUTF()
    sigma = in.readDouble()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(method)
    out.writeDouble(sigma)
  }
}
