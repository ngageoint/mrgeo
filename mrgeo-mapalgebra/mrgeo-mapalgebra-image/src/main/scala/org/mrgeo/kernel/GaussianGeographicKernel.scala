package org.mrgeo.kernel

import java.io.{ObjectOutput, ObjectInput}

import org.opencv.core.{Mat, Size}
import org.opencv.imgproc.Imgproc

class GaussianGeographicKernel(sigma:Double, zoom:Int, tilesize:Int) extends GaussianLaplacianGeographicKernel(sigma, 3, zoom, tilesize) {

  var pixelSigma:Double = Math.max(sigma / kernelWidth, sigma / kernelHeight)

  // no parameter constructor, use ONLY for serialization
  def this() = {
    this(0, 0, 0)
  }

  override def runKernel(data: Mat): Unit = {
    Imgproc.GaussianBlur(data, data, new Size(kernelWidth, kernelWidth), pixelSigma) // sigma in pixel space
  }

  override def readExternal(in: ObjectInput): Unit = {
    super.readExternal(in)
    pixelSigma = in.readDouble()
  }
  override def writeExternal(out: ObjectOutput): Unit = {
    super.writeExternal(out)
    out.writeDouble(pixelSigma)
  }

}
