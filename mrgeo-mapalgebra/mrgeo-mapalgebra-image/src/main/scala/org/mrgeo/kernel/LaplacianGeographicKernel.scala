package org.mrgeo.kernel

import java.io.{ObjectOutput, ObjectInput}

import org.opencv.core.{CvType, Size, Mat}
import org.opencv.imgproc.Imgproc

class LaplacianGeographicKernel(sigma:Double, zoom:Int, tilesize:Int) extends GaussianGeographicKernel(sigma, zoom, tilesize) {

  // no parameter constructor, use ONLY for serialization
  def this() = {
    this(0, 0, 0)
  }

  override def runKernel(data: Mat): Unit = {
    // smooth the image 1st.
    Imgproc.GaussianBlur(data, data, new Size(kernelWidth, kernelWidth), pixelSigma) // sigma in pixel space

    // now apply the laplacian filter, this will help find edges
    Imgproc.Laplacian(data, data, data.`type`(), 3, 1.0, 0.0) // scale by 1.0, offset 0.0 (no scaling or offset)
  }

  override def readExternal(in: ObjectInput): Unit = {
    super.readExternal(in)
  }
  override def writeExternal(out: ObjectOutput): Unit = {
    super.writeExternal(out)
  }
}
