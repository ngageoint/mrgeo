package org.mrgeo.kernel

import java.awt.image.Raster
import java.io.{ObjectOutput, ObjectInput, Externalizable}

import org.apache.spark.Logging

abstract class Kernel(var kernelWidth:Int, var kernelHeight:Int) extends Externalizable with Logging {

  def getWidth = kernelWidth
  def getHeight = kernelHeight

  def getKernel:Option[Array[Float]]
  def get2DKernel:Option[Array[Array[Float]]]

  def calculate(tileId:Long, tile:Raster, nodatas:Array[Double]):Option[Raster]

  def this() = {
    this(-1, -1)
  }

  override def readExternal(in: ObjectInput): Unit = {
    kernelWidth = in.readInt()
    kernelHeight = in.readInt()
  }
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(kernelWidth)
    out.writeInt(kernelHeight)
  }

}
