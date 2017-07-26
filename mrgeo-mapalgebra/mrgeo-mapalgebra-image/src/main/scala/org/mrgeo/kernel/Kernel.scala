/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.kernel

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.mrgeo.data.raster.MrGeoRaster
import org.mrgeo.utils.Logging

abstract class Kernel(var kernelWidth:Int, var kernelHeight:Int) extends Externalizable with Logging {

  def getWidth:Int = kernelWidth

  def getHeight:Int = kernelHeight

  def getKernel:Option[Array[Float]]

  def get2DKernel:Option[Array[Array[Float]]]

  def calculate(tileId:Long, tile:MrGeoRaster, nodatas:Array[Double]):Option[MrGeoRaster]

  def this() = {
    this(-1, -1)
  }

  override def readExternal(in:ObjectInput):Unit = {
    kernelWidth = in.readInt()
    kernelHeight = in.readInt()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeInt(kernelWidth)
    out.writeInt(kernelHeight)
  }

}
