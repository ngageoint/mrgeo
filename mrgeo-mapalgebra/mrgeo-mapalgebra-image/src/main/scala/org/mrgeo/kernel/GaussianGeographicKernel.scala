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

import java.io.{ObjectInput, ObjectOutput}

import org.opencv.core.{Mat, Size}
import org.opencv.imgproc.Imgproc

class GaussianGeographicKernel(sigma:Double, zoom:Int, tilesize:Int) extends GaussianLaplacianGeographicKernel(sigma,
  3, zoom, tilesize) {

  var pixelSigma:Double = Math.max(sigma / kernelWidth, sigma / kernelHeight)

  // no parameter constructor, use ONLY for serialization
  def this() = {
    this(0, 0, 0)
  }

  override def runKernel(data:Mat):Unit = {
    Imgproc.GaussianBlur(data, data, new Size(kernelWidth, kernelWidth), pixelSigma) // sigma in pixel space
  }

  override def readExternal(in:ObjectInput):Unit = {
    super.readExternal(in)
    pixelSigma = in.readDouble()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    super.writeExternal(out)
    out.writeDouble(pixelSigma)
  }

}
