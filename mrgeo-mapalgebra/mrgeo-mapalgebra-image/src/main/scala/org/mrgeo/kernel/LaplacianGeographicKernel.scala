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

class LaplacianGeographicKernel(sigma:Double, zoom:Int, tilesize:Int) extends GaussianGeographicKernel(sigma, zoom,
  tilesize) {

  // no parameter constructor, use ONLY for serialization
  def this() = {
    this(0, 0, 0)
  }

  override def runKernel(data:Mat):Unit = {
    // smooth the image 1st.
    Imgproc.GaussianBlur(data, data, new Size(kernelWidth, kernelWidth), pixelSigma) // sigma in pixel space

    // now apply the laplacian filter, this will help find edges
    Imgproc.Laplacian(data, data, data.`type`(), 3, 1.0, 0.0) // scale by 1.0, offset 0.0 (no scaling or offset)
  }

  override def readExternal(in:ObjectInput):Unit = {
    super.readExternal(in)
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    super.writeExternal(out)
  }
}
