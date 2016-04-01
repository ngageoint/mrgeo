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

package org.mrgeo.kernel

import java.awt.image.{DataBuffer, Raster}

import org.mrgeo.data.raster.RasterUtils
import org.mrgeo.utils.tms.TMSUtils
import TMSUtils.LatLon
import org.mrgeo.utils.{LatLng, OpenCVUtils}
import org.opencv.core.{CvType, Mat}

abstract class GaussianLaplacianGeographicKernel(kernelWidth:Int, kernelHeight:Int, sigmaMult:Int) extends Kernel(kernelWidth, kernelHeight) {

  def this(sigma: Double, sigmaMult:Int, zoom:Int, tilesize:Int) = {
    this({
      val resolution = TMSUtils.resolution(zoom, tilesize) * LatLng.METERS_PER_DEGREE
      val kernelsize = sigma * sigmaMult

      var kernelWidth = Math.ceil(kernelsize / resolution).toInt * 2 + 1
      // Make sure the kernel doesn't extend beyond the requested kernelSize by a pixel
      // around the outside ring of the kernel.
      if (kernelWidth * resolution > kernelsize) {
        kernelWidth -= 2
      }
      Math.max(1, kernelWidth)
    },
      {
        val resolution = TMSUtils.resolution(zoom, tilesize) * LatLng.METERS_PER_DEGREE
        val kernelsize = sigma * sigmaMult

        var kernelHeight = Math.ceil(kernelsize / resolution).toInt * 2 + 1
        // Make sure the kernel doesn't extend beyond the requested kernelSize by a pixel
        // around the outside ring of the kernel.
        if (kernelHeight * resolution > kernelsize) {
          kernelHeight -= 2
        }
        Math.max(1, kernelHeight)
      }, sigmaMult)
  }

  override def getKernel: Option[Array[Float]] = None

  override def get2DKernel: Option[Array[Array[Float]]] = None

  override def calculate(tileId: Long, raster: Raster, nodatas:Array[Double]): Option[Raster] = {

    val nodata = nodatas(0).doubleValue()
    def isNodata(value:Double):Boolean = {
      if (nodata.isNaN) {
        value.isNaN
      }
      else {
        value == nodata
      }
    }

    val tilesize = raster.getWidth - getWidth + 1

    val tileStart = System.currentTimeMillis()
    OpenCVUtils.register()

    val srcValues = raster.getSamples(0, 0, raster.getWidth, raster.getHeight, 0, null.asInstanceOf[Array[Float]])

    val data = new Mat(raster.getWidth, raster.getHeight, CvType.CV_32F)
    val mask = Mat.zeros(data.width(), data.height(), CvType.CV_8U)

    val tileWidth = raster.getWidth
    var row, col:Int = 0
    while (row < raster.getHeight) {
      col = 0
      while (col < tileWidth) {
        val v = srcValues(row * tileWidth + col)
        if (isNodata(v) ) {
          data.put(col, row, 0)
          mask.put(col, row, 1)  // set the mask pixel on
        }
        else {
          data.put(col, row, v)
        }
        //data.get(col, row, d)
        //print("%.5f (%.5f) ".format(d(0), srcValues(row * tileWidth + col)))

        col += 1
      }
      row += 1
    }

    //        {
    //          val save = RasterUtils.createEmptyRaster(data.width, data.height, 1, DataBuffer.TYPE_FLOAT)
    //          val d = Array.ofDim[Float](1)
    //          row = 0
    //          while (row < data.width) {
    //            col = 0
    //            while (col < data.height) {
    //              data.get(col, row, d)
    //              save.setSample(col, row, 0, d(0))
    //
    //              col += 1
    //            }
    //            row += 1
    //          }
    //
    //          val t = TMSUtils.tileid(tile._1.get(), zoom)
    //          val bounds = TMSUtils.tileBounds(t, zoom, tilesize)
    //
    //          bounds.expandBy(TMSUtils.resolution(zoom, tilesize) * halfKernelW,
    //            TMSUtils.resolution(zoom, tilesize) * halfKernelH)
    //          GDALUtils.saveRaster(save, "/data/export/gaussian/src_tile_%d".format(tile._1.get), bounds)
    //        }

    //println("----------")
    val blurStart = System.currentTimeMillis()

    runKernel(data)

    val blurEnd = System.currentTimeMillis()

    val dst = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT)


    //        {
    //          val save = RasterUtils.createEmptyRaster(data.width, data.height, 1, DataBuffer.TYPE_FLOAT)
    //          row = 0
    //          val d = Array.ofDim[Float](1)
    //          while (row < data.width) {
    //            col = 0
    //            while (col < data.height) {
    //              data.get(col, row, d)
    //              save.setSample(col, row, 0, d(0))
    //
    //              col += 1
    //            }
    //            row += 1
    //          }
    //
    //          val t = TMSUtils.tileid(tile._1.get(), zoom)
    //          val bounds = TMSUtils.tileBounds(t, zoom, tilesize)
    //
    //          bounds.expandBy(TMSUtils.resolution(zoom, tilesize) * halfKernelW,
    //            TMSUtils.resolution(zoom, tilesize) * halfKernelH)
    //          GDALUtils.saveRaster(save, "/data/export/gaussian/dst_tile_%d".format(tile._1.get), bounds)
    //        }

    val halfKernelW = getWidth / 2 + 1
    val halfKernelH = getHeight / 2 + 1

    val b = Array.ofDim[Byte](1)
    val d = Array.ofDim[Float](1)

    row = 0
    while (row < tilesize) {
      col = 0
      while (col < tilesize) {
        mask.get(col + halfKernelW, row + halfKernelH, b)

        // the source pixel was Nodata, make the output pixel nodata (NaN), as well
        if (b(0) == 1) {
          dst.setSample(col, row, 0, Float.NaN)
        }
        else {
          data.get(col + halfKernelW, row + halfKernelH, d)

          dst.setSample(col, row, 0,d(0))
        }
        col += 1
      }
      row += 1
    }

    if (log.isDebugEnabled()) {
      val endTime = System.currentTimeMillis()
      logDebug("Time to process tile " + tileId + " is " + (endTime - tileStart))
      logDebug("  prep " + (blurStart - tileStart))
      logDebug("  apply kernel " + (blurEnd - blurStart))
      logDebug("  post " + (endTime - blurEnd))
    }

    Some(dst)
  }

  def runKernel(data: Mat): Unit

}
