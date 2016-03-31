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

package org.mrgeo.mapalgebra

import java.io.{ObjectOutput, ObjectInput, Externalizable}

import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.TMSUtils
import org.mrgeo.utils.TMSUtils.Bounds

object CropExactMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("cropexact")
  }

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new CropExactMapOp(Some(raster), w, s, e, n)

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CropExactMapOp(node, variables)
}

class CropExactMapOp extends CropMapOp {
  private val EPSILON: Double = 1e-8

  private[mapalgebra] def this(raster:Option[RasterMapOp], w:Double, s:Double, e:Double, n:Double) = {
    this()

    inputMapOp = raster
    cropBounds = new Bounds(w, s, e, n)
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], rasterForBounds: Option[RasterMapOp]) = {
    this()

    inputMapOp = raster
    rasterForBoundsMapOp = rasterForBounds
  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 5 && node.getNumChildren != 2) {
      throw new ParserException("Usage: cropExact(raster, w, s, e, n) or cropExact(raster, rasterForBounds)")
    }
    parseChildren(node, variables)
  }

  override def processTile(tile: (TileIdWritable, RasterWritable),
                           zoom: Int, tilesize: Int,
                           nodatas: Array[Double]): TraversableOnce[(TileIdWritable, RasterWritable)] = {
    val pp = calculateCrop(zoom, tilesize)

    val t = pp._1.py
    val r = pp._1.px
    val b = pp._2.py
    val l = pp._2.px
    val tt = TMSUtils.tileid(tile._1.get(), zoom)
    if ((tt.tx == bounds.w) || (tt.tx == bounds.e) || (tt.ty == bounds.s) || (tt.ty == bounds.n)) {
      var minCopyX:Long = 0
      var maxCopyX:Long = tilesize
      var minCopyY:Long = 0
      var maxCopyY:Long = tilesize

      if (tt.tx == bounds.w) {
        minCopyX = l
      }
      if (tt.tx == bounds.e) {
        maxCopyX = r
      }
      if (tt.ty == bounds.s) {
        maxCopyY = b
      }
      if (tt.ty == bounds.n) {
        minCopyY = t
      }

      val raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(tile._2))

      var y: Int = 0
      while (y < raster.getHeight) {
        var x: Int = 0
        while (x < raster.getWidth) {
          var b: Int = 0
          while (b < raster.getNumBands) {
            if (x < minCopyX || x > maxCopyX || y < minCopyY || y > maxCopyY)
            {
              raster.setSample(x, y, 0, nodatas(b))
            }
            b += 1
          }
          x += 1
        }
        y += 1
      }

      Array((tile._1, RasterWritable.toWritable(raster))).iterator
    }
    else {
      Array(tile).iterator
    }
  }

  override def getOutputBounds(zoom: Int, tilesize: Int) = {
    // Use the exact bounds that were passed in through map algebra
    cropBounds
  }

  private def calculateCrop(zoom:Int, tilesize:Int) = {
    var bottomRightWorldPixel: TMSUtils.Pixel = TMSUtils
      .latLonToPixelsUL(cropBounds.s, cropBounds.e, zoom, tilesize)

    val bottomRightAtPixelBoundary: TMSUtils.LatLon = TMSUtils
      .pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize)

    if (Math.abs(bottomRightAtPixelBoundary.lat - cropBounds.n) < EPSILON) {
      bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px, bottomRightWorldPixel.py - 1)
    }

    if (Math.abs(bottomRightAtPixelBoundary.lon - cropBounds.e) < EPSILON) {
      bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px - 1, bottomRightWorldPixel.py)
    }

    val bottomRightPt: TMSUtils.LatLon = TMSUtils
      .pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize)
    val b: TMSUtils.Bounds = new TMSUtils.Bounds(cropBounds.w, bottomRightPt.lat, bottomRightPt.lon, cropBounds.n)

    (
      TMSUtils.latLonToTilePixelUL(cropBounds.n, bottomRightPt.lon, bounds.e, bounds.n, zoom, tilesize),
      TMSUtils.latLonToTilePixelUL(bottomRightPt.lat, cropBounds.w, bounds.w, bounds.s, zoom, tilesize)
      )
  }
}