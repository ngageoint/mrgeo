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

package org.mrgeo.mapalgebra

import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.{Bounds, LatLon, Pixel, TMSUtils}

object CropExactMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("cropexact")
  }

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new CropExactMapOp(Some(raster), w, s, e, n)

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double, zoom:Int):MapOp =
    new CropExactMapOp(Some(raster), w, s, e, n, zoom)

  def create(raster:RasterMapOp, rasterForBounds:RasterMapOp):MapOp =
    new CropExactMapOp(Some(raster), Some(rasterForBounds))

  def create(raster:RasterMapOp, rasterForBounds:RasterMapOp, zoom:Int):MapOp =
    new CropExactMapOp(Some(raster), Some(rasterForBounds), zoom)

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new CropExactMapOp(node, variables)
}

class CropExactMapOp extends CropMapOp {
  private val EPSILON:Double = 1e-8

  override def processTile(tile:(TileIdWritable, RasterWritable),
                           zoom:Int,
                           tilesize:Int,
                           nodatas:Array[Double]):(TileIdWritable, RasterWritable) = {
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

      val raster = RasterWritable.toMrGeoRaster(tile._2)

      var y:Int = 0
      val h = raster.height()
      val w = raster.width()
      val bands = raster.bands()
      while (y < h) {
        var x:Int = 0
        while (x < w) {
          var b:Int = 0
          while (b < bands) {
            if (x < minCopyX || x > maxCopyX || y < minCopyY || y > maxCopyY) {
              raster.setPixel(x, y, b, nodatas(b))
            }
            b += 1
          }
          x += 1
        }
        y += 1
      }

      (tile._1, RasterWritable.toWritable(raster))
    }
    else {
      tile
    }
  }

  override def getOutputBounds(zoom:Int, tilesize:Int):Bounds = {
    // Use the exact bounds that were passed in through map algebra
    cropBounds
  }

  protected override def processAllTiles(inputRDD:RasterRDD,
                                         zoom:Int,
                                         tilesize:Int,
                                         nodatas:Array[Double]):RDD[(TileIdWritable, RasterWritable)] = {
    inputRDD.map(tile => {
      processTile(tile, zoom, tilesize, nodatas)
    })
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], w:Double, s:Double, e:Double, n:Double) = {
    this()

    cropBounds = new Bounds(w, s, e, n)
    requestedZoom = None
    setInputMapOp(raster)
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], w:Double, s:Double, e:Double, n:Double, zoom:Int) = {
    this()

    cropBounds = new Bounds(w, s, e, n)
    requestedZoom = Some(zoom)
    setInputMapOp(raster)
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], rasterForBounds:Option[RasterMapOp]) = {
    this()

    rasterForBoundsMapOp = rasterForBounds
    requestedZoom = None
    setInputMapOp(raster)
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], rasterForBounds:Option[RasterMapOp], zoom:Int) = {
    this()

    rasterForBoundsMapOp = rasterForBounds
    requestedZoom = Some(zoom)
    setInputMapOp(raster)
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 5 && node.getNumChildren != 2) {
      throw new ParserException("Usage: cropExact(raster, w, s, e, n) or cropExact(raster, rasterForBounds)")
    }
    parseChildren(node, variables)
  }

  private def calculateCrop(zoom:Int, tilesize:Int) = {
    var bottomRightWorldPixel:Pixel = TMSUtils
        .latLonToPixelsUL(cropBounds.s, cropBounds.e, zoom, tilesize)

    val bottomRightAtPixelBoundary:LatLon = TMSUtils
        .pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize)

    if (Math.abs(bottomRightAtPixelBoundary.lat - cropBounds.n) < EPSILON) {
      bottomRightWorldPixel = new Pixel(bottomRightWorldPixel.px, bottomRightWorldPixel.py - 1)
    }

    if (Math.abs(bottomRightAtPixelBoundary.lon - cropBounds.e) < EPSILON) {
      bottomRightWorldPixel = new Pixel(bottomRightWorldPixel.px - 1, bottomRightWorldPixel.py)
    }

    val bottomRightPt:LatLon = TMSUtils
        .pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize)

    (
        TMSUtils.latLonToTilePixelUL(cropBounds.n, bottomRightPt.lon, bounds.e, bounds.n, zoom, tilesize),
        TMSUtils.latLonToTilePixelUL(bottomRightPt.lat, cropBounds.w, bounds.w, bounds.s, zoom, tilesize)
    )
  }
}