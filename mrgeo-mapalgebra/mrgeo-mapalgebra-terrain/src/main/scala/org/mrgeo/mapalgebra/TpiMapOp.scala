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

import java.awt.image.{WritableRaster, Raster}

import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{RawFocalMapOp, RasterMapOp}

object TpiMapOp extends MapOpRegistrar {
  def create(raster: RasterMapOp): MapOp =
    new TpiMapOp(Some(raster))

  override def register: Array[String] = {
    Array[String]("tpi")
  }

  override def apply(node: ParserNode, variables: String => Option[ParserNode]): MapOp =
    new TpiMapOp(node, variables)
}

class TpiMapOp extends RawFocalMapOp
{
  // Chad suggested a 33 pixel "radius" by default for TPI for good results
  private var neighborhoodSize: Int = 67

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 1 && node.getNumChildren != 2) {
      throw new ParserException("tpi usage: tpi(rasterInput, [neighborhood size])")
    }
    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    if (node.getNumChildren == 2) {
      val neighborhoodArg = MapOp.decodeInt(node.getChild(1), variables)
      neighborhoodArg match {
        case Some(k) => neighborhoodSize = k
        case None => throw new ParserException("Expected a number for the neighborhood size")
      }
    }
  }

  def this(input: Option[RasterMapOp]) {
    this()
    inputMapOp = input
  }

  override protected def computePixelValue(rasterValues: Array[Double], notnodata: Array[Boolean],
                                           rasterWidth: Int,
                                           processX: Int, processY: Int,
                                           xLeftOffset: Int, neighborhoodWidth: Int,
                                           yAboveOffset: Int, neighborhoodHeight: Int, tileId: Long): Double =
  {
    var x: Int = processX - xLeftOffset
    val maxX = x + neighborhoodWidth
    var y: Int = processY - yAboveOffset
    val maxY = y + neighborhoodHeight
    val processPixel: Double = rasterValues(calculateRasterIndex(rasterWidth, processX, processY))
    var sum: Double = 0.0
    var count: Int = 0
    while (y < maxY) {
      x = processX - xLeftOffset
      while (x < maxX) {
        if (x != processX || y != processY) {
          if (notnodata(y * rasterWidth + x)) {
            count += 1
            sum += rasterValues(calculateRasterIndex(rasterWidth, x, y))
          }
        }
        x += 1
      }
      y += 1
    }
    processPixel - sum / count
  }

  /**
    * Returns 2 values about the neighborhood to use (neighborhood width, neighborhood height).
    *
    * This method is called at the start of the execution of this map op.
    *
    * @return
    */
  override protected def getNeighborhoodInfo: (Int, Int) = {
    (neighborhoodSize, neighborhoodSize)
  }
}
