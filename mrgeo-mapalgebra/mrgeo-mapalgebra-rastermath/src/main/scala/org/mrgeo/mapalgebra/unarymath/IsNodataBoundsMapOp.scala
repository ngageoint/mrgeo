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

package org.mrgeo.mapalgebra.unarymath

import java.io.IOException

import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}
import org.mrgeo.utils.tms.Bounds

object IsNodataBoundsMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("isnodatabounds", "isnullbounds")
  }

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new IsNodataBoundsMapOp(raster, new Bounds(w, s, e, n))

  def create(raster:RasterMapOp, boundsRaster:RasterMapOp):MapOp =
    new IsNodataBoundsMapOp(raster, boundsRaster)

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new IsNodataBoundsMapOp(node, variables)
}

class IsNodataBoundsMapOp extends IsNodataMapOp {
  private var bounds:Option[Bounds] = None
  private var rasterForBounds:Option[RasterMapOp] = None

  override def getOutputBounds(inputMetadata:MrsPyramidMetadata):Bounds = {
    rasterForBounds match {
      case Some(rfb) => {
        rfb.metadata() match {
          case Some(metadata) => metadata.getBounds
          case _ => throw new ParserException("Unable to read metadata for bounds raster: ")
        }
      }
      case _ => {
        bounds match {
          case Some(b) => {
            b
          }
          case _ => throw new IOException("Invalid bounds specified to isNodataBounds")
        }
      }
    }
  }

  private[unarymath] def this(raster:RasterMapOp, bounds:Bounds) = {
    this()
    this.input = Some(raster)
    this.bounds = Some(bounds)
  }

  private[unarymath] def this(raster:RasterMapOp, rasterForBounds:RasterMapOp) = {
    this()
    this.input = Some(raster)
    this.rasterForBounds = Some(rasterForBounds)
  }

  private[unarymath] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 2 && node.getNumChildren != 5) {
      throw new ParserException("Usage: isNodata(raster, w, s, e, n) or isNodata(raster, rasterForBounds)")
    }

    if (node.getNumChildren == 2) {
      rasterForBounds = RasterMapOp.decodeToRaster(node.getChild(1), variables)
      if (rasterForBounds.isEmpty) {
        throw new ParserException("Missing bounds raster")
      }
    }
    else {
      bounds = Some(new Bounds(MapOp.decodeDouble(node.getChild(1), variables).get,
        MapOp.decodeDouble(node.getChild(2), variables).get,
        MapOp.decodeDouble(node.getChild(3), variables).get,
        MapOp.decodeDouble(node.getChild(4), variables).get))
    }

    input = RasterMapOp.decodeToRaster(node.getChild(0), variables)
  }
}
