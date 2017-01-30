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

import java.awt.image.DataBuffer

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object CosMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("cos")
  }

  def create(raster:RasterMapOp):MapOp =
    new CosMapOp(Some(raster))

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new CosMapOp(node, variables)
}

class CosMapOp extends RawUnaryMathMapOp {
  private[unarymath] def this(raster:Option[RasterMapOp]) = {
    this()
    input = raster
  }

  private[unarymath] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a:Double):Double = Math.cos(a)

  override private[unarymath] def datatype():Int = {
    DataBuffer.TYPE_FLOAT
  }

  override private[unarymath] def nodata():Double = {
    Float.NaN
  }

}
