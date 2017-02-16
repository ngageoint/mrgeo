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

package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object DivMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("div", "/")
  }

  def create(raster:RasterMapOp, const:Double):MapOp = {
    new DivMapOp(Some(raster), Some(const))
  }

  // rcreate's parameters (name, type, & order) must be the same as creates
  def rcreate(raster:RasterMapOp, const:Double):MapOp = {
    new DivMapOp(Some(raster), Some(const), true)
  }

  def create(rasterA:RasterMapOp, rasterB:RasterMapOp):MapOp = {
    new DivMapOp(Some(rasterA), Some(rasterB))
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new DivMapOp(node, variables)
}

class DivMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(raster:Option[RasterMapOp], paramB:Option[Any], reverse:Boolean = false) = {
    this()

    if (reverse) {
      varB = raster

      paramB match {
        case Some(rasterB:RasterMapOp) => varA = Some(rasterB)
        case Some(double:Double) => constA = Some(double)
        case Some(int:Int) => constA = Some(int.toDouble)
        case Some(long:Long) => constA = Some(long.toDouble)
        case Some(float:Float) => constA = Some(float.toDouble)
        case Some(short:Short) => constA = Some(short.toDouble)
        case _ => throw new ParserException(paramB + "\" is not a raster or constant")
      }
    }
    else {
      varA = raster

      paramB match {
        case Some(rasterB:RasterMapOp) => varB = Some(rasterB)
        case Some(double:Double) => constB = Some(double)
        case Some(int:Int) => constB = Some(int.toDouble)
        case Some(long:Long) => constB = Some(long.toDouble)
        case Some(float:Float) => constB = Some(float.toDouble)
        case Some(short:Short) => constB = Some(short.toDouble)
        case _ => throw new ParserException(paramB + "\" is not a raster or constant")
      }
    }
  }

  private[binarymath] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()
    initialize(node, variables)
  }

  override private[binarymath] def function(a:Double, b:Double):Double = a / b
}
