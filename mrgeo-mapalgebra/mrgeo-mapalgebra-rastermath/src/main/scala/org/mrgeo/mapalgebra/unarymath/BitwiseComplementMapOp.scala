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

package org.mrgeo.mapalgebra.unarymath

import org.apache.spark.SparkContext
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object BitwiseComplementMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("~")
  }

  def create(raster:RasterMapOp):MapOp =
    new BitwiseComplementMapOp(Some(raster))

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new BitwiseComplementMapOp(node, variables)
}

class BitwiseComplementMapOp extends RawUnaryMathMapOp {

  val EPSILON:Double = 1e-12

  override def execute(context:SparkContext):Boolean = {
    input match {
      case Some(r) =>
        val metadata = r.metadata().getOrElse(throw new ParserException("Uh oh - no metadata available"))
        if (metadata.isFloatingPoint) {
          log.warn(
            "Using a floating point raster like " + metadata.getPyramid + " is not recommended for bitwise operators")
        }
      case None =>
    }
    super.execute(context)
  }

  private[unarymath] def this(raster:Option[RasterMapOp]) = {
    this()
    input = raster
  }

  private[unarymath] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a:Double):Double = {
    val aLong = a.toLong
    val result = ~aLong
    result.toFloat
  }
}
