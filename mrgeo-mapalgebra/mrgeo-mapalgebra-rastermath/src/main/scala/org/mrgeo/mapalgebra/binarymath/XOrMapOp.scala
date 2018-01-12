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

import java.awt.image.DataBuffer

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object XOrMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("xor")
  }

  def create(raster:RasterMapOp, const:Double):MapOp = {
    new XOrMapOp(Some(raster), Some(const))
  }

  def create(rasterA:RasterMapOp, rasterB:RasterMapOp):MapOp = {
    new XOrMapOp(Some(rasterA), Some(rasterB))
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new XOrMapOp(node, variables)
}

class XOrMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(raster:Option[RasterMapOp], paramB:Option[Any]) = {
    this()

    varA = raster

    paramB match {
      case Some(rasterB:RasterMapOp) => varB = Some(rasterB)
      case Some(double:Double) => constB = Some(double)
      case Some(int:Int) => constB = Some(int.toDouble)
      case Some(long:Long) => constB = Some(long.toDouble)
      case Some(float:Float) => constB = Some(float.toDouble)
      case Some(short:Short) => constB = Some(short.toDouble)
      case _ => throw new ParserException("Second term \"" + paramB + "\" is not a raster or constant")
    }
  }

  private[binarymath] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  @SuppressFBWarnings(value = Array("UCF_USELESS_CONTROL_FLOW_NEXT_LINE"), justification = "False positive")
  override private[binarymath] def function(a:Double, b:Double):Double = {
    if ((a < -RasterMapOp.EPSILON || a > RasterMapOp.EPSILON) ==
        (b < -RasterMapOp.EPSILON || b > RasterMapOp.EPSILON)) {
      0.0
    }
    else {
      1.0
    }
  }

  override private[binarymath] def datatype():Int = {
    DataBuffer.TYPE_BYTE
  }

  override private[binarymath] def nodata():Double = {
    255
  }

}
