package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object NotEqualsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("!=", "^=", "<>", "ne")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new NotEqualsMapOp(node, variables)
}

class NotEqualsMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    val v = a - b
    // remember, this is "not"
    if (v >= -RasterMapOp.EPSILON && v <= RasterMapOp.EPSILON) {
      0
    }
    else {
      1
    }
  }
  }
