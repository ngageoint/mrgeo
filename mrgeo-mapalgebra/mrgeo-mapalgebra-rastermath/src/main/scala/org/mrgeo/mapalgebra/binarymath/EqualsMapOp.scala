package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object EqualsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("==", "eq")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new EqualsMapOp(node, variables)
}

class EqualsMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    val v = a - b
    if (v >= -RasterMapOp.EPSILON && v <= RasterMapOp.EPSILON) {
      1
    }
    else {
      0
    }
  }
}
