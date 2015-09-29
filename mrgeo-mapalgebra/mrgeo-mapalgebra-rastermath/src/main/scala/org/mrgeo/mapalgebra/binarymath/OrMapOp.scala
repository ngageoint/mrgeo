package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object OrMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("||", "|", "or")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new OrMapOp(node, variables)
}

class OrMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    if (a < -RasterMapOp.EPSILON || a > RasterMapOp.EPSILON || b < -RasterMapOp.EPSILON || b > RasterMapOp.EPSILON) {
      1
    }
    else {
      0
    }
  }
}
