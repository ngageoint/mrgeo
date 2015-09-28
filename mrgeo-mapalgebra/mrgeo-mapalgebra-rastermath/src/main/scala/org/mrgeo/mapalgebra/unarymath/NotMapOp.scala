package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object NotMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("!")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new NotMapOp(node, variables)
}

class NotMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a: Double): Double = if (a >= -RasterMapOp.EPSILON && a <= RasterMapOp.EPSILON) 0 else 1
}
