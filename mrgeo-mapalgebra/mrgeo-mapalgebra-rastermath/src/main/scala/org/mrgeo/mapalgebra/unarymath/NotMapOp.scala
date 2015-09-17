package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object NotMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("!")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new NotMapOp(node, variables, protectionLevel)
}

class NotMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    initialize(node, variables, protectionLevel)
  }

  override private[unarymath] def function(a: Double): Double = if (a >= -RasterMapOp.EPSILON && a <= RasterMapOp.EPSILON) 0 else 1
}
