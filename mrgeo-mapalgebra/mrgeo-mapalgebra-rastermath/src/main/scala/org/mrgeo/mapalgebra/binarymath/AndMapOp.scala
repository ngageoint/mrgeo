package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object AndMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("&&", "&", "and")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new AndMapOp(node, variables, protectionLevel)
}

class AndMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    initialize(node, variables, protectionLevel)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    if ((a < -RasterMapOp.EPSILON || a > RasterMapOp.EPSILON) && (b < -RasterMapOp.EPSILON || b > RasterMapOp.EPSILON)) {
      1
    }
    else {
      0
    }
  }
}
