package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.MapOp

object XOrMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("xor")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new XOrMapOp(node, variables)
}

class XOrMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    if ((a < -RasterMapOp.EPSILON || a > RasterMapOp.EPSILON) == (b < -RasterMapOp.EPSILON || b > RasterMapOp.EPSILON)) {
      0
    }
    else {
      1
    }
  }
}
