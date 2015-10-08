package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object LessThanEqualsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("<=", "le", "lte")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new LessThanEqualsMapOp(node, variables)
}

class LessThanEqualsMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = if (a <= b) 1 else 0
}
