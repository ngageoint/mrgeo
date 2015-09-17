package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object GreaterThanEqualsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String](">=", "ge", "gte")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new GreaterThanEqualsMapOp(node, variables, protectionLevel)
}

class GreaterThanEqualsMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    initialize(node, variables, protectionLevel)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = if (a >= b) 1 else 0
}
