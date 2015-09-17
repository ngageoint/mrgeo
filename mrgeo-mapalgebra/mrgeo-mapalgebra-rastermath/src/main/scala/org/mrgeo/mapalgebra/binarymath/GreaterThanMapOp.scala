package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object GreaterThanMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String](">", "gt")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new GreaterThanMapOp(node, variables, protectionLevel)
}

class GreaterThanMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    initialize(node, variables, protectionLevel)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = if (a > b) 1 else 0
}
