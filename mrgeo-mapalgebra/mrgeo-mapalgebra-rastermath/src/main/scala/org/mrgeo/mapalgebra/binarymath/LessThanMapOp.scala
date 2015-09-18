package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.MapOp

object LessThanMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("<", "lt")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new LessThanMapOp(node, variables)
}

class LessThanMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = if (a < b) 1 else 0
}
