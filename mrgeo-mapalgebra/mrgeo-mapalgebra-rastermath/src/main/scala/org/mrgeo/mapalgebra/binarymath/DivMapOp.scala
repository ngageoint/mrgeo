package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object DivMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("/")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new DivMapOp(node, variables)
}

class DivMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()
    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = a / b
}
