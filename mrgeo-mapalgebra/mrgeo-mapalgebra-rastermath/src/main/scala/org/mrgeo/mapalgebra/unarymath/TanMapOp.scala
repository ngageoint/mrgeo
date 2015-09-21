package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.MapOp

object TanMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("tan")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new TanMapOp(node, variables)
}

class TanMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a: Double): Double = Math.tan(a)
}
