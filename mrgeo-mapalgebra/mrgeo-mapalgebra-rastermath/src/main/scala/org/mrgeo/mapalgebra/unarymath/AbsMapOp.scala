package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.MapOp

object AbsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("abs")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new AbsMapOp(node, variables)
}

class AbsMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a: Double): Double = Math.abs(a)
}
