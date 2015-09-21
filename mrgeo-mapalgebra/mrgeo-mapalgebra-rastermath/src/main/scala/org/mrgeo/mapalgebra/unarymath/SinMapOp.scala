package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.MapOp

object SinMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("sin")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new SinMapOp(node, variables)
}

class SinMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a: Double): Double = Math.sin(a)
}
