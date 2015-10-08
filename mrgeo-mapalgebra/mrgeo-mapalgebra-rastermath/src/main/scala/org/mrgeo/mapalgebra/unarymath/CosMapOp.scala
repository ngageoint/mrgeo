package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}

object CosMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("cos")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CosMapOp(node, variables)
}

class CosMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a: Double): Double = Math.cos(a)
}
