package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object SinMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("sin")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new SinMapOp(node, variables, protectionLevel)
}

class SinMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    initialize(node, variables, protectionLevel)
  }

  override private[unarymath] def function(a: Double): Double = Math.sin(a)
}
