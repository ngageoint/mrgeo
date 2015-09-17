package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object TanMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("tan")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new TanMapOp(node, variables, protectionLevel)
}

class TanMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    initialize(node, variables, protectionLevel)
  }

  override private[unarymath] def function(a: Double): Double = Math.tan(a)
}
