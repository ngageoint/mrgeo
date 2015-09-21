package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.MapOp

object UnaryMinusMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("UMinus")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new UnaryMinusMapOp(node, variables)
}

class UnaryMinusMapOp extends RawUnaryMathMapOp {

   private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
     this()

     initialize(node, variables)
   }

  override private[unarymath] def function(a: Double): Double = -a
}
