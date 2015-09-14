package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object BinaryMultMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("*")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new BinaryMultMapOp(node, variables, protectionLevel)
}

class BinaryMultMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()
    initialize(node, variables, protectionLevel)
  }
  
  override private[binarymath] def function(a: Double, b: Double): Double = a * b
}
