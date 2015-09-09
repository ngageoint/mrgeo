package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode

trait MapOpRegistrar {
  def register:Array[String]
  def apply(name:String, node: ParserNode):MapOp
}
