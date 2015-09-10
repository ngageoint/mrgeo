package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode

trait MapOpRegistrar {
  def register:Array[String]

  // apply should call the mapop constructor, which needs to throw ParserExceptions on errors
  def apply(name:String, node: ParserNode, variables: String => Option[MapOp]):MapOp
}
