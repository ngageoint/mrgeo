package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode

trait MapOpRegistrar {
  def register:Array[String]

  // apply should call the mapop constructor, which needs to throw ParserExceptions on errors
  def apply(node: ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null):MapOp

  override def toString: String = getClass.getSimpleName.replace("$", "")
}
