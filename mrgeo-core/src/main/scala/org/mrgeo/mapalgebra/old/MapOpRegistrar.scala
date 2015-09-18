package org.mrgeo.mapalgebra.old

import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser.ParserNode

trait MapOpRegistrar {
  def register:Array[String]

  // apply should call the mapop constructor, which needs to throw ParserExceptions on errors
  def apply(node: ParserNode, variables: String => Option[ParserNode]):MapOp

  override def toString: String = getClass.getSimpleName.replace("$", "")
}
