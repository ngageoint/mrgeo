package org.mrgeo.mapalgebra

import com.vividsolutions.jts.geom.util.GeometryMapper.MapOp
import org.mrgeo.mapalgebra.parser.ParserNode

object RawBinaryMathMapOpRegistrar extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("+", "-", "*", "/")
  }
  override def apply(function:String, node:ParserNode): MapOp = new RawBinaryMathMapOp

  override def toString: String = "RawBinaryMathMapOp (object)"

}

class RawBinaryMathMapOp extends MapOp {
  //override def toString: String = "RawBinaryMathMapOp (class)"
}
