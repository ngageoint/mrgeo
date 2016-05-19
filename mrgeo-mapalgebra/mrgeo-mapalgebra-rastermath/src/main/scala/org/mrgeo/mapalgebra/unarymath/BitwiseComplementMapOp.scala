package org.mrgeo.mapalgebra.unarymath

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object BitwiseComplementMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("~")
  }

  def create(raster:RasterMapOp):MapOp =
    new BitwiseComplementMapOp(Some(raster))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new BitwiseComplementMapOp(node, variables)
}

class BitwiseComplementMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(raster: Option[RasterMapOp]) = {
    this()
    input = raster
  }

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[unarymath] def function(a: Double): Double = {
    val aLong = a.toLong
    val result = ~aLong
    result.toFloat
  }
}
