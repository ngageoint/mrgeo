package org.mrgeo.mapalgebra.binarymath

import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object BitwiseXorMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("^")
  }
  def create(raster:RasterMapOp, const:Double):MapOp = {
    new BitwiseXorMapOp(Some(raster), Some(const))
  }
  def create(rasterA:RasterMapOp, rasterB:RasterMapOp):MapOp = {
    new BitwiseXorMapOp(Some(rasterA), Some(rasterB))
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new BitwiseXorMapOp(node, variables)
}

class BitwiseXorMapOp extends RawBinaryMathMapOp {

  private[binarymath] def this(raster: Option[RasterMapOp], paramB:Option[Any]) = {
    this()

    varA = raster

    paramB match {
      case Some(rasterB:RasterMapOp) => varB = Some(rasterB)
      case Some(double:Double) => constB = Some(double)
      case Some(int:Int) => constB = Some(int.toDouble)
      case Some(long:Long) => constB = Some(long.toDouble)
      case Some(float:Float) => constB = Some(float.toDouble)
      case Some(short:Short) => constB = Some(short.toDouble)
      case _ =>  throw new ParserException("Second term \"" + paramB + "\" is not a raster or constant")
    }
  }

  private[binarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    val aLong = a.toLong
    val bLong = b.toLong
    val result = aLong ^ bLong
    result.toFloat
  }
}
