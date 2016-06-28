package org.mrgeo.mapalgebra.unarymath

import org.apache.spark.SparkContext
import org.mrgeo.data.raster.RasterUtils
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
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

  val EPSILON: Double = 1e-12

  private[unarymath] def this(raster: Option[RasterMapOp]) = {
    this()
    input = raster
  }

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def execute(context: SparkContext): Boolean = {
    input match {
      case Some(r) => {
        val metadata = r.metadata().getOrElse(throw new ParserException("Uh oh - no metadata available"))
        if (RasterUtils.isFloatingPoint(metadata.getTileType)) {
          log.warn("Using a floating point raster like " + metadata.getPyramid + " is not recommended for bitwise operators")
        }
      }
      case None => {}
    }
    return super.execute(context)
  }

  override private[unarymath] def function(a: Double): Double = {
    val aLong = a.toLong
    val result = ~aLong
    log.error("complement operator " + a + ", long: " + aLong + ", result = " + result)
    result.toFloat
  }
}
