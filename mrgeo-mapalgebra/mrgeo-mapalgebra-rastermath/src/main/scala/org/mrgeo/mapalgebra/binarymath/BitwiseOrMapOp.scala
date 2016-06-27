package org.mrgeo.mapalgebra.binarymath

import org.apache.spark.SparkContext
import org.mrgeo.data.raster.RasterUtils
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object BitwiseOrMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("|")
  }
  def create(raster:RasterMapOp, const:Double):MapOp = {
    new BitwiseOrMapOp(Some(raster), Some(const))
  }
  def create(rasterA:RasterMapOp, rasterB:RasterMapOp):MapOp = {
    new BitwiseOrMapOp(Some(rasterA), Some(rasterB))
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new BitwiseOrMapOp(node, variables)
}

class BitwiseOrMapOp extends RawBinaryMathMapOp {

  val EPSILON: Double = 1e-12

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

  override def execute(context: SparkContext): Boolean = {
    varA match {
      case Some(a) => {
        val metadata = a.metadata().getOrElse(throw new ParserException("Uh oh - no metadata available"))
        if (RasterUtils.isFloatingPoint(metadata.getTileType)) {
          log.warn("Using a floating point raster like " + metadata.getPyramid + " is not recommended for bitwise operators")
        }
      }
      case None => {}
    }
    constA match {
      case Some(a) => {
        val remainder = a % 1
        if (remainder < -EPSILON || remainder > EPSILON) {
          log.warn("Using floating point values in bitwise operators is not recommended")
        }
      }
      case None => {}
    }
    varB match {
      case Some(b) => {
        val metadata = b.metadata().getOrElse(throw new ParserException("Uh oh - no metadata available"))
        if (RasterUtils.isFloatingPoint(metadata.getTileType)) {
          log.warn("Using a floating point raster like " + metadata.getPyramid + " is not recommended for bitwise operators")
        }
      }
      case None => {}
    }
    constB match {
      case Some(b) => {
        val remainder = b % 1
        if (remainder < -EPSILON || remainder > EPSILON) {
          log.warn("Using floating point values in bitwise operators is not recommended")
        }
      }
      case None => {}
    }
    return super.execute(context)
  }

  override private[binarymath] def function(a: Double, b: Double): Double = {
    val aLong = a.toLong
    val bLong = b.toLong
    val result = aLong | bLong
    result.toFloat
  }
}
