package org.mrgeo.mapalgebra.binarymath

import org.apache.spark.SparkContext
import org.mrgeo.data.raster.RasterUtils
import org.mrgeo.mapalgebra.parser.ParserException

abstract class BitwiseMapOp extends RawBinaryMathMapOp {

  val EPSILON: Double = 1e-12

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

}
