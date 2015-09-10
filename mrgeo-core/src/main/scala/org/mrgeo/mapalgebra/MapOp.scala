package org.mrgeo.mapalgebra

import org.apache.spark.SparkContext
import org.mrgeo.mapalgebra.parser.ParserNode

abstract class MapOp {
  def execute(context:SparkContext): Boolean
}
