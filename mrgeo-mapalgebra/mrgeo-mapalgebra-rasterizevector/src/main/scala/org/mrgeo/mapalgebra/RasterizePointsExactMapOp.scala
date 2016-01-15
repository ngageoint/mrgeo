package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.utils.TMSUtils

object RasterizePointsExactMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("rasterizepointsexact")
  }

  def create(vector: VectorMapOp, aggregator:String, cellsize:String, w:Double, s:Double, e:Double, n:Double, column:String = null) =
  {
    new RasterizePointsMapOp(Some(vector), aggregator, cellsize, column, new TMSUtils.Bounds(w, n, e, s).toCommaString)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizePointsMapOp(node, variables)
}

class RasterizePointsExactMapOp extends RasterizePointsMapOp {}