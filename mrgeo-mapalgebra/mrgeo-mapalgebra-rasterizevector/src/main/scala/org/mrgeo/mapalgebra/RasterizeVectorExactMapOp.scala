package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.utils.TMSUtils

object RasterizeVectorExactMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("rasterizevectorexact", "rasterizeexact")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizeVectorMapOp(node, variables)

  def create(vector: VectorMapOp, aggregator:String, cellsize:String,
      w:Double, s:Double, e:Double, n:Double, column:String = null) =
  {
    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column,
      new TMSUtils.Bounds(w, n, e, s).toCommaString)
  }

  //  def create(vector: VectorMapOp, aggregator:String, cellsize:String, bounds:String, column:String = null) =
  //  {
  //    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column, bounds)
  //  }

}

class RasterizeVectorExactMapOp extends RasterizeVectorMapOp {}