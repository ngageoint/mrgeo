package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp

object CropExactMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("cropexact")
  }

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new CropMapOp(Some(raster), w, s, e, n, true)

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CropMapOp(node, variables, true)
}

class CropExactMapOp extends CropMapOp {}