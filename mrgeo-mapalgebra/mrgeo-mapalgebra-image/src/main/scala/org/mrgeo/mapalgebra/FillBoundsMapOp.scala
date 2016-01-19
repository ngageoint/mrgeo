package org.mrgeo.mapalgebra

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.Bounds

object FillBoundsMapOp extends MapOpRegistrar {

  override def register: Array[String] = {
    Array[String]("fillbounds")
  }

  def create(raster:RasterMapOp, fillRaster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new FillMapOp(raster, fillRaster, Some(new Bounds(w, s, e, n)))

  def create(raster:RasterMapOp, constFill:Double, w:Double, s:Double, e:Double, n:Double):MapOp =
    new FillMapOp(raster, constFill, Some(new Bounds(w, s, e, n)))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new FillMapOp(node, variables)
}

class FillBoundsMapOp extends FillMapOp {}