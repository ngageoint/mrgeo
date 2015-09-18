package org.mrgeo.mapalgebra.save

import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{SaveRasterMapOp, RasterMapOp}

object SaveMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("mosaic")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp = {
    // are we a raster, if so, create a SaveRasterMapOp
    try {
      val raster = RasterMapOp.decodeToRaster(node, variables)
      new SaveRasterMapOp(node, variables)
    }
    catch {
      case pe: ParserException =>
        throw pe
      // TODO:  Uncomment this!
      //        val vector = VectorMapOp.decodeToRaster(node, variables)
      //        new SaveVectorMapOp(node, true, variables, protectionLevel)
      //      }

    }
  }
}
