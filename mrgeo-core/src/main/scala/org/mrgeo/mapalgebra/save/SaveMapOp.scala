package org.mrgeo.mapalgebra.save

import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{SaveRasterMapOp, RasterMapOp}

object SaveMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("save")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp = {

    if (node.getNumChildren != 2) {
      throw new ParserException(node.getName + " takes 2 arguments")
    }

    // are we a raster, if so, create a SaveRasterMapOp
    try {
      val raster = RasterMapOp.decodeToRaster(node.getChild(0), variables)
      new SaveRasterMapOp(node, variables)
    }
    catch {
      case pe: ParserException =>
        throw pe
      // TODO:  Uncomment this!
      //        val vector = VectorMapOp.decodeToRaster(node.getChild(0), variables)
      //        new SaveVectorMapOp(node, true, variables, protectionLevel)
      //      }

    }
  }
}
