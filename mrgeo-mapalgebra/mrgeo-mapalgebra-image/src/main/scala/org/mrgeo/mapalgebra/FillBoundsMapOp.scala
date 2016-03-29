package org.mrgeo.mapalgebra

import java.io.IOException

import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.Bounds

object FillBoundsMapOp extends MapOpRegistrar {

  override def register: Array[String] = {
    Array[String]("fillbounds")
  }

  def create(raster:RasterMapOp, fillRaster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new FillBoundsMapOp(raster, fillRaster, new Bounds(w, s, e, n))

  def create(raster:RasterMapOp, constFill:Double, w:Double, s:Double, e:Double, n:Double):MapOp =
    new FillBoundsMapOp(raster, constFill, new Bounds(w, s, e, n))

  def create(raster:RasterMapOp, fillRaster:RasterMapOp, boundsRaster: RasterMapOp):MapOp =
    new FillBoundsMapOp(raster, fillRaster, boundsRaster)

  def create(raster:RasterMapOp, constFill:Double, boundsRaster: RasterMapOp):MapOp =
    new FillBoundsMapOp(raster, constFill, boundsRaster)

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new FillBoundsMapOp(node, variables)
}

class FillBoundsMapOp extends FillMapOp {
  private var bounds: Option[Bounds] = None
  private var rasterForBounds: Option[RasterMapOp] = None

  private[mapalgebra] def this(raster:RasterMapOp, const:Double, bounds: Bounds) = {
    this()
    inputMapOp = Some(raster)
    constFill = Some(const)
    this.bounds = Some(bounds)
  }

  private[mapalgebra] def this(raster:RasterMapOp, fillRaster:RasterMapOp, bounds: Bounds) = {
    this()
    inputMapOp = Some(raster)
    fillMapOp = Some(fillRaster)
    this.bounds = Some(bounds)
  }

  private[mapalgebra] def this(raster:RasterMapOp, const:Double, rasterForBounds: RasterMapOp) = {
    this()
    inputMapOp = Some(raster)
    constFill = Some(const)
    this.rasterForBounds = Some(rasterForBounds)
  }

  private[mapalgebra] def this(raster:RasterMapOp, fillRaster:RasterMapOp, rasterForBounds: RasterMapOp) = {
    this()
    inputMapOp = Some(raster)
    fillMapOp = Some(fillRaster)
    this.rasterForBounds = Some(rasterForBounds)
  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()
    if (node.getNumChildren != 6 && node.getNumChildren != 3) {
      throw new ParserException("Usage: fillbounds(raster, fill value, w, s, e, n) or fillbounds(raster, fill value, rasterForBounds)")
    }

    parseChildren(node, variables)

    if (node.getNumChildren == 3) {
      rasterForBounds = RasterMapOp.decodeToRaster(node.getChild(2), variables)
      if (rasterForBounds.isEmpty) {
        throw new ParserException("Missing bounds raster")
      }
    }
    else {
      bounds = Some(new Bounds(MapOp.decodeDouble(node.getChild(2), variables).get,
        MapOp.decodeDouble(node.getChild(3), variables).get,
        MapOp.decodeDouble(node.getChild(4), variables).get,
        MapOp.decodeDouble(node.getChild(5), variables).get))
    }
  }

  override def getOutputBounds(inputMetadata: MrsPyramidMetadata): Bounds = {
    rasterForBounds match {
      case Some(rfb) => {
        rfb.metadata() match {
          case Some(metadata) => metadata.getBounds
          case _ => throw new ParserException("Unable to read metadata for bounds raster: ")
        }
      }
      case _ => {
        bounds match {
          case Some(b) => {
            b
          }
          case _ => throw new IOException("Invalid bounds specified to fillBounds")
        }
      }
    }
  }
}