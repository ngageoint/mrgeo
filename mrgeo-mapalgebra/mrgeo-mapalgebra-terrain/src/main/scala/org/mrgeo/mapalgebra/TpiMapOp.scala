package org.mrgeo.mapalgebra

import java.awt.image.{WritableRaster, Raster}

import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp

object TpiMapOp extends MapOpRegistrar {
  def create(raster: RasterMapOp): MapOp =
    new TpiMapOp(Some(raster))

  override def register: Array[String] = {
    Array[String]("tpi")
  }

  override def apply(node: ParserNode, variables: String => Option[ParserNode]): MapOp =
    new TpiMapOp(node, variables)
}

class TpiMapOp extends TerrainIndexMapOp
{
  // Chad suggested a 33 pixel "radius" by default for TPI for good results
  private var kernelSize: Int = 67

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 1 && node.getNumChildren != 2) {
      throw new ParserException("tpi usage: tpi(rasterInput, [kernel size])")
    }
    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    if (node.getNumChildren == 2) {
      val kernelArg = MapOp.decodeInt(node.getChild(1), variables)
      kernelArg match {
        case Some(k) => kernelSize = k
        case None => throw new ParserException("Expected a number for the kernel size")
      }
    }
  }

  def this(input: Option[RasterMapOp]) {
    this()
    inputMapOp = input
  }

  override protected def computePixelValue(rasterValues: Array[Double], notnodata: Array[Boolean],
                                           rasterWidth: Int,
                                           processX: Int, processY: Int,
                                           xLeftOffset: Int, kernelWidth: Int,
                                           yAboveOffset: Int, kernelHeight: Int, tileId: Long): Double =
  {
    var x: Int = processX - xLeftOffset
    val maxX = x + kernelWidth
    var y: Int = processY - yAboveOffset
    val maxY = y + kernelHeight
    val processPixel: Double = rasterValues(calculateRasterIndex(rasterWidth, processX, processY))
    var sum: Double = 0.0
    var count: Int = 0
    while (y < maxY) {
      x = processX - xLeftOffset
      while (x < maxX) {
        if (x != processX || y != processY) {
          if (notnodata(y * rasterWidth + x)) {
            count += 1
            sum += rasterValues(calculateRasterIndex(rasterWidth, x, y))
          }
        }
        x += 1
      }
      y += 1
    }
    processPixel - sum / count
  }

  /**
    * Returns 2 values about the kernel to use (kernel width, kernel height).
    *
    * This method is called at the start of the execution of this map op.
    *
    * @return
    */
  override protected def getKernelInfo: (Int, Int) = {
    (kernelSize, kernelSize)
  }
}
