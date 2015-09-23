package org.mrgeo.mapalgebra.raster

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser.{ParserVariableNode, ParserException, ParserFunctionNode, ParserNode}
import org.mrgeo.utils.{Bounds, SparkUtils}

object RasterMapOp {

  val EPSILON: Double = 1e-8

  def isNodata(value:Double, nodata:Double):Boolean = {
    if (nodata.isNaN) {
      value.isNaN
    }
    else {
      nodata == value
    }
  }
  def isNotNodata(value:Double, nodata:Double):Boolean = {
    if (nodata.isNaN) {
      !value.isNaN
    }
    else {
      nodata != value
    }
  }

  def nearZero(v:Double):Boolean = {
    if (v >= -EPSILON && v <= EPSILON) {
      true
    }
    else {
      false
    }
  }

  def decodeToRaster(node:ParserNode, variables: String => Option[ParserNode]): Option[RasterMapOp] = {
    node match {
    case func: ParserFunctionNode => func.getMapOp match {
      case raster: RasterMapOp => Some(raster)
      case _ => throw new ParserException("Term \"" + node + "\" is not a raster input")
    }
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).getOrElse(throw new ParserException("Variable \"" + node + " has not been assigned")) match {
      case func: ParserFunctionNode => func.getMapOp match {
        case raster: RasterMapOp => Some(raster)
        case _ => throw new ParserException("Term \"" + node + "\" is not a raster input")
        }
      case _ => throw new ParserException("Term \"" + node + "\" is not a raster input")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a raster input")
    }
  }
}


abstract class RasterMapOp extends MapOp {

  private var meta:MrsImagePyramidMetadata = null


  def rdd():Option[RasterRDD]

  def metadata():Option[MrsImagePyramidMetadata] =  Option(meta)
  def metadata(meta:MrsImagePyramidMetadata) = { this.meta = meta}

  def save(output: String, providerProperties:ProviderProperties, context:SparkContext) = {
    rdd() match {
    case Some(rdd) =>
      val provider: MrsImageDataProvider =
        DataProviderFactory.getMrsImageDataProvider(output, AccessMode.OVERWRITE, providerProperties)
      metadata() match {
      case Some(metadata) =>
        val meta = metadata

        SparkUtils.saveMrsPyramid(rdd, provider, meta, meta.getMaxZoomLevel,
          context.hadoopConfiguration, providerproperties =  providerProperties)
      case _ =>
      }
    case _ =>
    }
  }


}
