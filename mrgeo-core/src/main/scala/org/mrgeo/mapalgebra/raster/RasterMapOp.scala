/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.mapalgebra.raster

import java.io.IOException

import org.apache.spark.SparkContext
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser.{ParserException, ParserFunctionNode, ParserNode, ParserVariableNode}
//import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.tms.{TileBounds, TMSUtils}
import org.mrgeo.utils.{GDALUtils, SparkUtils}

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

  def createEmptyRasterRDD(context: SparkContext, tb: TileBounds, zoom: Int) = {
    val tileBuilder = Array.newBuilder[(TileIdWritable, RasterWritable)]
    for (ty <- tb.s to tb.n) {
      for (tx <- tb.w to tb.e) {
        val id = TMSUtils.tileid(tx, ty, zoom)

        val tuple = (new TileIdWritable(id), new RasterWritable())
        tileBuilder += tuple
      }
    }

    new RasterRDD(context.parallelize(tileBuilder.result()))
  }
}


abstract class RasterMapOp extends MapOp {

  private var meta:MrsPyramidMetadata = null


  def rdd():Option[RasterRDD]

  def rdd(zoom: Int):Option[RasterRDD] = {
    if (meta != null && zoom == meta.getMaxZoomLevel) {
      rdd()
    }
    else {
      throw new IOException("rdd(zoom) rdd with zoom level other than max zoom not supported for raster RDD")
    }
  }

  def metadata():Option[MrsPyramidMetadata] =  Option(meta)
  def metadata(meta:MrsPyramidMetadata) = { this.meta = meta}

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
        throw new IOException("Unable to save - no metadata was assigned in " + this.getClass.getName)
      }
    case _ =>
      throw new IOException("Unable to save - no RDD was assigned in " + this.getClass.getName)
    }
  }

  def toRaster(exact:Boolean = false) = {
    val rasterrdd = rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + getClass.getName))
    SparkUtils.mergeTiles(rasterrdd, meta.getMaxZoomLevel, meta.getTilesize, meta.getDefaultValues,
      if (exact) meta.getBounds else null)
  }

  def toDataset(exact:Boolean = false) = {
    val rasterrdd = rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val raster = SparkUtils.mergeTiles(rasterrdd, zoom, tilesize, meta.getDefaultValues,
      if (exact) meta.getBounds else null)

    val bounds = if (exact) {
      meta.getBounds
    }
    else {
      TMSUtils.tileBounds(meta.getBounds, zoom, tilesize)
    }

    GDALUtils.toDataset(raster, meta.getDefaultValue(0), bounds)

  }
}
