/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.mapalgebra.raster

import java.io.IOException

import org.apache.spark.SparkContext
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser.{ParserException, ParserFunctionNode, ParserNode, ParserVariableNode}
import org.mrgeo.utils.SparkUtils

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

  def rdd(zoom: Int):Option[RasterRDD] = {
    throw new IOException("Unsupported zoom level for raster RDD")
  }

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
        throw new IOException("Unable to save - no metadata was assigned in " + this.getClass.getName)
      }
    case _ =>
      throw new IOException("Unable to save - no RDD was assigned in " + this.getClass.getName)
    }
  }


}
