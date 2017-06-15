/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.mapalgebra

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{MrsPyramidMapOp, RasterMapOp}
import org.mrgeo.utils.SparkUtils

object ZoomMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("zoom")
  }

  def create(raster:RasterMapOp, band:Int = 1) =
    new ZoomMapOp(Some(raster), band)

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new ZoomMapOp(node, variables)
}

class ZoomMapOp extends RasterMapOp with Externalizable
{
  private var rasterRDD:Option[RasterRDD] = None
  private var input:Option[RasterMapOp] = None
  private var zoom:Int = 0

  override def rdd():Option[RasterRDD] = rasterRDD

  override def getZoomLevel(): Int = {
    zoom
  }

  override def execute(context:SparkContext):Boolean = {
    input match {
      case Some(rmo) => {
        rmo match {
          case mapOp: MrsPyramidMapOp => {
            val newMapOp = mapOp.clone
            newMapOp.context(context)
            rasterRDD = newMapOp.rdd(zoom)
            val meta = mapOp.metadata().getOrElse(throw new IOException("Can't load metadata! Ouch! " + mapOp.getClass.getName))
            metadata(SparkUtils.calculateMetadata(rasterRDD.get,
              zoom,
              meta.getDefaultValues,
              bounds = meta.getBounds,
              calcStats = false))
            return true
          }
          case _ => throw new IOException("Can only pass a direct image data source to zoom")
        }
      }
      case None => throw new IOException("Can't work with input raster! Ouch!")
    }
    false
  }

  override def readExternal(in:ObjectInput):Unit = {
    zoom = in.readInt()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeInt(zoom)
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = true

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  private[mapalgebra] def this(raster:Option[RasterMapOp], zoom:Int) = {
    this()
    input = raster
    this.zoom = zoom
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 2) {
      throw new ParserException(node.getName + " requires two arguments - a source raster and a zoom level")
    }

    input = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    val inputMapOp = input.getOrElse(throw new ParserException("Can't load raster! Ouch!"))
    zoom = MapOp.decodeInt(node.getChild(1)).getOrElse(
      throw new ParserException("Expected a zoom level for the second argument instead of " +
        node.getChild(1).getName))
    if (zoom < 0) {
      throw new ParserException(s"The zoom level must be between 1 and the max zoom level of the input raster")
    }
  }
}
