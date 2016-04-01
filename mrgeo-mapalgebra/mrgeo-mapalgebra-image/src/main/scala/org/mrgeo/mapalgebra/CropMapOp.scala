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

package org.mrgeo.mapalgebra

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.SparkUtils
import org.mrgeo.utils.tms.{TileBounds, Bounds, TMSUtils}

object CropMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("crop")
  }

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new CropMapOp(Some(raster), w, s, e, n)

  def create(raster:RasterMapOp, rasterForBounds: RasterMapOp):MapOp =
    new CropMapOp(Some(raster), Some(rasterForBounds))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CropMapOp(node, variables)
}


class CropMapOp extends RasterMapOp with Externalizable {
  private var rasterRDD: Option[RasterRDD] = None

  protected var inputMapOp: Option[RasterMapOp] = None
  protected var rasterForBoundsMapOp: Option[RasterMapOp] = None
  protected var bounds:TileBounds = null
  protected var cropBounds:Bounds = null

  private[mapalgebra] def this(raster:Option[RasterMapOp], w:Double, s:Double, e:Double, n:Double) = {
    this()

    inputMapOp = raster
    cropBounds = new Bounds(w, s, e, n)
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], rasterForBounds: Option[RasterMapOp]) = {
    this()

    inputMapOp = raster
    rasterForBoundsMapOp = rasterForBounds
  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 5 && node.getNumChildren != 2) {
      throw new ParserException("Usage: crop(raster, w, s, e, n) or crop(raster, rasterForBounds)")
    }
    parseChildren(node, variables)
  }

  protected def parseChildren(node: ParserNode, variables: String => Option[ParserNode]) = {
    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    if (node.getNumChildren == 2) {
      rasterForBoundsMapOp = RasterMapOp.decodeToRaster(node.getChild(1), variables)
    }
    else {
      cropBounds = new Bounds(MapOp.decodeDouble(node.getChild(1), variables).get,
        MapOp.decodeDouble(node.getChild(2), variables).get,
        MapOp.decodeDouble(node.getChild(3), variables).get,
        MapOp.decodeDouble(node.getChild(4), variables).get)
    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {

    val input: RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse
        (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize
    val nodatas = meta.getDefaultValues

    if (rasterForBoundsMapOp.isDefined) {
      cropBounds = rasterForBoundsMapOp.get.metadata().getOrElse(
        throw new IOException("Unable to get metadata for the bounds raster")).getBounds
    }
    bounds = TMSUtils.boundsToTile(cropBounds, zoom, tilesize)

    //    val filtered = rdd.filter(tile => {
    //      val t = TMSUtils.tileid(tile._1.get(), zoom)
    //      (t.tx >= bounds.w) && (t.tx <= bounds.e) && (t.ty >= bounds.s) && (t.ty <= bounds.n)
    //    })


    rasterRDD = Some(RasterRDD(

      rdd.flatMap(tile => {
        val t = TMSUtils.tileid(tile._1.get(), zoom)
        if ((t.tx >= bounds.w) && (t.tx <= bounds.e) && (t.ty >= bounds.s) && (t.ty <= bounds.n)) {
          processTile(tile, zoom, tilesize, nodatas)
        }
        else {
          Array.empty[(TileIdWritable, RasterWritable)].iterator
        }
      })))

    val b = getOutputBounds(zoom, tilesize)

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, meta.getDefaultValues,
      bounds = b, calcStats = false))

    true
  }

  protected def getOutputBounds(zoom: Int, tilesize: Int): Bounds = {
    // Use the bounds of the tiles processed
    TMSUtils.tileToBounds(bounds, zoom, tilesize)
  }

  protected def processTile(tile: (TileIdWritable, RasterWritable),
      zoom: Int, tilesize: Int,
      nodatas: Array[Double]): TraversableOnce[(TileIdWritable, RasterWritable)] = {
    Array(tile).iterator
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
    bounds = TileBounds.fromCommaString(in.readUTF())
    cropBounds = Bounds.fromCommaString(in.readUTF())
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(bounds.toCommaString)
    out.writeUTF(cropBounds.toCommaString)
  }
}

