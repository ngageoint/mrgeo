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

package org.mrgeo.mapalgebra

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.{SparkUtils, TMSUtils}

object CropMapOp extends MapOpRegistrar {
  private[mapalgebra] val Crop = "crop"
  private[mapalgebra] val CropExact = "cropexact"

  override def register: Array[String] = {
    Array[String](Crop, CropExact)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CropMapOp(node, variables)
}

class CropMapOp extends RasterMapOp with Externalizable {
  private val EPSILON: Double = 1e-8

  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  private var bounds:TMSUtils.TileBounds = null
  private var cropBounds:TMSUtils.Bounds = null
  private var exact: Boolean = false

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 5) {
      throw new ParserException("Usage: crop(raster, w, s, e, n)")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    // get values unique for each function
    node.getName match {
    case CropMapOp.CropExact =>
      exact = true
    case _ =>
    }

    cropBounds = new TMSUtils.Bounds(MapOp.decodeDouble(node.getChild(1), variables).get,
      MapOp.decodeDouble(node.getChild(2), variables).get,
      MapOp.decodeDouble(node.getChild(3), variables).get,
      MapOp.decodeDouble(node.getChild(4), variables).get)

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
    val nodata = meta.getDefaultValue(0)

    bounds = TMSUtils.boundsToTile(cropBounds, zoom, tilesize)

    val filtered = rdd.filter(tile => {
      val t = TMSUtils.tileid(tile._1.get(), zoom)
      (t.tx >= bounds.w) && (t.tx <= bounds.e) && (t.ty >= bounds.s) && (t.ty <= bounds.n)
    })


    rasterRDD = Some(RasterRDD(if (exact) {
      val pp = calculateCrop(zoom, tilesize)

      val t = pp._1.py
      val r = pp._1.px
      val b = pp._2.py
      val l = pp._2.px

      filtered.map(tile => {
        val tt = TMSUtils.tileid(tile._1.get(), zoom)
        if ((tt.tx == bounds.w) || (tt.tx == bounds.e) || (tt.ty == bounds.s) || (tt.ty == bounds.n)) {
          var minCopyX:Long = 0
          var maxCopyX:Long = tilesize
          var minCopyY:Long = 0
          var maxCopyY:Long = tilesize

          if (tt.tx == bounds.w) {
            minCopyX = l
          }
          if (tt.tx == bounds.e) {
            maxCopyX = r
          }
          if (tt.ty == bounds.s) {
            maxCopyY = b
          }
          if (tt.ty == bounds.n) {
            minCopyY = t
          }

          val raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(tile._2))

          for (y <- 0 until raster.getHeight) {
            for (x <- 0 until raster.getWidth) {
              for (b <- 0 until raster.getNumBands) {
                if (x < minCopyX || x > maxCopyX || y < minCopyY || y > maxCopyY)
                {
                  raster.setSample(x, y, 0, nodata)
                }
              }
            }
          }

          (tile._1, RasterWritable.toWritable(raster))
        }
        else {
          tile
        }
      })
    }
    else {
      filtered
    }))

    val b = if (exact) {
      cropBounds
    }
    else {
      TMSUtils.tileToBounds(bounds, zoom, tilesize)
    }

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, meta.getDefaultValues,
      bounds = b.asBounds(), calcStats = false))

    true
  }

  private def calculateCrop(zoom:Int, tilesize:Int) = {
    var bottomRightWorldPixel: TMSUtils.Pixel = TMSUtils
        .latLonToPixelsUL(cropBounds.s, cropBounds.e, zoom, tilesize)

    val bottomRightAtPixelBoundary: TMSUtils.LatLon = TMSUtils
        .pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize)

    if (Math.abs(bottomRightAtPixelBoundary.lat - cropBounds.n) < EPSILON) {
      bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px, bottomRightWorldPixel.py - 1)
    }

    if (Math.abs(bottomRightAtPixelBoundary.lon - cropBounds.e) < EPSILON) {
      bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px - 1, bottomRightWorldPixel.py)
    }

    val bottomRightPt: TMSUtils.LatLon = TMSUtils
        .pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoom, tilesize)
    val b: TMSUtils.Bounds = new TMSUtils.Bounds(cropBounds.w, bottomRightPt.lat, bottomRightPt.lon, cropBounds.n)

    (
        TMSUtils.latLonToTilePixelUL(cropBounds.n, bottomRightPt.lon, bounds.e, bounds.n, zoom, tilesize),
        TMSUtils.latLonToTilePixelUL(bottomRightPt.lat, cropBounds.w, bounds.w, bounds.s, zoom, tilesize)
    )
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
    exact = in.readBoolean()
    bounds = TMSUtils.TileBounds.fromCommaString(in.readUTF())
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeBoolean(exact)
    out.writeUTF(bounds.toCommaString)
  }
}

