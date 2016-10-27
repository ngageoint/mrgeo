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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils
import org.mrgeo.utils.tms.{Bounds, TMSUtils, TileBounds}

import scala.util.control.Breaks

object MosaicMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("mosaic")
  }
  def create(first:RasterMapOp, others: Array[RasterMapOp]):MapOp =
    new MosaicMapOp(List(first) ++ others)

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new MosaicMapOp(node, variables)
}

class MosaicMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD:Option[RasterRDD] = None
  private var inputs:Array[Option[RasterMapOp]] = null

  private[mapalgebra] def this(rasters:Seq[RasterMapOp]) = {
    this()

    inputs = rasters.map(Some(_)).toArray
  }

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 2) {
      throw new ParserException(node.getName + " requires at least two arguments")
    }

    val inputBuilder = Array.newBuilder[Option[RasterMapOp]]
    for (i <- 0 until node.getNumChildren) {
      inputBuilder += RasterMapOp.decodeToRaster(node.getChild(i), variables)
    }

    inputs = inputBuilder.result()
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  @SuppressFBWarnings(value = Array("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"), justification = "tileIdOrdering() - false positivie")
  override def execute(context: SparkContext): Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    val pyramids = Array.ofDim[RasterRDD](inputs.length)
    val nodata = Array.ofDim[Array[Double]](inputs.length)

    var i: Int = 0
    var zoom: Int = -1
    var tilesize: Int = -1
    var tiletype: Int = -1
    var numbands: Int = -1
    var bounds: Bounds = null

    // loop through the inputs and load the pyramid RDDs and metadata
    for (input <- inputs) {

      input match {
      case Some(pyramid) =>
        pyramids(i) = pyramid.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + pyramid.getClass.getName))
        val meta = pyramid.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + pyramid.getClass.getName))

        nodata(i) = meta.getDefaultValues

        // check for the same max zooms
        if (zoom < 0) {
          zoom = meta.getMaxZoomLevel
        }
        else if (zoom != meta.getMaxZoomLevel) {
          throw new IllegalArgumentException("All images must have the same max zoom level. " +
              "one is " + meta.getMaxZoomLevel + ", others are " + zoom)
        }

        if (tilesize < 0) {
          tilesize = meta.getTilesize
        }
        else if (tilesize != meta.getTilesize) {
          throw new IllegalArgumentException("All images must have the same tilesize. " +
              "one is " + meta.getTilesize + ", others are " + tilesize)
        }

        if (tiletype < 0) {
          tiletype = meta.getTileType
        }
        else if (tiletype != meta.getTileType) {
          throw new IllegalArgumentException("All images must have the same tile type. " +
              "one is " + meta.getTileType + ", others are " + tiletype)
        }

        if (numbands < 0) {
          numbands = meta.getBands
        }
        else if (numbands != meta.getBands) {
          throw new IllegalArgumentException("All images must have the same number of bands. " +
              "one is " + meta.getBands + ", others are " + numbands)
        }

        // expand the total bounds
        if (bounds == null) {
          bounds = meta.getBounds
        }
        else {
          bounds = bounds.expand(meta.getBounds)
        }

        i += 1

      case _ =>
      }
    }

    val tileBounds: TileBounds = TMSUtils.boundsToTile(bounds, zoom, tilesize)

    logDebug("Bounds: " + bounds.toString)
    logDebug("TileBounds: " + tileBounds.toString)

    // cogroup needs a partitioner, so we'll give one here...
    var maxpartitions = 0
    val partitions = pyramids.foreach(p => {
      if (p.partitions.length > maxpartitions) {
        maxpartitions = p.partitions.length
      }
    })
    val groups = new CoGroupedRDD(pyramids, new HashPartitioner(maxpartitions))

    rasterRDD = Some(RasterRDD(groups.map(U => {

      def isnodata(sample: Double, nodata: Double): Boolean = {
        if (nodata.isNaN) {
          if (sample.isNaN) return true
        }
        else if (nodata == sample) return true
        false
      }

      var dst: MrGeoRaster = null
      var dstnodata: Array[Double] = null

      val done = new Breaks
      var img: Int = 0
      done.breakable {
        for (wr <- U._2) {
          if (wr != null && wr.nonEmpty) {
            val writable = wr.asInstanceOf[Seq[RasterWritable]].head

            if (dst == null) {
              dst = RasterWritable.toMrGeoRaster(writable)
              dstnodata = nodata(img)

              val looper = new Breaks

              // check if there are any nodatas in the 1st tile
              looper.breakable {
                var y: Int = 0
                while (y < dst.height()) {
                  var x: Int = 0
                  while (x < dst.width()) {
                    var b: Int = 0
                    while (b < dst.bands()) {
                      if (isnodata(dst.getPixelDouble(x, y, b), dstnodata(b))) {
                        looper.break()
                      }
                      b += 1
                    }
                    x += 1
                  }
                  y += 1
                }
                // we only get here if there aren't any nodatas, so we can just take the 1st tile verbatim
                done.break()
              }
            }
            else {
              // do the mosaic
              var hasnodata = false

              // the tile conversion is a WritableRaster, we can just typecast here
              val src = RasterWritable.toMrGeoRaster(writable)
              val srcnodata = nodata(img)

              var y: Int = 0
              while (y < dst.height()) {
                var x: Int = 0
                while (x < dst.width()) {
                  var b: Int = 0
                  while (b < dst.bands()) {
                    if (isnodata(dst.getPixelDouble(x, y, b), dstnodata(b))) {
                      val sample = src.getPixelDouble(x, y, b)
                      // if the src is also nodata, remember this, we still have to look in other tiles
                      if (isnodata(sample, srcnodata(b))) {
                        hasnodata = true
                      }
                      else {
                        dst.setPixel(x, y, b, sample)
                      }
                    }
                    b += 1
                  }
                  x += 1
                }
                y += 1
              }
              // we've filled up the tile, nothing left to do...
              if (!hasnodata) {
                done.break()
              }
            }
          }
          img += 1
        }
      }

      // write the tile...
      (new TileIdWritable(U._1), RasterWritable.toWritable(dst))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, nodata(0),
      bounds = bounds, calcStats = false))

    true

  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = true


  override def teardown(job: JobArguments, conf:SparkConf): Boolean = true

  override def writeExternal(out: ObjectOutput): Unit = {}

  override def readExternal(in: ObjectInput): Unit = {}
}
