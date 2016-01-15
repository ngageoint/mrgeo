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

import java.awt.image.WritableRaster
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.TMSUtils.TileBounds
import org.mrgeo.utils.{Bounds, SparkUtils, TMSUtils}

import scala.util.control.Breaks

object MosaicMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("mosaic")
  }
  def create(first:RasterMapOp, others:RasterMapOp*):MapOp =
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
    val bounds: Bounds = new Bounds()

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
        bounds.expand(meta.getBounds)

        i += 1

      case _ =>
      }
    }

    val tileBounds: TileBounds = TMSUtils.boundsToTile(bounds.getTMSBounds, zoom, tilesize)

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

      var dst: WritableRaster = null
      var dstnodata: Array[Double] = null

      val done = new Breaks
      var img: Int = 0
      done.breakable {
        for (wr <- U._2) {
          if (wr != null && wr.nonEmpty) {
            val writable = wr.asInstanceOf[Seq[RasterWritable]].head

            if (dst == null) {
              dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(writable))
              dstnodata = nodata(img)

              val looper = new Breaks

              // check if there are any nodatas in the 1st tile
              looper.breakable {
                for (y <- 0 until dst.getHeight) {
                  for (x <- 0 until dst.getWidth) {
                    for (b <- 0 until dst.getNumBands) {
                      if (isnodata(dst.getSampleDouble(x, y, b), dstnodata(b))) {
                        looper.break()
                      }
                    }
                  }
                }
                // we only get here if there aren't any nodatas, so we can just take the 1st tile verbatim
                done.break()
              }
            }
            else {
              // do the mosaic
              var hasnodata = false

              // the tile conversion is a WritableRaster, we can just typecast here
              val src = RasterUtils.makeRasterWritable(RasterWritable.toRaster(writable))
              val srcnodata = nodata(img)

              for (y <- 0 until dst.getHeight) {
                for (x <- 0 until dst.getWidth) {
                  for (b <- 0 until dst.getNumBands) {
                    if (isnodata(dst.getSampleDouble(x, y, b), dstnodata(b))) {
                      val sample = src.getSampleDouble(x, y, b)
                      // if the src is also nodata, remember this, we still have to look in other tiles
                      if (isnodata(sample, srcnodata(b))) {
                        hasnodata = true
                      }
                      else {
                        dst.setSample(x, y, b, sample)
                      }
                    }
                  }
                }
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
