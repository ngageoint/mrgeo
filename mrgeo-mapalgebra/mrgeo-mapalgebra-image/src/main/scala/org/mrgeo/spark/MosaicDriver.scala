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

package org.mrgeo.spark

import java.awt.image.WritableRaster

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{PairRDDFunctions, RDD, CoGroupedRDD}
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.partitioners.ImageSplitGenerator
import org.mrgeo.job.{MrGeoDriver, JobArguments, MrGeoJob}
import org.mrgeo.utils.TMSUtils.TileBounds
import org.mrgeo.utils.{TMSUtils, Bounds, SparkUtils}

import scala.collection.mutable.ArrayBuffer
import scala.util.control._

object MosaicDriver extends MrGeoDriver {

  def mosaic(inputs: Array[String], output:String): Unit = {

    val args:ArrayBuffer[String] = new ArrayBuffer[String]

    args += "--inputs"
    args += inputs.mkString(",")

    args += "--output"
    args += output

    // not sure how to get these into the args
//    args += "--cluster"
//    args += "yarn"
//    args += "--executors"
//    args += "2"
//    args += "--executorMemory"
//    args += "19G"
//    args += "--cores"
//    args += "10"

    run(args.toArray)
  }

}

class MosaicDriver extends MrGeoJob {
  var inputs: Array[String] = null
  var output:String = null


  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes.result()
  }


  override def execute(context: SparkContext): Boolean = {

    val pyramids = Array.ofDim[RDD[(TileIdWritable, RasterWritable)]](inputs.length)
    val nodata = Array.ofDim[Array[Double]](inputs.length)

    var i:Int = 0
    var zoom:Int = -1
    var tilesize:Int = -1
    var tiletype:Int = -1
    var numbands:Int = -1
    val bounds:Bounds = new Bounds()

    // loop through the inputs and load the pyramid RDDs and metadata
    for (input <- inputs) {
      val pyramid = SparkUtils.loadMrsPyramid(input, context)
      pyramids(i) = pyramid._1

      nodata(i) = pyramid._2.getDefaultValues

      // check for the same max zooms
      if (zoom < 0) {
        zoom = pyramid._2.getMaxZoomLevel
      }
      else if (zoom != pyramid._2.getMaxZoomLevel) {
        throw new IllegalArgumentException("All images must have the same max zoom level. " +
            pyramid._2.getPyramid + " is " + pyramid._2.getMaxZoomLevel + ", others are " + zoom)
      }

      if (tilesize < 0) {
        tilesize = pyramid._2.getTilesize
      }
      else if (tilesize != pyramid._2.getTilesize) {
        throw new IllegalArgumentException("All images must have the same tilesize. " +
            pyramid._2.getPyramid + " is " + pyramid._2.getTilesize + ", others are " + tilesize)
      }

      if (tiletype < 0) {
        tiletype = pyramid._2.getTileType
      }
      else if (tiletype != pyramid._2.getTileType) {
        throw new IllegalArgumentException("All images must have the same tile type. " +
            pyramid._2.getPyramid + " is " + pyramid._2.getTileType + ", others are " + tiletype)
      }

      if (numbands < 0) {
        numbands = pyramid._2.getBands
      }
      else if (numbands != pyramid._2.getBands) {
        throw new IllegalArgumentException("All images must have the same number of bands. " +
            pyramid._2.getPyramid + " is " + pyramid._2.getBands + ", others are " + numbands)
      }

      // expand the total bounds
      bounds.expand(pyramid._2.getBounds)

      i += 1
    }

    val tileBounds:TileBounds = TMSUtils.boundsToTile(bounds.getTMSBounds, zoom, tilesize)

    // cogroup needs a partitioner, so we'll give one here...
    val splitGenerator =  new ImageSplitGenerator(tileBounds.w, tileBounds.s,
      tileBounds.e, tileBounds.n, zoom, 1)

    val sparkPartitioner = new SparkTileIdPartitioner(splitGenerator)

    val groups = new CoGroupedRDD(pyramids, sparkPartitioner)

    val mosaiced:PairRDDFunctions[TileIdWritable, RasterWritable] = groups.map(U => {

      def isnodata(sample:Double, nodata:Double): Boolean = {
        if (nodata.isNaN && sample.isNaN) {
          true
        }
        else {
          nodata == sample
        }
      }

      var dst: WritableRaster = null
      var dstnodata:Array[Double] = null

      val i = 0
      val done = new Breaks
      done.breakable {
        for (wr <- U._2) {
          if (wr != null) {
            val writable = wr.asInstanceOf[RasterWritable]
            if (dst == null) {
              // the tile conversion is a WritableRaster, we can just typecast here
              dst = RasterWritable.toRaster(writable).asInstanceOf[WritableRaster]
              dstnodata = nodata(i)

              val looper = new Breaks

              // check if there are any nodatas in the 1st tile
              looper.breakable {
                for (y <- 0 to dst.getHeight) {
                  for (x <- 0 to dst.getWidth) {
                    for (b <- 0 to dst.getNumBands) {
                      if (isnodata(dst.getSampleDouble(x, y, b), dstnodata(b))) {
                        looper.break()
                      }
                    }
                  }
                }
                // we only get here if there aren't any nondatas, so we can just take the 1st tile verbatim
                done.break()
              }
            }
            else {
              // do the mosaic
              var hasnodata = false

              // the tile conversion is a WritableRaster, we can just typecast here
              val src = RasterWritable.toRaster(writable).asInstanceOf[WritableRaster]
              val srcnodata = nodata(i)

              for (y <- 0 to dst.getHeight) {
                for (x <- 0 to dst.getWidth) {
                  for (b <- 0 to dst.getNumBands) {
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
        }
      }

      // write the tile...
      (new TileIdWritable(U._1), RasterWritable.toWritable(dst))
    })

    val job:Job = new Job()

    // save the new pyramid
    //TODO:  protection level & properties
    MrsImageDataProvider.setupMrsPyramidOutputFormat(job, output, bounds, zoom,
      tilesize, tiletype, numbands, "", null)

    mosaiced.saveAsNewAPIHadoopDataset(job.getConfiguration)

    true
  }

  override def setup(job: JobArguments): Boolean = {

    val in:String = job.getSetting("inputs")

    inputs = in.split(",")

    output = job.getSetting("output")

    job.name = "Mosaic (" + in + ")"

    true
  }


  override def teardown(job: JobArguments): Boolean = {
    true
  }

}
