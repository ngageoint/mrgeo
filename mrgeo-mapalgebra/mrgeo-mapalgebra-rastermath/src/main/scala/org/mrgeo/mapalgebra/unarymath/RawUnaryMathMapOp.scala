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

package org.mrgeo.mapalgebra.unarymath

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.job.JobArguments
import org.mrgeo.utils.SparkUtils

import org.mrgeo.utils.MrGeoImplicits._

abstract class RawUnaryMathMapOp extends RasterMapOp with Externalizable {
  var input:Option[RasterMapOp] = None
  var rasterRDD:Option[RasterRDD] = None

  private[unarymath] def initialize(node:ParserNode, variables: String => Option[ParserNode]) = {

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires one arguments")
    }
    else if (node.getNumChildren > 1) {
      throw new ParserException(node.getName + " requires only two arguments")
    }

    input = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    val childA = node.getChild(0)


    if (input.isEmpty) {
      throw new ParserException("\"" + node.getName + "\" must have at least 1 raster input")
    }

    if (input.isDefined && !input.get.isInstanceOf[RasterMapOp]) {
      throw new ParserException("\"" + childA + "\" is not a raster input")
    }
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {

    // our metadata is the same as the raster
    val meta = input.get.metadata().get

    val rdd = input.get.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + input.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = meta.getDefaultValue(0)

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(tile._2))

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata)) {
              raster.setSample(x, y, b, function(v))
            }
          }
        }
      }
      (tile._1, RasterWritable.toWritable(raster))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, meta.getDefaultValues, calcStats = false))

    true
  }

  private[unarymath] def function(a:Double):Double

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

}
