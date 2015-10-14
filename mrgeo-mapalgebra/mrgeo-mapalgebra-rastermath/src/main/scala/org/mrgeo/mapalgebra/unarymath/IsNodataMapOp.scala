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

import java.awt.image.WritableRaster
import java.io.IOException

import org.apache.spark.SparkContext
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOpRegistrar, MapOp}
import org.mrgeo.utils.SparkUtils

object IsNodataMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("isNodata", "isNull")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new IsNodataMapOp(node, variables)
}

class IsNodataMapOp extends RawUnaryMathMapOp {

  var nodata:Double = Double.NegativeInfinity
  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  // Unfortunately
  override def execute(context: SparkContext): Boolean = {

    // our metadata is the same as the raster
    val meta = input.get.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    val rdd = input.get.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + input.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = meta.getDefaultValue(0)

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(tile._2))

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNodata(v, nodata)) {
              raster.setSample(x, y, b, 0)
            }
            else {
              raster.setSample(x, y, b, 1)
            }
          }
        }
      }
      (tile._1, RasterWritable.toWritable(raster))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, nodata))

    true
  }


  override private[unarymath] def function(a: Double): Double = { Double.NaN }
}
