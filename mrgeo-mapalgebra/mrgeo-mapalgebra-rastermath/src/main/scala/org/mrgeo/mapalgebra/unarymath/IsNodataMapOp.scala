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

import java.awt.image.DataBuffer
import java.io.IOException

import org.apache.spark.SparkContext
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.SparkUtils

object IsNodataMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("isNodata", "isNull")
  }
  def create(raster:RasterMapOp):MapOp =
    new IsNodataMapOp(Some(raster))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new IsNodataMapOp(node, variables)
}

class IsNodataMapOp extends RawUnaryMathMapOp {

  private[unarymath] def this(raster: Option[RasterMapOp]) = {
    this()
    input = raster
  }

  private[unarymath] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  // Unfortunately, the logic for isnodata uses nodata values, so we can't use the generic RawUnary execute
  override def execute(context: SparkContext): Boolean = {

    // our metadata is the same as the raster
    val meta = input.get.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    val rdd = input.get.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + input.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodatas = meta.getDefaultValues

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toRaster(tile._2)

      val output = RasterUtils.createEmptyRaster(raster.getWidth, raster.getHeight, raster.getNumBands, DataBuffer.TYPE_BYTE)

      var y: Int = 0
      while (y <  raster.getHeight) {
        var x: Int = 0
        while (x < raster.getWidth) {
          var b: Int = 0
          while (b < raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNodata(v, nodatas(b))) {
              output.setSample(x, y, b, 1)
            }
            else {
              output.setSample(x, y, b, 0)
            }
            b += 1
          }
          x += 1
        }
        y += 1
      }
      (tile._1, RasterWritable.toWritable(output))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, meta.getDefaultValues,
      bounds = meta.getBounds, calcStats = false))

    true
  }


  override private[unarymath] def function(a: Double): Double = { Double.NaN }
}
