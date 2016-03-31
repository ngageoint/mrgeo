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

package org.mrgeo.mapalgebra.unarymath

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.SparkUtils

abstract class RawUnaryMathMapOp extends RasterMapOp with Externalizable {
  var input:Option[RasterMapOp] = None
  var rasterRDD:Option[RasterRDD] = None

  private[unarymath] def initialize(node:ParserNode, variables: String => Option[ParserNode]) = {

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires one argument")
    }
    else if (node.getNumChildren > 1) {
      throw new ParserException(node.getName + " requires only one argument")
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

    val r = RasterWritable.toRaster(rdd.first()._2)
    val convert = datatype() != DataBuffer.TYPE_UNDEFINED && r.getSampleModel.getDataType != datatype()

    // copy this here to avoid serializing the whole mapop
    val nodatas = meta.getDefaultValues

    val outputnodata = if (convert)  Array.fill[Double](meta.getBands)(nodata()) else nodatas
    val outputdatatype = if (convert) datatype() else r.getSampleModel.getDataType

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toRaster(tile._2)

      val output = RasterUtils.createEmptyRaster(raster.getWidth, raster.getHeight, raster.getNumBands, outputdatatype)

      val width = raster.getWidth
      var b: Int = 0
      while (b < raster.getNumBands) {
        val pixels = raster.getSamples(0, 0, width, raster.getHeight, 0, null.asInstanceOf[Array[Double]])
        var y: Int = 0
        while (y < raster.getHeight) {
          var x: Int = 0
          while (x < width) {
            val v = pixels(y * width + x)
            if (RasterMapOp.isNotNodata(v, nodatas(b))) {
              output.setSample(x, y, b, function(v))
            }
            else {
              output.setSample(x, y, b, outputnodata(b))
            }
            x += 1
          }
          y += 1
        }
        b += 1
      }
      (tile._1, RasterWritable.toWritable(output))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, outputnodata,
      bounds = meta.getBounds, calcStats = false))

    true
  }

  private[unarymath] def function(a:Double):Double

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

  private[unarymath] def datatype():Int = { DataBuffer.TYPE_UNDEFINED }
  private[unarymath] def nodata():Double = { Float.NaN }


}
