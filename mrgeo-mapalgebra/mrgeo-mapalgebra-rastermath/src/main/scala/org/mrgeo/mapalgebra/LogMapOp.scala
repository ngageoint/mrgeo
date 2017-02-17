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

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils

object LogMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("log")
  }

  def create(raster:RasterMapOp, base:Double = 1.0):MapOp =
    new LogMapOp(Some(raster), Some(base))

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new LogMapOp(node, variables)
}

class LogMapOp extends RasterMapOp with Externalizable {

  private var inputMapOp:Option[RasterMapOp] = None
  private var base:Option[Double] = None
  private var rasterRDD:Option[RasterRDD] = None

  override def rdd():Option[RasterRDD] = rasterRDD

  override def execute(context:SparkContext):Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse
               (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))


    // precompute the denominator for the calculation
    val baseVal =
      if (base.isDefined) {
        Math.log(base.get)
      }
      else {
        1
      }

    val nodata = meta.getDefaultValues
    val outputnodata = Array.fill[Double](meta.getBands)(Float.NaN)

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toMrGeoRaster(tile._2)

      val output = MrGeoRaster.createEmptyRaster(raster.width(), raster.height(), raster.bands(), DataBuffer.TYPE_FLOAT)

      var y:Int = 0
      while (y < raster.height()) {
        var x:Int = 0
        while (x < raster.width()) {
          var b:Int = 0
          while (b < raster.bands()) {
            val v = raster.getPixelDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata(b))) {
              output.setPixel(x, y, b, Math.log(v) / baseVal)
            }
            else {
              output.setPixel(x, y, b, outputnodata(b))
            }
            b += 1
          }
          x += 1
        }
        y += 1
      }

      (tile._1, RasterWritable.toWritable(output))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, outputnodata,
      bounds = meta.getBounds, calcStats = false))

    true
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = true

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {
    base = in.readObject().asInstanceOf[Option[Double]]
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeObject(base)

  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], base:Option[Double]) = {
    this()
    inputMapOp = raster
    this.base = base
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires at least one argument")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " requires only one or two arguments")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    if (node.getNumChildren == 2) {
      base = MapOp.decodeDouble(node.getChild(1))
    }

  }

}
