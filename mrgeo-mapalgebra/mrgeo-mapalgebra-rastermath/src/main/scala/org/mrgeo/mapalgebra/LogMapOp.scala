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

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.job.JobArguments
import org.mrgeo.utils.SparkUtils

object LogMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("log")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new LogMapOp(node, variables)
}

class LogMapOp extends RasterMapOp with Externalizable {

  private var inputMapOp:Option[RasterMapOp] = None
  private var base:Option[Double] = None
  private var rasterRDD:Option[RasterRDD] = None

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
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

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = meta.getDefaultValue(0)

    // precompute the denominator for the calculation
    val baseVal =
    if (base.isDefined) {
       Math.log(base.get)
    }
    else {
      1
    }

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(tile._2))

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata)) {
              raster.setSample(x, y, b, Math.log(v) / baseVal)
            }
          }
        }
      }

      (tile._1, RasterWritable.toWritable(raster))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, meta.getDefaultValues, calcStats = false))

    true
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
    base = in.readObject().asInstanceOf[Option[Double]]
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(base)

  }

}
