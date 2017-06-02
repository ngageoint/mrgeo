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

package org.mrgeo.mapalgebra.binarymath

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils
import org.mrgeo.utils.tms.Bounds

abstract class RawBinaryMathMapOp extends RasterMapOp with Externalizable {
  var constA:Option[Double] = None
  var constB:Option[Double] = None

  var varA:Option[RasterMapOp] = None
  var varB:Option[RasterMapOp] = None

  var rasterRDD:Option[RasterRDD] = None

  override def setup(job:JobArguments, conf:SparkConf):Boolean = true

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def getZoomLevel(): Int = {
    varA match {
      case Some(a) => {
        val azoom = a.getZoomLevel()
        varB match {
          case Some(b) => {
            if (azoom != b.getZoomLevel()) {
              throw new IOException("Zoom levels do not match for " +
                this.getClass.getName)
            }
            azoom
          }
          case None => azoom
        }
      }
      case None => {
        varB match {
          case Some(b) => b.getZoomLevel()
          case None => throw new IOException(
            "No inputs from which to get the zoom level for " +
              this.getClass.getName)
        }
      }
    }
  }

  override def execute(context:SparkContext):Boolean = {
    rasterRDD =
        if (constA.isDefined) {
          computeWithConstantA(varB.get, constA.get)
        }
        else if (constB.isDefined) {
          computeWithConstantB(varA.get, constB.get)
        }
        else {
          compute(varA.get, varB.get)
        }
    true
  }

  override def readExternal(in:ObjectInput):Unit = {
    constA = in.readObject().asInstanceOf[Option[Double]]
    constB = in.readObject().asInstanceOf[Option[Double]]
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeObject(constA)
    out.writeObject(constB)
  }

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

  private[binarymath] def initialize(node:ParserNode, variables:String => Option[ParserNode]) = {

    if (node.getNumChildren < 2) {
      throw new ParserException(node.getName + " requires two arguments")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " requires only two arguments")
    }

    val childA = node.getChild(0)
    val childB = node.getChild(1)

    try {
      varA = RasterMapOp.decodeToRaster(childA, variables)
    }
    catch {
      case e:ParserException =>
        try {
          constA = MapOp.decodeDouble(childA, variables)
        }
        catch {
          case e:ParserException => throw new ParserException(
            "First term \"" + childA + "\" is not a raster or constant")
        }
    }
    try {
      varB = RasterMapOp.decodeToRaster(childB, variables)
    }
    catch {
      case e:ParserException =>
        try {
          constB = MapOp.decodeDouble(childB, variables)
        }
        catch {
          case e:ParserException => throw new ParserException(
            "Second term \"" + childB + "\" is not a raster or constant")
        }
    }

    if (constA.isEmpty && varA.isEmpty) {
      throw new ParserException("First term \"" + childA + "\" is invalid")
    }
    if (constB.isEmpty && varB.isEmpty) {
      throw new ParserException("Second term \"" + childA + "\" is invalid")
    }

    if (varA.isEmpty && varB.isEmpty) {
      throw new ParserException("\"" + node.getName + "\" must have at least 1 raster input")
    }
  }

  private[binarymath] def computeWithConstantA(raster:RasterMapOp, const:Double):Option[RasterRDD] = {

    val rdd = raster.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + raster.getClass.getName))

    val meta = raster.metadata()
        .getOrElse(throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodatas = meta.getDefaultValues

    val r1 = RasterWritable.toMrGeoRaster(rdd.first()._2)

    val outputnodata = if (datatype == r1.datatype()) {
      nodatas
    }
    else {
      Array.fill[Double](r1.bands())(nodata())
    }

    val answer = RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toMrGeoRaster(tile._2)

      val width = raster.width()
      val height = raster.height()
      val bands = raster.bands()

      val output = MrGeoRaster.createEmptyRaster(width, height, bands, datatype())

      var b:Int = 0
      while (b < bands) {
        var y:Int = 0
        while (y < height) {
          var x:Int = 0
          while (x < width) {
            val v = raster.getPixelDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodatas(b))) {
              output.setPixel(x, y, b, function(const, v))
            }
            else {
              output.setPixel(x, y, b, outputnodata(b))
            }

            x += 1
          }
          y += 1
        }
        b += 1
      }
      (tile._1, RasterWritable.toWritable(output))
    }))

    metadata(SparkUtils.calculateMetadata(answer, raster.metadata().get.getMaxZoomLevel, outputnodata,
      bounds = meta.getBounds, calcStats = false))

    Some(answer)

  }

  private[binarymath] def computeWithConstantB(raster:RasterMapOp, const:Double):Option[RasterRDD] = {

    val rdd = raster.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + raster.getClass.getName))

    val meta = raster.metadata()
        .getOrElse(throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodatas = meta.getDefaultValues

    val r1 = RasterWritable.toMrGeoRaster(rdd.first()._2)

    val outputnodata = if (datatype == r1.datatype()) {
      nodatas
    }
    else {
      Array.fill[Double](r1.bands())(nodata())
    }

    val answer = RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toMrGeoRaster(tile._2)

      val width = raster.width()
      val height = raster.height()
      val bands = raster.bands()

      val output = MrGeoRaster.createEmptyRaster(width, height, bands, datatype())

      var b:Int = 0
      while (b < bands) {
        var y:Int = 0
        while (y < height) {
          var x:Int = 0
          while (x < width) {
            val v = raster.getPixelDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodatas(b))) {
              output.setPixel(x, y, b, function(v, const))
            }
            else {
              output.setPixel(x, y, b, outputnodata(b))
            }
            x += 1
          }
          y += 1
        }
        b += 1
      }
      (tile._1, RasterWritable.toWritable(output))
    }))

    metadata(SparkUtils.calculateMetadata(answer, raster.metadata().get.getMaxZoomLevel, outputnodata,
      bounds = meta.getBounds, calcStats = false))

    Some(answer)

  }

  private[binarymath] def compute(raster1:RasterMapOp, raster2:RasterMapOp):Option[RasterRDD] = {
    val rdd1 = raster1.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + raster1.getClass.getName))
    val rdd2 = raster2.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + raster2.getClass.getName))

    val r1 = RasterWritable.toMrGeoRaster(rdd1.first()._2)
    val r2 = RasterWritable.toMrGeoRaster(rdd2.first()._2)

    // copy this here to avoid serializing the whole mapop
    val nodata1 = raster1.metadata() match {
      case Some(metadata) => metadata.getDefaultValues
      case _ =>
        Array.fill[Double](r1.bands())(Double.NaN)
    }
    val nodata2 = raster2.metadata() match {
      case Some(metadata) => metadata.getDefaultValues
      case _ =>
        Array.fill[Double](r2.bands())(Double.NaN)
    }

    val outputnodata = if (datatype == r1.datatype()) {
      nodata1
    }
    else if (datatype == r2.datatype()) {
      nodata2
    }
    else {
      Array.fill[Double](r1.bands())(nodata())
    }

    val convertr1 = r1.datatype() != datatype
    //val convertr2 = r2.getSampleModel.getDataType != datatype || !(nodata1 sameElements nodata2)

    // group the RDDs
    val group = new PairRDDFunctions(rdd1).cogroup(rdd2)

    val answer = RasterRDD(group.flatMap(tile => {
      val iter1 = tile._2._1
      val iter2 = tile._2._2

      // if raster 1 or 2 is missing, we can't do the binary math
      if (iter1.nonEmpty && iter2.nonEmpty) {
        // we know there are only 1 item in each group's iterator, so we can use head()
        val raster1 = RasterWritable.toMrGeoRaster(iter1.head)
        val raster2 = RasterWritable.toMrGeoRaster(iter2.head)

        val output = if (convertr1) {
          MrGeoRaster.createEmptyRaster(raster1.width(), raster1.height(), raster1.bands(), datatype())
        }
        else {
          raster1
        }

        val width = raster1.width()
        var b:Int = 0
        while (b < raster1.bands()) {
          var y:Int = 0
          while (y < raster1.height()) {
            var x:Int = 0
            while (x < width) {
              val v1 = raster1.getPixelDouble(x, y, b)
              if (RasterMapOp.isNotNodata(v1, nodata1(b))) {
                val v2 = raster2.getPixelDouble(x, y, b)
                if (RasterMapOp.isNotNodata(v2, nodata2(b))) {
                  output.setPixel(x, y, b, function(v1, v2))
                }
                else {
                  // if raster2 is nodata, we need to set raster1's pixel to nodata as well
                  output.setPixel(x, y, b, outputnodata(b))
                }
              }
              else if (convertr1) {
                output.setPixel(x, y, b, outputnodata(b))
              }
              x += 1
            }
            y += 1
          }
          b += 1
        }

        Array((tile._1, RasterWritable.toWritable(output))).iterator
      }
      else {
        Array.empty[(TileIdWritable, RasterWritable)].iterator
      }
    }))

    metadata(SparkUtils.calculateMetadata(answer, raster1.metadata().get.getMaxZoomLevel, outputnodata,
      bounds = Bounds.combine(raster1.metadata().get.getBounds, raster2.metadata().get.getBounds), calcStats = false))

    Some(answer)
  }

  private[binarymath] def function(a:Double, b:Double):Double

  private[binarymath] def datatype():Int = {
    DataBuffer.TYPE_FLOAT
  }

  private[binarymath] def nodata():Double = {
    Float.NaN
  }

}
