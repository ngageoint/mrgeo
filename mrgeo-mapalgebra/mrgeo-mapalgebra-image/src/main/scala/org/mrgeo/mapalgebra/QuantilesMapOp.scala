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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.quantiles.Quantiles

import scala.language.existentials


object QuantilesMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("quantiles")
  }

  def create(raster:RasterMapOp, numQuantiles:Int, fraction:Float = 1.0f) =
    new QuantilesMapOp(Some(raster), Some(numQuantiles), Some(fraction))

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new QuantilesMapOp(node, variables)
}

class QuantilesMapOp extends RasterMapOp with Externalizable {
  private var rasterRDD:Option[RasterRDD] = None

  private var inputMapOp:Option[RasterMapOp] = None
  private var numQuantiles:Option[Int] = None
  private var fraction:Option[Float] = None

  def this(node:ParserNode, variables:String => Option[ParserNode]) {
    this()

    if ((node.getNumChildren < 2) || (node.getNumChildren > 3)) {
      throw new ParserException(
        "quantiles usage: quantiles(source raster, num quantiles, [percent of pixels to use])")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    numQuantiles = MapOp.decodeInt(node.getChild(1), variables)
    if (numQuantiles.isEmpty) {
      throw new ParserException("The value for the numQuantiles parameter must be an integer")
    }
    if (node.getNumChildren > 2) {
      fraction = MapOp.decodeFloat(node.getChild(2), variables)
      fraction match {
        case Some(f) => {
          if ((f <= 0.0 || f > 1.0)) {
            throw new ParserException(
              "The fraction passed to quantiles " + f + " must be between 0.0 and 1.0")
          }
        }
        case None => throw new ParserException(
          "The value for the fraction parameter must be a number between 0.0 and 1.0");
      }
    }
  }

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

  override def registerClasses():Array[Class[_]] = {
    Array[Class[_]](classOf[Array[Double]],
      classOf[Array[Float]],
      classOf[Array[Int]],
      classOf[Array[Short]],
      classOf[Array[Byte]],
      classOf[Array[Object]]
    )
  }

  override def getZoomLevel(): Int = {
    inputMapOp.getOrElse(throw new IOException("No raster input specified")).getZoomLevel()
  }

  @SuppressFBWarnings(value = Array("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"),
    justification = "implicits - false positivie")
  override def execute(context:SparkContext):Boolean = {

    implicit val doubleOrdering = new Ordering[Double] {
      override def compare(x:Double, y:Double):Int = x.compareTo(y)
    }

    implicit val floatOrdering = new Ordering[Float] {
      override def compare(x:Float, y:Float):Int = x.compareTo(y)
    }

    implicit val intOrdering = new Ordering[Int] {
      override def compare(x:Int, y:Int):Int = x.compareTo(y)
    }

    implicit val shortOrdering = new Ordering[Short] {
      override def compare(x:Short, y:Short):Int = x.compareTo(y)
    }

    implicit val byteOrdering = new Ordering[Byte] {
      override def compare(x:Byte, y:Byte):Int = x.compareTo(y)
    }

    val input:RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))
    val numberOfQuantiles = numQuantiles getOrElse (throw new IOException("numQuantiles not valid!"))

    val meta = input.metadata() getOrElse
               (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    rasterRDD = input.rdd()
    val rdd = rasterRDD getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))
    // No reason to calculate metadata like raster map ops that actually compute a raster. This
    // map op does not compute the raster output, it just uses the input raster. All we need to
    // do is update the metadata already computed for the input raster map op.
    //    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, meta.getDefaultValues,
    //      bounds = meta.getBounds, calcStats = false))

    // Compute the quantile values and save them to metadata
    val quantiles = Quantiles.compute(rdd, numberOfQuantiles, fraction, meta)
    var b:Int = 0
    while (b < quantiles.length) {
      meta.setQuantiles(b, quantiles(b))
      b += 1
    }
    metadata(meta)
    true
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}

  private[mapalgebra] def this(raster:Option[RasterMapOp], numQuantiles:Option[Int],
                               fraction:Option[Float]) = {
    this()

    this.inputMapOp = raster
    this.numQuantiles = numQuantiles
    this.fraction = fraction
  }
}