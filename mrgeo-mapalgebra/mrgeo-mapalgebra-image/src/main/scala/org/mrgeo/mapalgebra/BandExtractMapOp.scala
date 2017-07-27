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
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils

object BandExtractMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("bandextract", "extract", "be")
  }

  def create(raster:RasterMapOp, band:Int = 1) =
    new BandExtractMapOp(Some(raster), band)

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new BandExtractMapOp(node, true, variables)
}

class BandExtractMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD:Option[RasterRDD] = None
  private var input:Option[RasterMapOp] = _
  private var band:Int = 0

  override def rdd():Option[RasterRDD] = rasterRDD

  override def getZoomLevel(): Int = {
    input.getOrElse(throw new IOException("No raster input specified")).getZoomLevel()
  }

  @SuppressFBWarnings(value = Array("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"),
    justification = "tileIdOrdering() - false positivie")
  override def execute(context:SparkContext):Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x:TileIdWritable, y:TileIdWritable):Int = x.compareTo(y)
    }

    input match {
      case Some(pyramid) =>
        val meta = pyramid.metadata() getOrElse
                   (throw new IOException("Can't load metadata! Ouch! " + pyramid.getClass.getName))

        if (band >= meta.getBands) {
          throw new ParserException(s"The input raster has ${meta.getBands} bands. Cannot extract band $band")
        }
        val zoom = meta.getMaxZoomLevel

        val inputRDD = pyramid.rdd().getOrElse(throw new IOException("Can't load RDD! Ouch!"))
        rasterRDD = Some(RasterRDD(inputRDD.map(U => {
          val sourceRaster = RasterWritable.toMrGeoRaster(U._2)
          // Create a new raster containing all the pixels of the specified band of the source
          val dst = sourceRaster.clip(0, 0, sourceRaster.width(), sourceRaster.height(), band)
          (U._1, RasterWritable.toWritable(dst))
        })))

        metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, meta.getDefaultValue(band),
          bounds = meta.getBounds, calcStats = false))
        true
      case _ =>
        throw new IOException("Can't work with input raster! Ouch!")
    }
  }

  override def readExternal(in:ObjectInput):Unit = {
    band = in.readInt()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeInt(band)
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = true

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  private[mapalgebra] def this(raster:Option[RasterMapOp], band:Int) = {
    this()
    input = raster
    this.band = band - 1
  }

  private[mapalgebra] def this(node:ParserNode, isSlope:Boolean, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 2) {
      throw new ParserException(node.getName + " requires two arguments - a source raster and a band number")
    }

    input = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    val inputMapOp = input.getOrElse(throw new ParserException("Can't load raster! Ouch!"))
    band = MapOp.decodeInt(node.getChild(1)).getOrElse(
      throw new ParserException("Expected a numeric band number in the second argument instead of " +
                                node.getChild(1).getName)) - 1
    if (band < 0) {
      throw new ParserException(s"The band must be between 1 and the number of bands in the input raster")
    }
  }

}
