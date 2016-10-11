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

package org.mrgeo.mapalgebra

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.SparkUtils

object BandExtractMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("bandextract", "extract", "be")
  }

  def create(raster:RasterMapOp, band:Int = 1) =
    new BandExtractMapOp(Some(raster), band)

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new BandExtractMapOp(node, true, variables)
}

class BandExtractMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None
  private var input: Option[RasterMapOp] = null
  private var band: Int = 0

  private[mapalgebra] def this(raster:Option[RasterMapOp], band:Int) = {
    this()
    input = raster
    this.band = band - 1
  }

  private[mapalgebra] def this(node:ParserNode, isSlope:Boolean, variables: String => Option[ParserNode]) = {
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

  override def rdd(): Option[RasterRDD] = rasterRDD


  @SuppressFBWarnings(value = Array("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"), justification = "tileIdOrdering() - false positivie")
  override def execute(context: SparkContext): Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    var zoom: Int = -1
    var tilesize: Int = -1
    var tiletype: Int = -1
    var totalbands: Int = 0

    input match {
      case Some(pyramid) =>
        val meta = pyramid.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + pyramid.getClass.getName))

        if (band >= meta.getBands) {
          throw new ParserException(s"The input raster has ${meta.getBands} bands. Cannot extract band $band")
        }
        // check for the same max zooms
        if (zoom < 0) {
          zoom = meta.getMaxZoomLevel
        }
        else if (zoom != meta.getMaxZoomLevel) {
          throw new IllegalArgumentException("All images must have the same max zoom level. " +
            "one is " + meta.getMaxZoomLevel + ", others are " + zoom)
        }

        if (tilesize < 0) {
          tilesize = meta.getTilesize
        }
        else if (tilesize != meta.getTilesize) {
          throw new IllegalArgumentException("All images must have the same tilesize. " +
            "one is " + meta.getTilesize + ", others are " + tilesize)
        }

        if (tiletype < 0) {
          tiletype = meta.getTileType
        }
        else if (tiletype != meta.getTileType) {
          throw new IllegalArgumentException("All images must have the same tile type. " +
            "one is " + meta.getTileType + ", others are " + tiletype)
        }
        val nodata = meta.getDefaultValue(band)
        val inputRDD = pyramid.rdd().getOrElse(throw new IOException("Can't load RDD! Ouch!"))
        rasterRDD = Some(RasterRDD(inputRDD.map(U => {
          val dst = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, tiletype, nodata)
          val sourceRaster = RasterWritable.toRaster(U._2)
          var y: Int = 0
          while (y < sourceRaster.getHeight) {
            var x: Int = 0
            while (x < sourceRaster.getWidth) {
              val v = sourceRaster.getSampleDouble(x, y, band)
              dst.setSample(x, y, 0, v)
              x += 1
            }
            y += 1
          }
          (U._1, RasterWritable.toWritable(dst))
        })))

        metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, meta.getDefaultValue(band),
          bounds = meta.getBounds, calcStats = false))
        true
      case _ =>
        throw new IOException("Can't work with input raster! Ouch!")
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    band = in.readInt()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(band)
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

}
