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
import javax.vecmath.Vector3d

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.utils.tms.TMSUtils
import org.mrgeo.utils.{LatLng, SparkUtils}

@SuppressFBWarnings(value = Array("UPM_UNCALLED_PRIVATE_METHOD"), justification = "Scala constant")
@SuppressFBWarnings(value = Array("UUF_UNUSED_FIELD"), justification = "Scala constant")
object SlopeAspectMapOp {

  final val RAD_2_DEG:Double = 57.2957795

  final private val np = 0
  final private val zp = 1
  final private val pp = 2
  final private val nz = 3
  final private val zz = 4
  final private val pz = 5
  final private val nn = 6
  final private val zn = 7
  final private val pn = 8


  private def isnodata(v:Double, nodata:Double):Boolean = if (nodata.isNaN) {
    v.isNaN
  }
  else {
    v == nodata
  }

  private def calculateNormal(raster:MrGeoRaster, x:Int, y:Int, mpd:Double,
                              nodata:Double):(Double, Double, Double) = {
    val z = Array.ofDim[Double](9)

    val vx = new Vector3d()
    val vy = new Vector3d()
    val normal = new Vector3d()

    // if the origin pixel is nodata, the normal is nodata
    val origin = raster.getPixelDouble(x, y, 0)
    if (isnodata(origin, nodata)) {
      return (Double.NaN, Double.NaN, Double.NaN)
    }

    // get the elevations of the 3x3 grid of elevations, if a neighbor is nodata, make the elevation
    // the same as the origin, this makes the slopes a little prettier
    var ndx = 0
    var dy:Int = y - 1
    while (dy <= y + 1) {
      var dx:Int = x - 1
      while (dx <= x + 1) {
        z(ndx) = raster.getPixelDouble(dx, dy, 0)
        if (isnodata(z(ndx), nodata)) {
          z(ndx) = origin
        }

        ndx += 1
        dx += 1
      }
      dy += 1
    }

    vx.x = mpd
    vx.y = 0.0
    vx.z = ((z(pp) + z(pz) * 2 + z(pn)) - (z(np) + z(nz) * 2 + z(nn))) / 8.0

    vy.x = 0.0
    vy.y = mpd
    vy.z = ((z(pp) + z(zp) * 2 + z(np)) - (z(pn) + z(zn) * 2 + z(nn))) / 8.0

    normal.cross(vx, vy)
    normal.normalize()

    // we want the normal to always point up.
    normal.z = Math.abs(normal.z)

    (normal.x, normal.y, normal.z)
  }

}

abstract class SlopeAspectMapOp extends RasterMapOp with Externalizable {

  //  final val DEG_2_RAD: Double = 0.0174532925

  private var inputMapOp:Option[RasterMapOp] = None
  private var units:String = "rad"

  private var rasterRDD:Option[RasterRDD] = None

  override def rdd():Option[RasterRDD] = rasterRDD

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  def computeTheta(normal:(Double, Double, Double)):Double

  override def execute(context:SparkContext):Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse
               (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val tb = TMSUtils.boundsToTile(meta.getBounds, zoom, tilesize)

    val nodatas = meta.getDefaultValuesNumber

    val bufferX = 1
    val bufferY = 1

    val tiles = FocalBuilder.create(rdd, bufferX, bufferY, meta.getBounds, zoom, nodatas, context)

    rasterRDD =
        Some(RasterRDD(calculate(tiles, bufferX, bufferY, nodatas(0).doubleValue(), zoom, tilesize)))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, Array[Double](Double.NaN),
      bounds = meta.getBounds, calcStats = false))

    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def readExternal(in:ObjectInput):Unit = {
    units = in.readUTF()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeUTF(units)
  }

  private[mapalgebra] def initialize(inputMapOp:Option[RasterMapOp], units:String):Unit = {
    this.inputMapOp = inputMapOp

    if (!(units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad") || units.equalsIgnoreCase("gradient") ||
          units.equalsIgnoreCase("percent"))) {
      throw new ParserException("units must be \"deg\", \"rad\", \"gradient\", or \"percent\".")
    }
    this.units = units
  }

  private def calculate(tiles:RDD[(TileIdWritable, RasterWritable)], bufferX:Int, bufferY:Int, nodata:Double,
                        zoom:Int, tilesize:Int) = {

    tiles.map(tile => {
      val raster = RasterWritable.toMrGeoRaster(tile._2)

      val width = raster.width() - bufferX * 2
      val height = raster.height() - bufferY * 2

      val answer = MrGeoRaster.createEmptyRaster(width, height, 1, DataBuffer.TYPE_FLOAT)
      // val answer = RasterUtils.createEmptyRaster(width, height, 1, DataBuffer.TYPE_FLOAT) // , Float.NaN)

      val m = TMSUtils.resolution(zoom, tilesize) * LatLng.METERS_PER_DEGREE

      var y:Int = 0
      while (y < height) {
        var x:Int = 0
        while (x < width) {
          val normal = SlopeAspectMapOp.calculateNormal(raster, x + bufferX, y + bufferY, m, nodata)

          answer.setPixel(x, y, 0, calculateAngle(normal))
          x += 1
        }

        y += 1
      }

      (new TileIdWritable(tile._1), RasterWritable.toWritable(answer))
    })
  }

  @SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
  private def calculateAngle(normal:(Double, Double, Double)):Float = {
    if (normal._1.isNaN) {
      return Float.NaN
    }

    val theta = computeTheta(normal)
    units match {
      case "deg" => (theta * SlopeAspectMapOp.RAD_2_DEG).toFloat
      case "rad" => theta.toFloat
      case "percent" => (Math.tan(theta) * 100.0).toFloat
      case _ => Math.tan(theta).toFloat
    }
  }

}
