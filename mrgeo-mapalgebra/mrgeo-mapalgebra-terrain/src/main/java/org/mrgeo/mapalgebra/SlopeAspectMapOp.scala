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

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import javax.vecmath.Vector3d

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.{SparkUtils, LatLng, TMSUtils}

object SlopeAspectMapOp {
  final val Input = "input"
  final val Output = "output"
  final val Units = "units"
  final val Type = "type"

  final val Slope = "slope"
  final val Aspect = "aspect"

}

class SlopeAspectMapOp extends RasterMapOp with Externalizable {

  final val DEG_2_RAD: Double = 0.0174532925
  final val RAD_2_DEG: Double = 57.2957795
  final val TWO_PI:Double = 6.28318530718
  final val THREE_PI_OVER_2:Double = 4.71238898038

  private var inputMapOp:Option[RasterMapOp] = None
  private var units:String = "rad"
  private var slope:Boolean = true

  private var rasterRDD:Option[RasterRDD] = None

  private[mapalgebra] def this(node:ParserNode, isSlope:Boolean, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires at least one argument")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " requires only one or two arguments")
    }

    slope = isSlope

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    if (node.getNumChildren == 2) {
      units = MapOp.decodeString(node.getChild(1)) match {
      case Some(s) => s
      case _ => throw new ParserException("Error decoding string")
      }

      if (!(units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad") || units.equalsIgnoreCase("gradient") ||
          units.equalsIgnoreCase("percent"))) {
        throw new ParserException("units must be \"deg\", \"rad\", \"gradient\", or \"percent\".")
      }

    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    conf.set("spark.storage.memoryFraction", "0.2") // set the storage amount lower...
    conf.set("spark.shuffle.memoryFraction", "0.3") // set the shuffle higher

    true
  }

  private def calculate(tiles:RDD[(TileIdWritable, RasterWritable)], bufferX:Int, bufferY: Int, nodata:Double, zoom:Int, tilesize:Int) = {

    tiles.map(tile => {

      val raster = RasterWritable.toRaster(tile._2)

      val np = 0
      val zp = 1
      val pp = 2
      val nz = 3
      val zz = 4
      val pz = 5
      val nn = 6
      val zn = 7
      val pn = 8

      val bounds = TMSUtils.tileBounds(TMSUtils.tileid(tile._1.get, zoom), zoom, tilesize)

      // calculate the great circle distance of the tile (through the middle)
      val midx = bounds.w + ((bounds.e - bounds.w) / 2.0)
      val midy = bounds.s + ((bounds.n - bounds.s) / 2.0)
      val dx = LatLng.calculateGreatCircleDistance(new LatLng(midy, bounds.w), new LatLng(midy, bounds.e)) / tilesize
      val dy = LatLng.calculateGreatCircleDistance(new LatLng(bounds.n, midx), new LatLng(bounds.s, midx)) / tilesize

      val z = Array.ofDim[Double](9)

      val vx = new Vector3d(dx, 0.0, 0.0)
      val vy = new Vector3d(0.0, dy, 0.0)
      val normal = new Vector3d()
      val up = new Vector3d(0, 0, 1.0)  // z (up) direction

      var theta:Double = 0.0


      def isnodata(v:Double, nodata:Double):Boolean = if (nodata.isNaN) v.isNaN  else v == nodata

      def calculateNormal(x: Int, y: Int): (Double, Double, Double) = {

        // if the origin pixel is nodata, the normal is nodata

        val origin = raster.getSampleDouble(x, y, 0)
        if (isnodata(origin, nodata)) {
          return (Double.NaN, Double.NaN, Double.NaN)
        }

        // get the elevations of the 3x3 grid of elevations, if a neighbor is nodata, make the elevation
        // the same as the origin, this makes the slopes a little prettier
        var ndx = 0
        for (dy <- y - 1 to y + 1) {
          for (dx <- x - 1 to x + 1) {
            z(ndx) = raster.getSampleDouble(dx, dy, 0)
            if (isnodata(z(ndx), nodata)) {
              z(ndx) = origin
            }

            ndx += 1
          }
        }

        vx.z = ((z(pp) + z(pz) * 2 + z(pn)) - (z(np) + z(nz) * 2 + z(nn))) / 8.0
        vy.z = ((z(pp) + z(zp) * 2 + z(np)) - (z(pn) + z(zn) * 2 + z(nn))) / 8.0

        normal.cross(vx, vy)
        normal.normalize()

        // we want the normal to always point up.
        normal.z = Math.abs(normal.z)

        (normal.x, normal.y, normal.z)
      }

      def calculateAngle(normal: (Double, Double, Double)): Float = {
        if (normal._1.isNaN) {
          return Float.NaN
        }

        if (slope) {
          theta  = Math.acos(up.dot(new Vector3d(normal._1, normal._2, normal._3)))
        }
        else {  // aspect
          // change from (-Pi to Pi) to (0 to 2Pi), make 0 deg north (+ 3pi/2)
          // convert to clockwise (2pi -)
          theta = TWO_PI - (Math.atan2(normal._2, normal._1) + THREE_PI_OVER_2) % TWO_PI
        }

        units match {
        case "deg"  => (theta * RAD_2_DEG).toFloat
        case "rad" => theta.toFloat
        case "percent" => (Math.tan(theta) * 100.0).toFloat
        case _ => Math.tan(theta).toFloat
        }
      }

      val width = raster.getWidth - bufferX * 2
      val height = raster.getHeight - bufferY * 2

      val answer = RasterUtils.createEmptyRaster(width, height, 1, DataBuffer.TYPE_FLOAT) // , Float.NaN)

      for (y <- 0 until height) {
        for (x <- 0 until width) {
          val normal = calculateNormal(x + bufferX, y + bufferY)

          answer.setSample(x, y, 0, calculateAngle(normal))
        }
      }

      (new TileIdWritable(tile._1), RasterWritable.toWritable(answer))
    })
  }


  override def execute(context: SparkContext): Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    val tb = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(meta.getBounds), zoom, tilesize)

    val nodatas = Array.ofDim[Number](meta.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = meta.getDefaultValue(i)
    }

    val bufferX = 1
    val bufferY = 1

    val tiles = FocalBuilder.create(rdd, bufferX, bufferY, meta.getBounds, zoom, nodatas, context)

    rasterRDD =
        Some(RasterRDD(calculate(tiles, bufferX, bufferY, nodatas(0).doubleValue(), zoom, tilesize)))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, nodatas(0).doubleValue()))

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    units = in.readUTF()
    slope = in.readBoolean()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(units)
    out.writeBoolean(slope)
  }

}
