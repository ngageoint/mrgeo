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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.FocalBuilder
import org.mrgeo.utils.tms.TMSUtils
import org.mrgeo.utils.{LatLng, SparkUtils}

object Slope8MapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("slope8", "directionalslope", "dirslope")
  }

  def create(raster:RasterMapOp, units:String = "rad"):MapOp = {
    new Slope8MapOp(Some(raster), units, true)
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new Slope8MapOp(node, true, variables)
}

// Dummy class definition to allow the python reflection to find the Slope mapop
class Slope8MapOp extends RasterMapOp with Externalizable {

  final val DEG_2_RAD:Double = 0.0174532925
  final val RAD_2_DEG:Double = 57.2957795
  final val TWO_PI:Double = 2 * Math.PI
  final val THREE_PI_OVER_2:Double = (3.0 * Math.PI) / 2

  private var inputMapOp:Option[RasterMapOp] = None
  private var units:String = "rad"
  //private var slope:Boolean = true

  private var rasterRDD:Option[RasterRDD] = None

  override def rdd():Option[RasterRDD] = rasterRDD

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

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


    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, Array.fill(8)(Double.NaN),
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

  private[mapalgebra] def this(inputMapOp:Option[RasterMapOp], units:String, isSlope:Boolean) = {
    this()

    this.inputMapOp = inputMapOp

    if (!(units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad") || units.equalsIgnoreCase("gradient") ||
          units.equalsIgnoreCase("percent"))) {
      throw new ParserException("units must be \"deg\", \"rad\", \"gradient\", or \"percent\".")
    }
    this.units = units

  }

  private[mapalgebra] def this(node:ParserNode, isSlope:Boolean, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires at least one argument")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " requires only one or two arguments")
    }

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

  private def calculate(tiles:RDD[(TileIdWritable, RasterWritable)], bufferX:Int, bufferY:Int, nodata:Double,
                        zoom:Int, tilesize:Int) = {

    tiles.map(tile => {

      val raster = RasterWritable.toMrGeoRaster(tile._2)

      //      val tb = TMSUtils.tileBounds(TMSUtils.tileid(tile._1.get(), zoom), zoom, tilesize)

      val out_u = 0
      val out_ur = 1
      val out_r = 2
      val out_br = 3
      val out_b = 4
      val out_bl = 5
      val out_l = 6
      val out_ul = 7

      val in_ul = 0
      val in_u = 1
      val in_ur = 2
      val in_l = 3
      val in_c = 4
      val in_r = 5
      val in_bl = 6
      val in_b = 7
      val in_br = 8


      val dist = TMSUtils.resolution(zoom, tilesize) * LatLng.METERS_PER_DEGREE
      val diagdist = Math.sqrt(2.0 * dist * dist)

      //val up = new Vector3d(0, 0, 1.0)  // z (up) direction

      def isnodata(v:Double, nodata:Double):Boolean = if (nodata.isNaN) {
        v.isNaN
      }
      else {
        v == nodata
      }

      def calculateOffsets(x:Int, y:Int):Array[(Double, Double)] = {
        val vectors = Array.ofDim[(Double, Double)](8)

        // if the origin pixel is nodata, the vectors are nodata
        val origin = raster.getPixelDouble(x, y, 0)
        if (isnodata(origin, nodata)) {
          var ndx = 0
          while (ndx < 8) {
            vectors(ndx) = (Double.NaN, Double.NaN)
            ndx += 1
          }
          return vectors
        }

        // get the elevations of the 3x3 grid of elevations, if a neighbor is nodata, make the elevation
        // the same as the origin, this makes the slopes a little prettier
        var ndx = 0
        var dy:Int = y - 1
        while (dy <= y + 1) {
          var dx:Int = x - 1
          while (dx <= x + 1) {
            // just skip the center
            if (ndx != in_c) {
              val dz = {
                val e = raster.getPixelDouble(dx, dy, 0)
                if (isnodata(e, nodata)) {
                  0
                }
                else {
                  e - origin
                }
              }

              val vector = if (ndx == in_u) {
                (dist, dz)
              }
              else if (ndx == in_ur) {
                (diagdist, dz)
              }
              else if (ndx == in_r) {
                (dist, dz)
              }
              else if (ndx == in_br) {
                (diagdist, dz)
              }
              else if (ndx == in_b) {
                (dist, dz)
              }
              else if (ndx == in_bl) {
                (diagdist, dz)
              }
              else if (ndx == in_l) {
                (dist, dz)
              }
              else if (ndx == in_ul) {
                (diagdist, dz)
              }
              else {
                (0.0, dz)
              }

              vectors({
                if (ndx == in_u) {
                  out_u
                }
                else if (ndx == in_ur) {
                  out_ur
                }
                else if (ndx == in_r) {
                  out_r
                }
                else if (ndx == in_br) {
                  out_br
                }
                else if (ndx == in_b) {
                  out_b
                }
                else if (ndx == in_bl) {
                  out_bl
                }
                else if (ndx == in_l) {
                  out_l
                }
                else {
                  // if (ndx == ul)
                  out_ul
                }

              }) = vector
            }
            ndx += 1
            dx += 1
          }
          dy += 1
        }

        vectors
      }

      @SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"),
        justification = "Scala generated code")
      def calculateAngle(offset:(Double, Double)):Float = {
        if (offset._1.isNaN || offset._1 == 0.0) {
          return Float.NaN
        }

        val theta = {
          // calcualte angle
          val ang = Math.atan2(offset._2, offset._1)

          // if angle is greater than 180, make it negative
          if (ang > Math.PI) {
            ang - TWO_PI
          }
          else {
            ang
          }
        }

        units match {
          case "deg" => (theta * RAD_2_DEG).toFloat
          case "rad" => theta.toFloat
          case "percent" => (Math.tan(theta) * 100.0).toFloat
          case _ => Math.tan(theta).toFloat
        }
      }

      val width = raster.width() - bufferX * 2
      val height = raster.height() - bufferY * 2

      val answer = MrGeoRaster.createEmptyRaster(width, height, 8, DataBuffer.TYPE_FLOAT) // , Float.NaN)

      var y:Int = 0
      while (y < height) {
        var x:Int = 0
        while (x < width) {
          val vectors = calculateOffsets(x + bufferX, y + bufferY)

          var band = 0
          while (band < answer.bands()) {
            answer.setPixel(x, y, band, calculateAngle(vectors(band)))
            band += 1
          }
          x += 1
        }
        y += 1
      }

      (new TileIdWritable(tile._1), RasterWritable.toWritable(answer))
    })
  }

}
