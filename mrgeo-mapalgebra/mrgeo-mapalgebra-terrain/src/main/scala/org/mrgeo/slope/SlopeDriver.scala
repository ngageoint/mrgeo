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

package org.mrgeo.slope

import java.awt.image.{DataBuffer, Raster}
import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.util.Properties
import javax.vecmath.Vector3d

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.spark.TileNeighborhood
import org.mrgeo.spark.job.{MrGeoJob, JobArguments, MrGeoDriver}
import org.mrgeo.utils.{LatLng, TMSUtils, SparkUtils}

import scala.collection.mutable

object SlopeDriver extends MrGeoDriver with Externalizable {
  final private val Input = "input"
  final private val Output = "output"
  final private val Units = "units"


  def slope(input:String, units:String, output:String, conf:Configuration) = {
    val args =  mutable.Map[String, String]()

    val name = "Slope (" + input + ")"

    args += Input -> input
    args += Units -> units
    args += Output -> output

    run(name, classOf[SlopeDriver].getName, args.toMap, conf)
  }

  override def writeExternal(out: ObjectOutput): Unit = {}
  override def readExternal(in: ObjectInput): Unit = {}

  override def setup(job: JobArguments): Boolean = {
    true
  }
}

class SlopeDriver extends MrGeoJob with Externalizable {

  var input:String = null
  var output:String = null
  var units:String = null

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes.result()

  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    input = job.getSetting(SlopeDriver.Input)
    output = job.getSetting(SlopeDriver.Output)
    units = job.getSetting(SlopeDriver.Units)

    true
  }

  private def calculateSlope(tiles:RDD[(Long, TileNeighborhood)], dx:Double, dy:Double, nodata:Double) = {

    tiles.map(tile => {
      val anchor = RasterWritable.toRaster(tile._2.anchor)
      val anchorX = tile._2.anchorX()
      val anchorY = tile._2.anchorY()

      val np = 0
      val zp = 1
      val pp = 2
      val nz = 3
      val zz = 4
      val pz = 5
      val nn = 6
      val zn = 7
      val pn = 8

      val elevations = {
        val neighborhood = tile._2
        val rasters = Array.ofDim[Raster](neighborhood.height, neighborhood.width)

        for (y <- 0 until neighborhood.height) {
          for (x <- 0 until neighborhood.width) {
            rasters(y)(x) = RasterWritable.toRaster(neighborhood.neighborAbsolute(x, y))
          }
        }
        rasters
      }

      def isnodata(v:Double, nodata:Double):Boolean = {
        (nodata.isNaN && v.isNaN) || (v == nodata)
      }

      def getElevation(x:Int, y:Int):Double = {

        var offsetX = 0
        var offsetY = 0
        if (x < 0) {
          offsetX = -1
        }
        else if (x >= anchor.getWidth) {
          offsetX = 1
        }
        if (y < 0) {
          offsetY = -1
        }
        else if (y >= anchor.getWidth) {
          offsetY = 1
        }

        if (offsetX == 0 && offsetY == 0) {
          return anchor.getSampleDouble(x, y, 0)
        }

        var px:Int = x
        if (offsetX < 0) {
          px = anchor.getWidth + x
        }
        else if (offsetX > 0) {
          px = x - anchor.getWidth
        }

        var py:Int = y
        if (offsetY < 0) {
          py = anchor.getHeight + y
        }
        else if (offsetY > 0) {
          py = y - anchor.getHeight
        }

        val elevation = elevations(anchorY + offsetY)(anchorX + offsetX)

        elevation.getSampleDouble(px, py, 0)
      }

      def calculateNormal(x: Int, y: Int): (Double, Double, Double) = {

        // if the origin pixel is nodata, the normal is nodata
        val origin = anchor.getSampleDouble(x, y, 0)
        if (isnodata(origin, nodata)) {
          return (nodata, nodata, nodata)
        }

        // get the elevations of the 3x3 grid of elevations, if a neighbor is nodata, make the elevation
        // the same as the origin, this makes the slopes a little prettier
        val z = Array.ofDim[Double](9)
        var ndx = 0
        for (dy <- y - 1 to y + 1) {
          for (dx <- x - 1 to x + 1) {
            z(ndx) = getElevation(dx, dy)
            if (isnodata(z(ndx), nodata)) {
              z(ndx) = origin
            }

            ndx += 1
          }
        }

        val vx = new Vector3d(dx, 0.0, ((z(pp) + z(pz) * 2 + z(pn)) - (z(np) + z(nz) * 2 + z(nn))) / 8.0)
        val vy = new Vector3d(0.0, dy, ((z(pp) + z(zp) * 2 + z(np)) - (z(pn) + z(zn) * 2 + z(nn))) / 8.0)

        val normal = new Vector3d()

        normal.cross(vx, vy)
        normal.normalize()
        // we want the normal to always point up.
        normal.z = Math.abs(normal.z)

        (normal.x, normal.y, normal.z)
      }

      def calculateSlope(x: Int, y: Int, normal: (Double, Double, Double)): Float = {
        if (normal._1.isNaN) {
          return Float.NaN
        }

        val up: Vector3d = new Vector3d(0, 0, 1.0)
        val theta: Double = Math.acos(up.dot(new Vector3d(normal._1, normal._2, normal._3)))

        if (units.equalsIgnoreCase("deg")) {
          (theta * 180.0 / Math.PI).toFloat
        }
        else if (units.equalsIgnoreCase("rad")) {
          theta.toFloat
        }
        else if (units.equalsIgnoreCase("percent")) {
          (Math.tan(theta) * 100.0).toFloat
        }
        else {
          Math.tan(theta).toFloat
        }
      }

      val sample = elevations(0)(0)
      val slope = RasterUtils.createEmptyRaster(sample.getWidth, sample.getHeight, 1, DataBuffer.TYPE_FLOAT, Float.NaN)

      for (y <- 0 until sample.getHeight) {
        for (x <- 0 until sample.getWidth) {
          val normal = calculateNormal(x, y)

          slope.setSample(x, y, 0, calculateSlope(x, y, normal))
        }
      }

      (new TileIdWritable(tile._1), RasterWritable.toWritable(slope))
    })
  }


  override def execute(context: SparkContext): Boolean =
  {

    val ip = DataProviderFactory.getMrsImageDataProvider(input, AccessMode.READ, null.asInstanceOf[Properties])

    val metadata: MrsImagePyramidMetadata = ip.getMetadataReader.read
    val zoom = metadata.getMaxZoomLevel
    val pyramid = SparkUtils.loadMrsPyramid(ip, zoom, context)

    val tiles = TileNeighborhood.createNeighborhood(pyramid, -1, -1, 3, 3,
      zoom, metadata.getTilesize, metadata.getDefaultValue(0), context)

    // todo:  The old code used great circle to calculate this.  I don't think that was necessarry, but we
    //        may want to investigate
    val res = TMSUtils.resolution(zoom, metadata.getTilesize) * LatLng.METERS_PER_DEGREE

    tiles.count()

    val slope = calculateSlope(tiles, res, res, metadata.getDefaultValue(0))

    slope.count()

    val op = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.WRITE, null.asInstanceOf[Properties])
    SparkUtils.saveMrsPyramid(slope, op, ip, zoom, context.hadoopConfiguration, null)

    true
  }




  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    input = in.readUTF()
    output = in.readUTF()
    units = in.readUTF()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(input)
    out.writeUTF(output)
    out.writeUTF(units)
  }
}
