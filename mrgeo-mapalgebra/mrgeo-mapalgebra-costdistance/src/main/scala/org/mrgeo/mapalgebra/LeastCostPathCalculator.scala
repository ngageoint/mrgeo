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

import java.io._
import java.text.DecimalFormat

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.{RasterRDD, VectorRDD}
import org.mrgeo.geometry.{Geometry, GeometryFactory, Point}
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.{Pixel, TMSUtils, Tile}
import org.mrgeo.utils.{LatLng, Logging}

import scala.collection.mutable

@SuppressFBWarnings(value = Array("NP_LOAD_OF_KNOWN_NULL_VALUE"), justification = "Scala generated code")
object LeastCostPathCalculator extends Logging {

  @throws(classOf[IOException])
  def run(costDist:RasterMapOp, destination:VectorRDD, context:SparkContext, zoom:Int = -1):VectorRDD = {

    val meta = costDist.metadata() getOrElse
               (throw new IOException("Can't load metadata! Ouch! " + costDist.getClass.getName))
    val rdd = costDist.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + costDist.getClass.getName))

    val localPersist = rdd.getStorageLevel == StorageLevel.NONE

    if (localPersist) {
      rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }

    val lcps = destination.collect.map(feature => {
      val lcp = GeometryFactory.createLineString()

      feature._2 match {
        case pt:Point =>
          val calculator = new LeastCostPathCalculator(pt, rdd, meta)

          while (calculator.hasnext) {
            lcp.addPoint(calculator.point)
          }

          val df:DecimalFormat = new DecimalFormat("###.###")

          if (lcp.getNumPoints == 0) {
            lcp.addPoint(pt)
            lcp.setAttribute("COST_S", df.format(0.0))
            lcp.setAttribute("DISTANCE_M", df.format(0.0))
            lcp.setAttribute("MINSPEED_MPS", df.format(0.0))
            lcp.setAttribute("MAXSPEED_MPS", df.format(0.0))
            lcp.setAttribute("AVGSPEED_MPS", df.format(0.0))
          }
          else {
            lcp.setAttribute("COST_S", df.format(calculator.totalcost))
            lcp.setAttribute("DISTANCE_M", df.format(calculator.totaldist))
            lcp.setAttribute("MINSPEED_MPS", df.format(calculator.minspeed))
            lcp.setAttribute("MAXSPEED_MPS", df.format(calculator.maxspeed))
            lcp.setAttribute("AVGSPEED_MPS", df.format(calculator.totaldist / calculator.totalcost))
          }

          (feature._1, lcp.asInstanceOf[Geometry])
        case g:Geometry =>
          throw new IOException("Expected a point to be passed to LeastCostPath, but instead got " + g)
      }
    })

    if (localPersist) {
      rdd.unpersist()
    }

    VectorRDD(context.parallelize(lcps))
  }

}

@SuppressFBWarnings(
  value = Array("NM_FIELD_NAMING_CONVENTION", "FE_FLOATING_POINT_EQUALITY", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"),
  justification = "1) false positive - case class NeighborData correctly named, 2 & 3) Scala generated code")
private class LeastCostPathCalculator(start:Point, rdd:RasterRDD, meta:MrsPyramidMetadata) extends Logging {
  val zoom = meta.getMaxZoomLevel
  val tilesize = meta.getTilesize
  private val pixelsize = (TMSUtils.resolution(zoom, tilesize) * LatLng.METERS_PER_DEGREE).toFloat
  private val pixelsizediag = Math.sqrt(2.0 * pixelsize * pixelsize).toFloat
  val neighborData = Array[NeighborData](
    NeighborData(-1, -1, pixelsizediag), // UP_LEFT),
    NeighborData(0, -1, pixelsize), // UP),
    NeighborData(1, -1, pixelsizediag), // UP_RIGHT),
    NeighborData(-1, 0, pixelsize), // LEFT),
    NeighborData(1, 0, pixelsize), // RIGHT),
    NeighborData(-1, 1, pixelsizediag), // DOWN_LEFT),
    NeighborData(0, 1, pixelsize), // DOWN),
    NeighborData(1, 1, pixelsizediag) // DOWN_RIGHT)
  )
  val maxPx = tilesize - 1
  private val CACHE_HALFWIDTH = 3
  var cache = mutable.HashMap.empty[Long, MrGeoRaster]
  var tile = TMSUtils.latLonToTile(start.getY, start.getX, zoom, tilesize)
  var pt = TMSUtils.latLonToTilePixelUL(start.getY, start.getX, tile.tx, tile.ty, zoom, tilesize)
  var totalcost:Float = getcost(0, 0)
  var totaldist:Float = 0.0f
  var minspeed:Float = Float.MaxValue
  var maxspeed:Float = 0

  def hasnext:Boolean = {
    var direction:Int = -1
    val curcost = getcost(0, 0)
    var mincost:Float = curcost

    if (log.isDebugEnabled) {
      logDebug("current point = " + pt + " with cost: " + curcost + " and dist: " + totaldist +
               " minspeed: " + minspeed + " maxspeed: " + maxspeed)
    }


    var ndx = 0
    while (ndx < neighborData.length) {
      val neighbor = neighborData(ndx)

      val cost = getcost(neighbor.dx, neighbor.dy)

      if (cost < mincost) {
        mincost = cost
        direction = ndx
      }

      ndx += 1
    }

    if (direction < 0) {
      return false
    }

    val neighbor = neighborData(direction)
    val (newx, newy, newtile) = newpoint(neighbor.dx, neighbor.dy)

    pt = new Pixel(newx, newy)
    tile = newtile

    val speed = neighbor.dist / (curcost - mincost)
    if (speed < minspeed) {
      minspeed = speed
    }
    if (speed > maxspeed) {
      maxspeed = speed
    }


    totaldist += neighbor.dist

    true
  }

  def point:Point = {
    val p = TMSUtils.tilePixelULToLatLon(pt.px, pt.py, tile, zoom, tilesize)
    GeometryFactory.createPoint(p.lon, p.lat)
  }

  def newpoint(dx:Int, dy:Int) = {
    val x = (pt.px + dx).toInt
    val y = (pt.py + dy).toInt

    val (px, t) = if (x < 0) {
      (tilesize + x, new Tile(tile.tx - 1, tile.ty))
    }
    else if (x > maxPx) {
      (x - tilesize, new Tile(tile.tx + 1, tile.ty))
    }
    else {
      (x, tile)
    }

    val (py, newtile) = if (y < 0) {
      (tilesize + y, new Tile(t.tx, t.ty + 1))
    }
    else if (y > maxPx) {
      (y - tilesize, new Tile(t.tx, t.ty - 1))
    }
    else {
      (y, t)
    }

    (px, py, newtile)
  }

  def getcost(dx:Int, dy:Int):Float = {
    val (newx, newy, newtile) = newpoint(dx, dy)

    val raster = gettile(newtile)

    if (raster == null) {
      Float.MaxValue
    }
    else {
      raster.getPixelFloat(newx, newy, 0)
    }
  }

  def gettile(t:Tile):MrGeoRaster = {
    val id = TMSUtils.tileid(t.tx, t.ty, zoom)

    cache.get(id) match {
      case Some(raster) => raster
      case _ =>
        updatecache(t)
        cache.getOrElse(id, null)
    }
  }

  def updatecache(t:Tile) = {
    val newcache = mutable.HashMap.empty[Long, MrGeoRaster]

    val tilebuilder = Array.newBuilder[Long]

    // 1st see if any of the new tiles are already loaded.  If so, just copy them into the new cache.
    // if not, put them into an array, so the filter can find them
    var dy = t.ty - CACHE_HALFWIDTH
    while (dy <= t.ty + CACHE_HALFWIDTH) {
      var dx = t.tx - CACHE_HALFWIDTH
      while (dx <= t.tx + CACHE_HALFWIDTH) {
        val id = TMSUtils.tileid(dx, dy, zoom)

        if (cache.contains(id)) {
          newcache.put(id, cache(id))
        }
        else {
          tilebuilder += id
        }
        dx += 1
      }
      dy += 1
    }

    val tilelist = tilebuilder.result()

    // filter the new tiles and put them into the new cache
    rdd.filter(tile => {
      tilelist.contains(tile._1.get)
    }).collect.foreach(tile => {
      newcache.put(tile._1.get, RasterWritable.toMrGeoRaster(tile._2))
    })

    cache = newcache
  }

  case class NeighborData(dx:Int, dy:Int, dist:Float)

}
