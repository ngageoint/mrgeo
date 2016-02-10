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

import java.awt.image.Raster
import java.io._
import java.text.DecimalFormat

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.{RasterRDD, VectorRDD}
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.{Geometry, GeometryFactory, Point, WritableLineString}
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.utils.{LatLng, TMSUtils}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object LeastCostPathCalculator {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[LeastCostPathCalculator])

  @throws(classOf[IOException])
  def run(cdrdd: RasterRDD, cdMetadata: MrsPyramidMetadata, zoomLevel: Int,
          destrdd: VectorRDD, sparkContext: SparkContext): VectorRDD = {
    var lcp: LeastCostPathCalculator = null
    lcp = new LeastCostPathCalculator(cdrdd, cdMetadata.getTilesize, zoomLevel,
      destrdd, sparkContext)
    lcp.run()
  }
}

class LeastCostPathCalculator extends Externalizable
{
  private var cdrdd: RasterRDD = null
  private var pointsrdd: VectorRDD = null
  private var zoomLevel: Int = -1
  private var tileSize: Int = -1
  private var curRaster: Raster = null
  private var curTile: TMSUtils.Tile = null
  private var curTileBounds: TMSUtils.Bounds = null
  private var resolution: Double = .0
  private var curPixel: TMSUtils.Pixel = null
  private var curValue: Double = 0.0
  private var pathCost: Double = 0f
  private var pathDistance: Double = 0f
  private var df: DecimalFormat = new DecimalFormat("###.#")
  private var pathMinSpeed: Double = 1000000f
  private var pathMaxSpeed: Double = 0f
  private var numPoints: Long = 0
  private var numTiles: Int = 0
  private val dx: Array[Short] = Array[Short](-1, 0, 1, 1, 1, 0, -1, -1)
  private val dy: Array[Short] = Array[Short](-1, -1, -1, 0, 1, 1, 1, 0)
  private var tilecache = collection.mutable.Map[Long,Raster]()
  private var sparkContext: SparkContext = null

  @throws(classOf[IOException])
  def this(cdrdd: RasterRDD, tileSize: Int, zoomLevel: Int, destPoint: VectorRDD,
           sparkContext: SparkContext) {
    this()
    this.cdrdd = cdrdd
    this.tileSize = tileSize
    this.zoomLevel = zoomLevel
    this.pointsrdd = destPoint
    this.sparkContext = sparkContext
  }

  @throws(classOf[IOException])
  private def run(): VectorRDD =
  {
    try {
      cdrdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val destGeom = pointsrdd.first()._2
      if (!destGeom.isInstanceOf[Point]) {
        throw new IOException("Expected a point to be passed to LeastCostPath, but instead got " + destGeom)
      }
      val destPoint = destGeom.asInstanceOf[Point]
      curTile = TMSUtils.latLonToTile(destPoint.getY, destPoint.getX, zoomLevel, tileSize)
      curPixel = TMSUtils.latLonToTilePixelUL(destPoint.getY, destPoint.getX, curTile.tx, curTile.ty, zoomLevel, tileSize)
      val startTileId: Long = TMSUtils.tileid(curTile.tx, curTile.ty, zoomLevel)

      resolution = TMSUtils.resolution(zoomLevel, tileSize)
      curTile = TMSUtils.tileid(startTileId, zoomLevel)
      curTileBounds = TMSUtils.tileBounds(curTile.tx, curTile.ty, zoomLevel, tileSize)
      curRaster = getTile(curTile.tx, curTile.ty)
      curValue = curRaster.getSampleFloat(curPixel.px.toInt, curPixel.py.toInt, 0)
      if (curValue.isNaN) {
        throw new IllegalStateException(String.format("Destination point \"%s\" falls outside cost surface", destPoint))
      }
      pathCost = curValue
      val lcp = GeometryFactory.createLineString()
      addPixelToOutput(lcp)
      numPoints += 1
      while (next) {
        addPixelToOutput(lcp)
      }
      if (LeastCostPathCalculator.LOG.isDebugEnabled) {
        LeastCostPathCalculator.LOG.debug("Total points = " + numPoints + " and total tiles = " + numTiles)
      }
      lcp.setAttribute("VALUE", df.format(pathCost))
      lcp.setAttribute("DISTANCE", df.format(pathDistance))
      lcp.setAttribute("MINSPEED", df.format(pathMinSpeed))
      lcp.setAttribute("MAXSPEED", df.format(pathMaxSpeed))
      lcp.setAttribute("AVGSPEED", df.format(pathDistance / pathCost))
      val lcpData = new ListBuffer[(FeatureIdWritable, Geometry)]()
      lcpData += ((new FeatureIdWritable(1), lcp))
      VectorRDD(sparkContext.parallelize(lcpData))
    } finally {
      cdrdd.unpersist()
    }
  }

  private def next: Boolean = {
    if (LeastCostPathCalculator.LOG.isDebugEnabled) {
      LeastCostPathCalculator.LOG.debug("curPixel = " + curPixel + " with value " + curValue)
    }
    var candNextRaster: Raster = curRaster
    var candNextTile: TMSUtils.Tile = curTile
    var minNextRaster: Raster = null
    var minNextTile: TMSUtils.Tile = null
    var minXNeighbor: Short = Short.MaxValue
    var minYNeighbor: Short = Short.MaxValue
    val tileWidth: Int = tileSize
    val widthMinusOne: Short = (tileWidth - 1).toShort
    var leastValue: Double = curValue
    var deltaX: Long = 0
    var deltaY: Long = 0
    var i: Int = 0
    while (i < 8) {
      var xNeighbor: Short = (curPixel.px + dx(i)).toShort
      var yNeighbor: Short = (curPixel.py + dy(i)).toShort
      if (xNeighbor >= 0 && xNeighbor <= widthMinusOne && yNeighbor >= 0 && yNeighbor <= widthMinusOne) {
        candNextRaster = curRaster
        candNextTile = curTile
      }
      else if (xNeighbor == -1 && yNeighbor == -1) {
        candNextTile = new TMSUtils.Tile(curTile.tx - 1, curTile.ty + 1)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        xNeighbor = widthMinusOne
        yNeighbor = widthMinusOne
      }
      else if (xNeighbor >= 0 && xNeighbor <= widthMinusOne && yNeighbor == -1) {
        candNextTile = new TMSUtils.Tile(curTile.tx, curTile.ty + 1)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        yNeighbor = widthMinusOne
      }
      else if (xNeighbor == tileWidth && yNeighbor == -1) {
        candNextTile = new TMSUtils.Tile(curTile.tx + 1, curTile.ty + 1)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        xNeighbor = 0
        yNeighbor = widthMinusOne
      }
      else if (xNeighbor == tileWidth && yNeighbor >= 0 && yNeighbor <= widthMinusOne) {
        candNextTile = new TMSUtils.Tile(curTile.tx + 1, curTile.ty)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        xNeighbor = 0
      }
      else if (xNeighbor == tileWidth && yNeighbor == tileWidth) {
        candNextTile = new TMSUtils.Tile(curTile.tx + 1, curTile.ty - 1)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        xNeighbor = 0
        yNeighbor = 0
      }
      else if (xNeighbor >= 0 && xNeighbor <= widthMinusOne && yNeighbor == tileWidth) {
        candNextTile = new TMSUtils.Tile(curTile.tx, curTile.ty - 1)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        yNeighbor = 0
      }
      else if (xNeighbor == -1 && yNeighbor == tileWidth) {
        candNextTile = new TMSUtils.Tile(curTile.tx - 1, curTile.ty - 1)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        xNeighbor = widthMinusOne
        yNeighbor = 0
      }
      else if (xNeighbor == -1 && yNeighbor >= 0 && yNeighbor <= widthMinusOne) {
        candNextTile = new TMSUtils.Tile(curTile.tx - 1, curTile.ty)
        candNextRaster = getTile(candNextTile.tx, candNextTile.ty)
        xNeighbor = widthMinusOne
      }
      else {
        assert((true))
      }
      val value: Float = candNextRaster.getSampleFloat(xNeighbor, yNeighbor, 0)
      if (!value.isNaN && value >= 0 && value < leastValue) {
        minXNeighbor = xNeighbor
        minYNeighbor = yNeighbor
        minNextRaster = candNextRaster
        minNextTile = candNextTile
        leastValue = value
        deltaX = dx(i)
        deltaY = dy(i)
      }
      i += 1
    }

    if (leastValue == curValue) return false
    numPoints += 1
    val p1 = {
      val lat: Double = curTileBounds.n - (curPixel.py * resolution)
      val lon: Double = curTileBounds.w + (curPixel.px * resolution)
      new LatLng(lat, lon)
    }
    curPixel = new TMSUtils.Pixel(minXNeighbor, minYNeighbor)
    val deltaTime: Double = curValue - leastValue
    curValue = leastValue
    if (!(curTile == minNextTile)) {
      curRaster = minNextRaster
      curTile = minNextTile
      curTileBounds = TMSUtils.tileBounds(curTile.tx, curTile.ty, zoomLevel, tileSize)
    }
    val p2 = {
      val lat: Double = curTileBounds.n - (curPixel.py * resolution)
      val lon: Double = curTileBounds.w + (curPixel.px * resolution)
      new LatLng(lat, lon)
    }
    val deltaDistance = LatLng.calculateGreatCircleDistance(p1, p2)
    pathDistance += deltaDistance.toFloat
    val speed: Double = deltaDistance / deltaTime
    if (speed < pathMinSpeed) pathMinSpeed = speed
    if (speed > pathMaxSpeed) pathMaxSpeed = speed
    return true
  }

  private def getTile(tx: Long, ty: Long): Raster = {
    val tileid = TMSUtils.tileid(tx, ty, zoomLevel)
    val result = tilecache.get(tileid)
    result match {
      case Some(r) => r
      case None => {
        tilecache.clear()
        // Each time a tile needs to be loaded into the cache, get a 7 x 7 area
        // of tiles centered around the requested tile. Because of how LCP works,
        // it always requests consecutive tiles, so this should limit the number
        // of times overall that we have to filter the RDD.
        val filteredRdd = cdrdd.filter(tile => {
          val checkTile = TMSUtils.tileid(tile._1.get(), zoomLevel)
          checkTile.tx >= tx - 3 && checkTile.tx <= tx + 3 &&
            checkTile.ty >= ty - 3 && checkTile.ty <= ty + 3
        }).collect().foreach(U => {
          val raster = RasterWritable.toRaster(U._2)
          tilecache += (U._1.get() -> raster)
        })
        tilecache.get(tileid).get
      }
    }
  }

  private def addPixelToOutput(lineString: WritableLineString): Unit = {
    val lat: Double = curTileBounds.n - (curPixel.py * resolution)
    val lon: Double = curTileBounds.w + (curPixel.px * resolution)
    val point = GeometryFactory.createPoint(lon, lat)
    lineString.addPoint(point)
  }

  override def readExternal(in: ObjectInput): Unit = {
    zoomLevel = in.readInt()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(zoomLevel)
  }
}
