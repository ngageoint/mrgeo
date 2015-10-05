package org.mrgeo.mapalgebra

import java.awt.image.Raster
import java.io._
import java.text.DecimalFormat

import com.google.common.cache.{LoadingCache, CacheLoader, Cache, CacheBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.{RasterRDD, VectorRDD}
import org.mrgeo.geometry.Point
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.utils.{LatLng, HadoopUtils, TMSUtils}
import org.slf4j.{Logger, LoggerFactory}

object LeastCostPathCalculator {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[LeastCostPathCalculator])

  @throws(classOf[IOException])
  def run(cdrdd: RasterRDD, cdMetadata: MrsImagePyramidMetadata, zoomLevel: Int,
          destrdd: VectorRDD, outputName: String, sparkContext: SparkContext) {
    var lcp: LeastCostPathCalculator = null
    lcp = new LeastCostPathCalculator(cdrdd, cdMetadata.getTilesize, zoomLevel,
      destrdd, sparkContext)
    lcp.run(outputName)
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
  private var outputStr: String = ""
  private var pathCost: Double = 0f
  private var pathDistance: Double = 0f
  private var df: DecimalFormat = new DecimalFormat("###.#")
  private var pathMinSpeed: Double = 1000000f
  private var pathMaxSpeed: Double = 0f
  private var pathAvgSpeed: Double = 0f
  private var numPixels: Int = 0
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
  private def run(outputName: String): Unit =
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
      addPixelToOutput
      numPixels += 1
      while (next) {
        addPixelToOutput
      }
      pathAvgSpeed = pathDistance / pathCost
      val outFilePath: Path = new Path(outputName, "leastcostpaths.tsv")
      writeToFile(outFilePath)
      if (LeastCostPathCalculator.LOG.isDebugEnabled) {
        LeastCostPathCalculator.LOG.debug("Total pixels = " + numPixels + " and total tiles = " + numTiles)
      }
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
    for (i <- 0 until 8) {
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
    }

    if (leastValue == curValue) return false
    numPixels += 1
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

  @throws(classOf[IOException])
  private def writeToFile(outFilePath: Path) {
    val conf: Configuration = sparkContext.hadoopConfiguration
    val fs: FileSystem = outFilePath.getFileSystem(conf)
    if (fs.exists(outFilePath)) {
      fs.delete(outFilePath, true)
    }
    var br: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(outFilePath)))
    val finalOutputStr: String = packageFinalOutput
    br.write(finalOutputStr)
    br.close
    val columnsPath: Path = new Path(outFilePath.toUri.toString + ".columns")
    if (fs.exists(columnsPath)) {
      fs.delete(columnsPath, true)
    }
    br = new BufferedWriter(new OutputStreamWriter(fs.create(columnsPath)))
    var columns: String = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    columns += "<AllColumns firstLineHeader=\"false\">\n"
    columns += "<Column name=\"GEOMETRY\" type=\"Nominal\"/>\n"
    columns += "<Column name=\"VALUE\" type=\"Numeric\"/>\n"
    columns += "<Column name=\"DISTANCE\" type=\"Numeric\"/>\n"
    columns += "<Column name=\"MINSPEED\" type=\"Numeric\"/>\n"
    columns += "<Column name=\"MAXSPEED\" type=\"Numeric\"/>\n"
    columns += "<Column name=\"AVGSPEED\" type=\"Numeric\"/>\n"
    columns += "</AllColumns>\n"
    br.write(columns)
    br.close
  }

  private def packageFinalOutput: String = {
    val outStr: StringBuffer = new StringBuffer
    outStr.append("LINESTRING(")
    val indexOfLastComma: Int = outputStr.lastIndexOf(',')
    assert((indexOfLastComma > -1 && indexOfLastComma == outputStr.length - 1))
    outStr.append(outputStr.substring(0, indexOfLastComma))
    outStr.append(")\t")
    outStr.append(df.format(pathCost))
    outStr.append("\t")
    outStr.append(df.format(pathDistance))
    outStr.append("\t")
    outStr.append(df.format(pathMinSpeed))
    outStr.append("\t")
    outStr.append(df.format(pathMaxSpeed))
    outStr.append("\t")
    outStr.append(df.format(pathAvgSpeed))
    outStr.append("\n")
    return outStr.toString
  }

  private def addPixelToOutput {
    val lat: Double = curTileBounds.n - (curPixel.py * resolution)
    val lon: Double = curTileBounds.w + (curPixel.px * resolution)
    outputStr += (lon.toString + " " + lat.toString + ",")
  }

  override def readExternal(in: ObjectInput): Unit = {
    zoomLevel = in.readInt()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(zoomLevel)
  }
}
