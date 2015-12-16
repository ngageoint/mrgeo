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

import java.awt.image.{BandedSampleModel, DataBuffer, Raster, WritableRaster}
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util

import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.spark.{Accumulator, AccumulatorParam, SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.{RasterRDD, VectorRDD}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.{GeometryFactory, Point}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.utils._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * Given a friction surface input image, and a source point, compute the
 * cost to travel to each pixel from the source point out to an optional
 * maximum cost. The friction surface is single-band where pixel values
 * are seconds/meter. The source point can be either a vector data source
 * of the result of a map algebra function that returns vector data (like
 * InlineCsv).
 *
 * The general algorithm starts at the specified start pixel and computes
 * the cost to traverse from the center of the pixel to the center of each
 * of its eight neighbor pixels. That cost is stored in the output image
 * tile. Each time a neighbor pixel changes, it is added to the processing
 * queue and is eventually processed itself. Pixel values are only changed
 * if the cost from the "center" pixel being processed is smaller than the
 * current cost to reach that pixel. Note that pixels can be revisited
 * many times. The algorithm iterates until there are no more changed pixels
 * in the processing queue.
 *
 * Executing this algorithm on a tiled image adds complexity. The distributed
 * processing is implemented with Spark. Initially, a Spark RDD is constructed
 * that copies the original friction tiles into new 2-band tiles where the first
 * band contains those friction values, and the second band contains the current
 * cost to reach that pixel from the source point. The source point and the tile
 * it resides in are included in the set of changed pixels. The code loops until
 * there are no more changed pixels. Each pass through the loop performs a Spark
 * "map" operation over the 2-band tiles. If there are no pixel changes for that
 * tile, the original tile is simply returned. If there are changes, the tile
 * is re-computed based on the algorithm described above to update other pixels
 * within that tile. If any edge pixels in that tile change, they are added to
 * the accumulator. After the map operation completes, the content of the
 * accumulator becomes the list of changed pixels to process during the next
 * pass through the loop. This is continued until there are no more pixels
 * to process. Note that the result of each pass through the loop can result in
 * pixel changes across many tiles (which get processed in the next pass).
 * Note that the same tile can get processed multiple times before the
 * algorithm completes.
 */
object CostDistanceMapOp extends MapOpRegistrar {
  val SELF: Byte = 0
  val ABOVE: Byte = 1
  val BELOW: Byte = 2
  val LEFT: Byte = 3
  val RIGHT: Byte = 4
  val ABOVE_LEFT: Byte = 5
  val ABOVE_RIGHT: Byte = 6
  val BELOW_LEFT: Byte = 7
  val BELOW_RIGHT: Byte = 8
  val DIRECTION_COUNT: Byte = 9

  val LOG: Logger = LoggerFactory.getLogger(classOf[CostDistanceMapOp])

  override def register: Array[String] = {
    Array[String]("costDistance", "cd")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CostDistanceMapOp(node, variables)
}


class CostDistanceMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None

  var friction:Option[RasterMapOp] = None
  var sourcePointsMapOp:Option[VectorMapOp] = None
  var frictionZoom:Option[Int] = None
  var requestedBounds:Option[Bounds] = None
  var tileBounds: TMSUtils.TileBounds = null
  var numExecutors: Int = -1

  var maxCost:Double = -1
  var zoomLevel:Int = -1

  val neighborsAbove = Array((-1, TraversalDirection.UP_LEFT),
    (0, TraversalDirection.UP), (1, TraversalDirection.UP_RIGHT))
  val neighborsBelow = Array((-1, TraversalDirection.DOWN_LEFT),
    (0, TraversalDirection.DOWN), (1, TraversalDirection.DOWN_RIGHT))
  val neighborsToLeft = Array((-1, TraversalDirection.UP_LEFT),
    (0, TraversalDirection.LEFT), (1, TraversalDirection.DOWN_LEFT))
  val neighborsToRight = Array((-1, TraversalDirection.UP_RIGHT),
    (0, TraversalDirection.RIGHT), (1, TraversalDirection.DOWN_RIGHT))

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    val usage: String = "CostDistance takes the following arguments " +
        "(source point, [friction zoom level], friction raster, [maxCost], [minX, minY, maxX, maxY])"

    val numChildren = node.getNumChildren
    if (numChildren < 2 || numChildren > 8) {
      throw new ParserException(usage)
    }

    var nodeIndex: Int = 0
    sourcePointsMapOp = VectorMapOp.decodeToVector(node.getChild(nodeIndex), variables)
    nodeIndex += 1

    // Check for optional friction zoom level
    MapOp.decodeInt(node.getChild(nodeIndex)) match {
    case Some(i) =>
      frictionZoom = Some(i)
      nodeIndex += 1
    case _ =>
    }

    // Check that there are enough required arguments left
    if (numChildren <= nodeIndex) {
      throw new ParserException(usage)
    }

    friction = RasterMapOp.decodeToRaster(node.getChild(nodeIndex), variables)
    nodeIndex += 1

    // Check for optional max cost. Both max cost and bounds are optional, so there
    // must be either 1 argument left or 5 arguments left in order for max cost to
    // be included.
    if ((numChildren == (nodeIndex + 1)) || (numChildren == (nodeIndex + 5))) {
      MapOp.decodeDouble(node.getChild(nodeIndex), variables) match {
      case Some(d) =>
        maxCost = if (d < 0) -1 else d
        nodeIndex += 1
      case _ =>
      }
    }

    // Check for optional bounds
    if (numChildren > nodeIndex) {
      if (numChildren == nodeIndex + 4) {
        val b = Array.ofDim[Double](4)
        for (i <- 0 until 4) {
          b(i) = MapOp.decodeDouble(node.getChild(nodeIndex + i), variables) match {
          case Some(d) => d
          case _ => throw new ParserException("Can't decode double")
          }
        }
        requestedBounds = Some(new Bounds(b(0), b(1), b(2), b(3)))
      }
      else {
        throw new ParserException("The bounds argument must include minX, minY, maxX and maxY")
      }
    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    numExecutors = conf.getInt("spark.executor.instances", -1)
    CostDistanceMapOp.LOG.info("num executors = " + numExecutors)
//    conf.set("spark.kryo.registrationRequired", "true")
//    conf.set("spark.driver.extraJavaOptions", "-Dsun.io.serialization.extendedDebugInfo=true")
//    conf.set("spark.executor.extraJavaOptions", "-Dsun.io.serialization.extendedDebugInfo=true")
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {
    val t0 = System.nanoTime()

    val inputFriction:RasterMapOp = friction getOrElse(throw new IOException("Input MapOp not valid!"))
    val sourcePointsRDD = sourcePointsMapOp.getOrElse(throw new IOException("Missing source points")).
      rdd().getOrElse(throw new IOException("Missing source points"))
    val frictionMeta = inputFriction.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + inputFriction.getClass.getName))

    zoomLevel = frictionZoom match {
    case Some(zoom) =>
      if (frictionMeta.getMaxZoomLevel < zoom) {
        throw new IOException("The image has a maximum zoom level of " + frictionMeta.getMaxZoomLevel)
      }
      zoom
    case _ => frictionMeta.getMaxZoomLevel
    }

    val outputBounds = {
      requestedBounds match {
      case None =>
        if (maxCost < 0) {
          frictionMeta.getBounds
        }
        else {
          // Find the number of tiles beyond the MBR that need to be included to
          // cover the maxCost. Pixel values are in meters/sec and the maxCost is
          // in seconds. We use a worst-case scenario here to ensure that we include
          // at least as many tiles, and we accomplish that by using the minimum
          // pixel value.
          //
          // NOTE: Currently, we only work with a single band - in other words cost
          // distance is computed using a single friction value regardless of the
          // direction of travel. If we ever modify the algorithm to support different
          // friction values per direction, then this computation must change as well.
          val stats = frictionMeta.getImageStats(zoomLevel, 0)
          if (stats == null) {
            throw new IOException(s"No stats for ${frictionMeta.getPyramid}." +
                " You must either build the pyramid for it or specify the bounds for the cost distance.")
          }

          CostDistanceMapOp.LOG.info("Calculating tile bounds for maxCost " + maxCost + ", min = " + stats.min + " and bounds " + frictionMeta.getBounds)
          if (stats.min == Double.MaxValue) {
            throw new IllegalArgumentException("Invalid stats for the friction surface: " + frictionMeta.getPyramid + ". You will need to explicitly include the bounds in the CostDistance call")
          }
          calculateBoundsFromCost(maxCost, sourcePointsRDD, stats.min, frictionMeta.getBounds)
        }
      case Some(rb) => rb
      }
    }

    log.debug("outputBounds = " + outputBounds)
    val tilesize = frictionMeta.getTilesize
    val frictionRDD = inputFriction.rdd(zoomLevel) getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputFriction.getClass.getName))

    tileBounds = {
      val requestedTMSBounds = TMSUtils.Bounds.convertOldToNewBounds(outputBounds)
      TMSUtils.boundsToTile(requestedTMSBounds, zoomLevel, tilesize)
    }
    if (tileBounds == null)
    {
      throw new IllegalArgumentException("No tile bounds for " + frictionMeta.getPyramid + " at zoom level " + zoomLevel)
    }

    log.info("tileBounds = " + tileBounds)
    val width: Short = tilesize.toShort
    val height: Short = tilesize.toShort
    val res = TMSUtils.resolution(zoomLevel, tilesize)
    val outputNodata = Float.NaN

    // Create a hash map lookup where the key is the tile id and the value is
    // the set of pixels in the tile that are source points.
    val starts = new scala.collection.mutable.HashMap[Long, scala.collection.mutable.Set[TMSUtils.Pixel]]
      with scala.collection.mutable.MultiMap[Long, TMSUtils.Pixel]
    val startTilesAndPoints = sourcePointsRDD.map(startGeom => {
      if (startGeom == null || startGeom._2 == null || !startGeom._2.isInstanceOf[Point]) {
        throw new IOException("Invalid starting point, expected a point geometry: " + startGeom)
      }
      val startPt = startGeom._2.asInstanceOf[Point]
      val tile: TMSUtils.Tile = TMSUtils.latLonToTile(startPt.getY.toFloat, startPt.getX.toFloat, zoomLevel,
        tilesize)
      val startTileId = TMSUtils.tileid(tile.tx, tile.ty, zoomLevel)
      val startPixel = TMSUtils.latLonToTilePixelUL(startPt.getY.toFloat, startPt.getX.toFloat, tile.tx, tile.ty,
        zoomLevel, tilesize)
      (startTileId, startPixel)
    })
    startTilesAndPoints.collect().foreach(entry => {
      starts.addBinding(entry._1, entry._2)
    })

    // Limit processing to the tile bounds
    var resultsrdd = frictionRDD.filter(U => {
      // Trim the friction surface to only the tiles in our bounds
      val t = TMSUtils.tileid(U._1.get, zoomLevel)
      tileBounds.contains(TMSUtils.tileid(U._1.get, zoomLevel))
    })
    // After some experimentation with different values for repartitioning, the
    // best performance seems to come from using the number of executors for this
    // job.
    resultsrdd = if (numExecutors > 1) {
      CostDistanceMapOp.LOG.info("Repartitioning to " + numExecutors + " partitions")
      resultsrdd.repartition(numExecutors)
    }
    else {
      CostDistanceMapOp.LOG.info("No need to repartition")
      resultsrdd
    }
    // Build a list of changed points to start the cost distance algorithm. It must
    // contain all of the source points provided by the vector input.
    var changesToProcess = new NeighborChangedPoints
    var initialChangesAccum = context.accumulator(new NeighborChangedPoints)(NeighborChangesAccumulator)
    resultsrdd = resultsrdd.map(U => {
      val sourceRaster: Raster = RasterWritable.toRaster(U._2)
      val startTileId = U._1.get()
      if (starts.contains(startTileId)) {
        val pointsInTile = starts.getOrElse(U._1.get(), default = scala.collection.mutable.Set[TMSUtils.Pixel]())
        val costPoints = new ListBuffer[CostPoint]()
        for (startPixel <- pointsInTile) {
          val startPixelFriction = sourceRaster.getSampleFloat(startPixel.px.toInt, startPixel.py.toInt, 0)
          CostDistanceMapOp.LOG.info("Initially putting tile " + startTileId + " " + TMSUtils.tileid(startTileId, zoomLevel) + " and point " + startPixel.px + ", " + startPixel.py + " in the list")
          costPoints += new CostPoint(startPixel.px.toShort, startPixel.py.toShort, 0.0f, startPixelFriction)
        }
        val ncp = new NeighborChangedPoints
        ncp.addPoints(startTileId, CostDistanceMapOp.SELF, costPoints.toList)
        initialChangesAccum.add(ncp)
      }
      (U._1,
        RasterWritable.toWritable(makeCostDistanceRaster(U._1.get, sourceRaster, zoomLevel, res,
          width, height, outputNodata)))
    })
    changesToProcess = initialChangesAccum.value
    resultsrdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Force the RDD to materialize
    val rddCount = resultsrdd.count()
    CostDistanceMapOp.LOG.info("RDD count " + rddCount)

    val pixelSizeMeters: Double = res * LatLng.METERS_PER_DEGREE

    // Process changes until there aren't any more
    var counter: Long = 0
    do {
      // Use an accumulator to capture changes for the neighbor tiles as we process.
      var changesAccum = context.accumulator(new NeighborChangedPoints)(NeighborChangesAccumulator)
      val previousrdd = resultsrdd
      var mapAccum = context.accumulator(0, "MapCount")
      var processedAccum = context.accumulator(0, "ProcessTileCount")
      resultsrdd = previousrdd.map(U => {
        mapAccum += 1
        val tileid = U._1.get
        val tileChanges = changesToProcess.get(tileid)
        if (tileChanges != null) {
          processedAccum += 1
//          changesToProcess.dump(zoomLevel)
          // This tile has changes to process. Update the pixel values within the
          // tile accordingly while accumulating changes to this tile's neighbors.
          val writableRaster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(U._2))
          val changedPoints = processTile(U._1.get(), zoomLevel, res, pixelSizeMeters, writableRaster,
            tileChanges, changesAccum)
          (U._1, RasterWritable.toWritable(writableRaster))
        }
        else {
          (U._1, U._2)
        }
      })
      resultsrdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
//      if (counter % 5 == 0) {
//        resultsrdd.checkpoint()
//      }
      // Force the resultsrdd to materialize
      resultsrdd.count()
      previousrdd.unpersist()
      changesToProcess = changesAccum.value
//      CostDistanceMapOp.LOG.error("Changes after iteration " + counter)
////      changesToProcess.dump(zoomLevel)
//      changesToProcess.dumpTotalChanges()
      counter += 1
    } while(changesToProcess.size() > 0)

    rasterRDD = Some(RasterRDD(resultsrdd.map(U => {
      val sourceRaster = RasterWritable.toRaster(U._2)
      // Need to convert our raster to a single band raster for output.
      val model = new BandedSampleModel(DataBuffer.TYPE_FLOAT, sourceRaster.getWidth,
        sourceRaster.getHeight, 1)
      val singleBandRaster = Raster.createWritableRaster(model, null)
      val totalCostBand = sourceRaster.getNumBands - 1
      for (x <- 0 until sourceRaster.getWidth) {
        for (y <- 0 until sourceRaster.getHeight) {
          val s: Float = sourceRaster.getSampleFloat(x, y, totalCostBand)
          singleBandRaster.setSample(x, y, 0, s)
        }
      }
      val t = TMSUtils.tileid(U._1.get, zoomLevel)
      (new TileIdWritable(U._1), RasterWritable.toWritable(singleBandRaster))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoomLevel, outputNodata, calcStats = false))
    true
  }

  /**
   * Given one or more source points, compute a Bounds surrounding those points that
   * extends out to maxCost above, below, and the left and right of the MBR of those
   * points. This will encompass the region in which the cost distance algorithm
   * will run.
   *
   * The maxCost is in seconds, and the minPixelValue is in meters/second. This method
   * first computes the distance in meters, and then determines how many pixels are
   * required to cover that distance, both vertically and horizontally. It then adds
   * that number of pixels to the top, bottom, left and right of the MBR of the source
   * points to find the pixel bounds. Finally, it converts those pixels to
   * lat/lng in order to construct bounds from those coordinates.
   *
   * The returned Bounds will not extend beyond world bounds.
   *
   * @param maxCost How far the cost distance algorithm should go in computing distances.
   *                Measured in seconds.
   * @param sourcePointsRDD One or more points from which the cost distance algorithm will
   *                     compute minimum distances to all other pixels.
   * @param minPixelValue The smallest value assigned to a pixel in the friction surface
   *                      being used for the cost distance. Measure in seconds/meter.
   * @return
   */
  def calculateBoundsFromCost(maxCost: Double, sourcePointsRDD:VectorRDD,
      minPixelValue: Double, imageBounds: Bounds): Bounds =
  {
    // Locate the MBR of all the source points
    val bounds = SparkVectorUtils.calculateBounds(sourcePointsRDD)
    val distanceInMeters = maxCost / minPixelValue

    // Since we want the distance along the 45 deg diagonal (equal distance above and
    // to the right) of the top right corner of the source points MBR, we can compute
    // the point at a 45 degree bearing from that corner, and use pythagorean for the
    // diagonal distance to use.
    val diagonalDistanceInMeters = Math.sqrt(2) * distanceInMeters
    // Find the coordinates of the point that is distance meters to right and distance
    // meters above the top right corner of the sources points MBR.
    val tr = new LatLng(bounds.getMaxY, bounds.getMaxX)
    val trExpanded = LatLng.calculateCartesianDestinationPoint(tr, diagonalDistanceInMeters, 45.0)

    // Find the coordinates of the point that is distance meters to left and distance
    // meters below the bottom left corner of the sources points MBR.
    val bl = new LatLng(bounds.getMinY, bounds.getMinX)
    val blExpanded = LatLng.calculateCartesianDestinationPoint(bl, diagonalDistanceInMeters, 225.0)

    // Make sure the returned bounds does not extend beyond the image bounds itself.
    new Bounds(Math.max(blExpanded.getLng, imageBounds.getMinX),
      Math.max(blExpanded.getLat, imageBounds.getMinY),
      Math.min(trExpanded.getLng, imageBounds.getMaxX),
      Math.min(trExpanded.getLat, imageBounds.getMaxY))
  }

  def makeCostDistanceRaster(tileId: Long, source: Raster,
      zoom: Int,
      res: Double,
      width: Short,
      height: Short,
      nodata: Float): WritableRaster = {
    val sourceBands = source.getNumBands
    val numBands = sourceBands + 1
    val model = new BandedSampleModel(DataBuffer.TYPE_FLOAT, width, height, numBands)
    var bandedRaster = Raster.createWritableRaster(model, null)

    val sourceValue: Array[Float] = new Array[Float](sourceBands)
    val newValue: Array[Float] = new Array[Float](numBands)
    for (py <- 0 until source.getHeight) {
      for (px <- 0 until source.getWidth) {
        // When the source friction surface contains a single band, we compute
        // the friction values for each of the eight directions from that single
        // band value. As a result, all the diagonal directions will have the
        // same cost, all the vertical directions will have the same cost, and
        // all the horizontal directions will have the same cost.
        source.getPixel(px, py, sourceValue)
        for (b <- 0 until sourceBands) {
          newValue(b) = sourceValue(b)
        }
        newValue(numBands - 1) = nodata
        bandedRaster.setPixel(px, py, newValue)
      }
    }
//    RasterWritable.toWritable(bandedRaster)
    bandedRaster
  }

  def calculateSourceTile(destTileId: Long, zoom:Int, direction: Byte): Long = {
    val tile = TMSUtils.tileid(destTileId, zoom)
    val srcTile = direction match {
//      case CostDistanceMapOp.SELF => tile
      case TraversalDirection.UP => new TMSUtils.Tile(tile.tx, tile.ty - 1)
      case TraversalDirection.UP_LEFT => new TMSUtils.Tile(tile.tx + 1, tile.ty - 1)
      case TraversalDirection.UP_RIGHT => new TMSUtils.Tile(tile.tx - 1, tile.ty - 1)
      case TraversalDirection.DOWN => new TMSUtils.Tile(tile.tx, tile.ty + 1)
      case TraversalDirection.DOWN_LEFT => new TMSUtils.Tile(tile.tx + 1, tile.ty + 1)
      case TraversalDirection.DOWN_RIGHT => new TMSUtils.Tile(tile.tx - 1, tile.ty + 1)
      case TraversalDirection.LEFT => new TMSUtils.Tile(tile.tx + 1, tile.ty)
      case TraversalDirection.RIGHT => new TMSUtils.Tile(tile.tx - 1, tile.ty)
    }
    TMSUtils.tileid(srcTile.tx, srcTile.ty, zoom)
  }

  // Check to see if the destination pixel is valid. If so, then compute the
  // cost to that pixel from the source point (using the cost stored in the specified
  // bandIndex). If the cost is smaller than the current total cost for that pixel,
  def changeDestPixel(destTileId: Long, zoom: Int, res: Double,
                      pixelSizeMeters: Double, srcPoint: CostPoint,
                      destRaster: WritableRaster, pxDest: Short, pyDest: Short,
                      direction: Byte, totalCostBandIndex: Short,
                      width: Int,
                      height: Int,
                      topValues: Array[Float],
                      bottomValues: Array[Float],
                      leftValues: Array[Float],
                      rightValues: Array[Float],
                      queue: java.util.concurrent.PriorityBlockingQueue[CostPoint]): Unit =
  {
    val currDestTotalCost = destRaster.getSampleFloat(pxDest, pyDest, totalCostBandIndex) // destBuf(numBands * (py * width + px) + totalCostBandIndex)
    val srcTileId = calculateSourceTile(destTileId, zoom, direction)
    val srcCost = getPixelCost(direction, pixelSizeMeters, srcPoint.pixelFriction)
    val destCost = getPixelCost(direction, pixelSizeMeters,
      destRaster.getSampleFloat(pxDest, pyDest, 0))
    // Compute the cost to travel from the center of the source pixel to
    // the center of the destination pixel (the sum of half the cost of
    // each pixel).
    val newTotalCost = srcPoint.totalCost + srcCost * 0.5f + destCost * 0.5f
    if (isValueSmaller(newTotalCost, currDestTotalCost)) {
//      destRaster.setSample(pxDest, pyDest, totalCostBandIndex, newTotalCost)
      if (pxDest == 0) {
        leftValues(pyDest) = newTotalCost
      }
      else if (pxDest == width - 1) {
        rightValues(pyDest) = newTotalCost
      }
      if (pyDest == 0) {
        topValues(pxDest) = newTotalCost
      }
      if (pyDest == height - 1) {
        bottomValues(pyDest) = newTotalCost
      }
      queue.add(new CostPoint(pxDest, pyDest, newTotalCost,
        destRaster.getSampleFloat(pxDest, pyDest, 0)))
    }
  }

  /**
   * Apply the neighbor changes to the outer ring of pixels in the destination raster.
   * This method updates the pixels in the destination raster and adds the modified
   * pixels to the queue which is used for processing the tile later.
   *
   * @param queue
   * @param tileId
   * @param raster
   * @param width
   * @param height
   * @param res
   * @param pixelSizeMeters
   * @param neighborChanges
   */
  def propagateNeighborChangesToOuterPixels(queue: java.util.concurrent.PriorityBlockingQueue[CostPoint],
                                            tileId: Long,
                                            raster: WritableRaster,
                                            width: Int,
                                            height: Int,
                                            res: Double,
                                            pixelSizeMeters: Double,
                                            topValues: Array[Float],
                                            bottomValues: Array[Float],
                                            leftValues: Array[Float],
                                            rightValues: Array[Float],
                                            neighborChanges: Array[List[CostPoint]]): Unit =
  {
    val numBands: Short = raster.getNumBands.toShort
    val costBand: Short = (numBands - 1).toShort // 0-based index of the last band is the cost band

    neighborChanges.view.zipWithIndex foreach { case (cpList, direction) => {
      if (cpList != null) {
        for (srcPoint <- cpList) {
          direction match {
            case CostDistanceMapOp.SELF => {
              // This direction is only used at the start of the algorithm for source
              // points inside of tiles. Just add these changed points to the queue.
              queue.add(srcPoint)
            }
            case CostDistanceMapOp.ABOVE => {
              // The destination tile is above the source tile. If any changed pixels in
              // the source tile are in the top row of the tile, then compute the changes
              // that would propagate to that pixel's neighbors in the bottom row of the
              // destination tile and send messages whenever the total cost lowers for any
              // of those pixels.
              if (srcPoint.py == 0) {
                for (n <- neighborsAbove) {
                  val pxNeighbor: Short = (srcPoint.px + n._1).toShort
                  if (pxNeighbor >= 0 && pxNeighbor < width) {
                    changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                      srcPoint, raster,
                      pxNeighbor, (height - 1).toShort, n._2, costBand,
                      width,
                      height,
                      topValues,
                      bottomValues,
                      leftValues,
                      rightValues,
                      queue)
                  }
                }
              }
            }
            case CostDistanceMapOp.BELOW => {
              // The destination tile is below the source tile. For any pixels that changed
              // in the source tile, propagate those changes to the neighboring pixels in the
              // top row of the destination tile.
              if (srcPoint.py == height - 1) {
                for (n <- neighborsBelow) {
                  val pxNeighbor: Short = (srcPoint.px + n._1).toShort
                  if (pxNeighbor >= 0 && pxNeighbor < width) {
                    changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                      srcPoint, raster,
                      pxNeighbor, 0, n._2, costBand,
                      width,
                      height,
                      topValues,
                      bottomValues,
                      leftValues,
                      rightValues,
                      queue)
                  }
                }
              }
            }
            case CostDistanceMapOp.LEFT => {
              // The destination tile is to the left of the source tile. For any pixels that changed
              // in the source tile, propagate those changes to the neighboring pixels in the
              // right-most column of the destination tile.
              if (srcPoint.px == 0) {
                for (n <- neighborsToLeft) {
                  val pyNeighbor: Short = (srcPoint.py + n._1).toShort
                  if (pyNeighbor >= 0 && pyNeighbor < height) {
                    changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                      srcPoint, raster,
                      (width - 1).toShort, pyNeighbor, n._2, costBand,
                      width,
                      height,
                      topValues,
                      bottomValues,
                      leftValues,
                      rightValues,
                      queue)
                  }
                }
              }
            }
            case CostDistanceMapOp.RIGHT => {
              // The destination tile is to the right of the source tile. For any pixels that changed
              // in the source tile, propagate those changes to the neighboring pixels in the
              // left-most column of the destination tile.
              if (srcPoint.px == width - 1) {
                for (n <- neighborsToRight) {
                  val pyNeighbor: Short = (srcPoint.py + n._1).toShort
                  if (pyNeighbor >= 0 && pyNeighbor < height) {
                    changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                      srcPoint, raster,
                      0.toShort, pyNeighbor, n._2, costBand,
                      width,
                      height,
                      topValues,
                      bottomValues,
                      leftValues,
                      rightValues,
                      queue)
                  }
                }
              }
            }
            case CostDistanceMapOp.ABOVE_LEFT => {
              // The destination tile is to the top-left of the source tile. If the top-left
              // pixel of the source tile changed, propagate that change to the bottom-right
              // pixel of the destination tile.
              if (srcPoint.px == 0 && srcPoint.py == 0) {
                changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                  srcPoint, raster,
                  (width - 1).toShort, (height - 1).toShort,
                  TraversalDirection.UP_LEFT, costBand,
                  width,
                  height,
                  topValues,
                  bottomValues,
                  leftValues,
                  rightValues,
                  queue)
              }
            }
            case CostDistanceMapOp.ABOVE_RIGHT => {
              // The destination tile is to the top-right of the source tile. If the top-right
              // pixel of the source tile changed, propagate that change to the bottom-left
              // pixel of the destination tile.
              if (srcPoint.px == width - 1 && srcPoint.py == 0) {
                changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                  srcPoint, raster,
                  0, (height - 1).toShort,
                  TraversalDirection.UP_RIGHT, costBand,
                  width,
                  height,
                  topValues,
                  bottomValues,
                  leftValues,
                  rightValues,
                  queue)
              }
            }
            case CostDistanceMapOp.BELOW_LEFT => {
              // The destination tile is to the bottom-left of the source tile. If the bottom-left
              // pixel of the source tile changed, propagate that change to the top-right
              // pixel of the destination tile.
              if (srcPoint.px == 0 && srcPoint.py == height - 1) {
                changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                  srcPoint, raster,
                  (width - 1).toShort, 0,
                  TraversalDirection.DOWN_LEFT, costBand,
                  width,
                  height,
                  topValues,
                  bottomValues,
                  leftValues,
                  rightValues,
                  queue)
              }
            }
            case CostDistanceMapOp.BELOW_RIGHT => {
              // The destination tile is to the bottom-right of the source tile. If the bottom-right
              // pixel of the source tile changed, propagate that change to the top-left
              // pixel of the destination tile.
              if (srcPoint.px == width - 1 && srcPoint.py == height - 1) {
                changeDestPixel(tileId, zoomLevel, res, pixelSizeMeters,
                  srcPoint, raster,
                  0, 0,
                  TraversalDirection.DOWN_RIGHT, costBand,
                  width,
                  height,
                  topValues,
                  bottomValues,
                  leftValues,
                  rightValues,
                  queue)
              }
            }
          }
        }
      }
    }
    }
  }

  def processTile(tileId: Long,
                  zoom: Int,
                  res: Double,
                  pixelSizeMeters: Double,
                  raster: WritableRaster,
                  changes: Array[List[CostPoint]],
                  changesAccum:Accumulator[NeighborChangedPoints]): Unit =
  {
    val neighborMetadata8Band = Array(
      (-1, -1, TraversalDirection.UP_LEFT)
      , (-1, 0, TraversalDirection.LEFT)
      , (-1, 1, TraversalDirection.DOWN_LEFT)
      , (1, -1, TraversalDirection.UP_RIGHT)
      , (1, 0, TraversalDirection.RIGHT)
      , (1, 1, TraversalDirection.DOWN_RIGHT)
      , (0, -1, TraversalDirection.UP)
      , (0, 1, TraversalDirection.DOWN)
    )
    val numBands = raster.getNumBands
    val totalCostBand = numBands - 1 // cost band is the last band in the raster
    val startTime = System.nanoTime()
    val width = raster.getWidth
    val height = raster.getHeight
    // Store the edge values in the raster before processing it so we can compare
    // after processing to see which edge pixels changed
    val origTopEdgeValues: Array[Float] = new Array[Float](width)
    val origBottomEdgeValues: Array[Float] = new Array[Float](width)
    val origLeftEdgeValues: Array[Float] = new Array[Float](height)
    val origRightEdgeValues: Array[Float] = new Array[Float](height)
    val preStart: Double = System.nanoTime()
    for (px <- 0 until width) {
      origTopEdgeValues(px) = raster.getSampleFloat(px, 0, totalCostBand)
      origBottomEdgeValues(px) = raster.getSampleFloat(px, height-1, totalCostBand)
    }
    for (py <- 0 until height) {
      origLeftEdgeValues(py) = raster.getSampleFloat(0, py, totalCostBand)
      origRightEdgeValues(py) = raster.getSampleFloat(width-1, py, totalCostBand)
    }
    // First determine the impact of the changed neighbor pixels on the
    // outer ring of pixels for this tile. The changed pixels for this
    // tile make up the queue of changed points that need to be processed
    // for this tile.
    val tile1 = TMSUtils.tileid(tileId, zoomLevel)
    var queue = new java.util.concurrent.PriorityBlockingQueue[CostPoint]()
    propagateNeighborChangesToOuterPixels(queue, tileId, raster, width, height, res, pixelSizeMeters,
      origTopEdgeValues, origBottomEdgeValues, origLeftEdgeValues, origRightEdgeValues, changes)

    // Now process each element from the priority queue until empty
    // For each element, check to see if the cost is smaller than the
    // cost in the band3 of the vertex. If it is, then compute the cost
    // to each of its neighbors, check the new cost to see if it's less
    // than the neighbor's current cost, and add a new entry to the queue
    // for the neighbor point. If a point around the perimeter of the tile
    // changes, then add an entry to local changedPoints.

    val preProcessingTime: Double = System.nanoTime() - preStart
    var totalEnqueue: Double = 0.0
    var totalDequeue: Double = 0.0
    var maxHeapSize: Int = 0
    var counter: Long = 0L

    // Set up variables used for calculating great circle distances of pixels
    val tile: TMSUtils.Tile = TMSUtils.tileid(tileId, zoom)

    // Process the queue of changed points until it is empty
    breakable {
      while (!queue.isEmpty) {
        //    while (queue.nonEmpty) {
        if (queue.size > maxHeapSize) {
          maxHeapSize = queue.size
        }
        counter += 1
        var t0 = System.nanoTime()
        val currPoint = queue.poll()

        totalDequeue = totalDequeue + (System.nanoTime() - t0)
        val currTotalCost = raster.getSampleFloat(currPoint.px, currPoint.py, totalCostBand)
        if ((maxCost <= 0.0 || currPoint.totalCost <= (maxCost + 1e-8)) && isValueSmaller(currPoint.totalCost, currTotalCost)) {
          raster.setSample(currPoint.px, currPoint.py, totalCostBand, currPoint.totalCost)
          // In the vertex data, set the point's cost to the new smaller value
          //        vertex.raster.setSample(currPoint.px, currPoint.py, totalCostBand, currPoint.cost)
          // Since this point has a new cost, check to see if the cost to each
          // of its neighbors is smaller than the current cost assigned to those
          // neighbors. If so, add those neighbor points to the queue.
          for (metadata <- neighborMetadata8Band) {
            val pxNeighbor: Short = (currPoint.px + metadata._1).toShort
            val pyNeighbor: Short = (currPoint.py + metadata._2).toShort
            if (pxNeighbor >= 0 && pxNeighbor < width && pyNeighbor >= 0 && pyNeighbor < height) {
              val currNeighborTotalCost = raster.getSampleFloat(pxNeighbor, pyNeighbor, totalCostBand)
              val direction = metadata._3
              // Compute the cost increase which is the sum of the distance from the
              // source pixel center point to the neighbor pixel center point.
              val sourcePixelCost = getPixelCost(direction, pixelSizeMeters,
                raster.getSampleFloat(currPoint.px, currPoint.py, 0))
              val neighborPixelCost = getPixelCost(direction, pixelSizeMeters,
                raster.getSampleFloat(pxNeighbor, pyNeighbor, 0))
              if (neighborPixelCost.isNaN) {
                // If the cost to traverse the neighbor is NaN (unreachable), and no
                // total cost has yet been assigned to the neighbor (PositiveInfinity),
                // then set the neighbor's total cost to NaN since it will never be
                // reachable.
                raster.setSample(pxNeighbor, pyNeighbor, totalCostBand, neighborPixelCost)
              }
              else {
                val costIncrease = sourcePixelCost * 0.5f + neighborPixelCost * 0.5f
                val newNeighborCost = currPoint.totalCost + costIncrease
                if (isValueSmaller(newNeighborCost, currNeighborTotalCost)) {
                  val neighborPoint = new CostPoint(pxNeighbor, pyNeighbor, newNeighborCost,
                    raster.getSampleFloat(pxNeighbor, pyNeighbor, 0))
                  t0 = System.nanoTime()
                  queue.add(neighborPoint)
                  totalEnqueue = totalEnqueue + (System.nanoTime() - t0)
                }
              }
            }
          }
        }
      }
    }

    val t0 = System.nanoTime()
    val edgePixelChanges = new NeighborChangedPoints
    // Find edge pixels that have changed so we know how to send messages
    val topChanges = new ListBuffer[CostPoint]()
    val bottomChanges = new ListBuffer[CostPoint]()
    for (px <- 0 until width) {
      if (tile.ty < tileBounds.n) {
        val currTopCost = raster.getSampleFloat(px, 0, totalCostBand)
        val currTopFriction = raster.getSampleFloat(px, 0, 0)
        val origTopValue = origTopEdgeValues(px)
        if (isValueSmaller(currTopCost, origTopValue)) {
          if (px == 0 && tile.tx > tileBounds.w) {
            val tlTileId = TMSUtils.tileid(tile.tx - 1, tile.ty + 1, zoomLevel)
            edgePixelChanges.addPoints(tlTileId, CostDistanceMapOp.ABOVE_LEFT,
              List[CostPoint](new CostPoint(px.toShort, 0, currTopCost, currTopFriction)))
          }
          if (px == width - 1 && tile.tx < tileBounds.e) {
            val trTileId = TMSUtils.tileid(tile.tx + 1, tile.ty + 1, zoomLevel)
            edgePixelChanges.addPoints(trTileId, CostDistanceMapOp.ABOVE_RIGHT,
              List[CostPoint](new CostPoint(px.toShort, 0, currTopCost, currTopFriction)))
          }
          topChanges += new CostPoint(px.toShort, 0, currTopCost, currTopFriction)
        }
      }
      if (tile.ty > tileBounds.s) {
        val currBottomCost = raster.getSampleFloat(px, height - 1, totalCostBand)
        val currBottomFriction = raster.getSampleFloat(px, height - 1, 0)
        val origBottomValue = origBottomEdgeValues(px)
        if (isValueSmaller(currBottomCost, origBottomValue)) {
          if (px == 0 && tile.tx > tileBounds.w) {
            val blTileId = TMSUtils.tileid(tile.tx - 1, tile.ty - 1, zoomLevel)
            edgePixelChanges.addPoints(blTileId, CostDistanceMapOp.BELOW_LEFT,
              List[CostPoint](new CostPoint(px.toShort, (height - 1).toShort,
                currBottomCost, currBottomFriction)))
          }
          if (px == width - 1 && tile.tx < tileBounds.e) {
            val brTileId = TMSUtils.tileid(tile.tx + 1, tile.ty - 1, zoomLevel)
            edgePixelChanges.addPoints(brTileId, CostDistanceMapOp.BELOW_RIGHT,
              List[CostPoint](new CostPoint(px.toShort, (height - 1).toShort,
                currBottomCost, currBottomFriction)))
          }
          bottomChanges += new CostPoint(px.toShort, (height - 1).toShort,
            currBottomCost, currBottomFriction)
        }
      }
    }
    if (topChanges.nonEmpty) {
      val aboveTile = TMSUtils.tileid(tile.tx, tile.ty + 1, zoomLevel)
      edgePixelChanges.addPoints(aboveTile, CostDistanceMapOp.ABOVE, topChanges.toList)
    }
    if (bottomChanges.nonEmpty) {
      val belowTile = TMSUtils.tileid(tile.tx, tile.ty - 1, zoomLevel)
      edgePixelChanges.addPoints(belowTile, CostDistanceMapOp.BELOW, bottomChanges.toList)
    }

    // Don't process corner pixels again (already handled as part of top/bottom
    val leftChanges = new ListBuffer[CostPoint]()
    val rightChanges = new ListBuffer[CostPoint]()
    for (py <- 0 until height) {
      if (tile.tx > tileBounds.w) {
        val currLeftCost = raster.getSampleFloat(0, py, totalCostBand)
        val currLeftFriction = raster.getSampleFloat(0, py, 0)
        val origLeftValue = origLeftEdgeValues(py)
        if (isValueSmaller(currLeftCost, origLeftValue)) {
          leftChanges += new CostPoint(0, py.toShort, currLeftCost, currLeftFriction)
        }
      }
      if (tile.tx < tileBounds.e) {
        val currRightCost = raster.getSampleFloat(width-1, py, totalCostBand)
        val currRightFriction = raster.getSampleFloat(width-1, py, 0)
        val origRightValue = origRightEdgeValues(py)
        if (isValueSmaller(currRightCost, origRightValue)) {
          rightChanges += new CostPoint((width - 1).toShort, py.toShort, currRightCost, currRightFriction)
        }
      }
    }
    if (leftChanges.nonEmpty) {
      val leftTile = TMSUtils.tileid(tile.tx - 1, tile.ty, zoomLevel)
      edgePixelChanges.addPoints(leftTile, CostDistanceMapOp.LEFT, leftChanges.toList)
    }
    if (rightChanges.nonEmpty) {
      val rightTile = TMSUtils.tileid(tile.tx + 1, tile.ty, zoomLevel)
      edgePixelChanges.addPoints(rightTile, CostDistanceMapOp.RIGHT, rightChanges.toList)
    }
//    CostDistanceMapOp.LOG.error("Changes being added to accumulator " + edgePixelChanges.size() + " for tile " + tile1)
    changesAccum.add(edgePixelChanges)

    val totalTime: Double = System.nanoTime() - startTime
    val postProcessingTime: Double = System.nanoTime() - t0
  }

  private def getPixelCost(direction: Byte, pixelSizeMeters: Double, pixelFriction: Double): Float =
  {
    val distanceMeters = direction match {
      case (TraversalDirection.DOWN | TraversalDirection.UP |
            TraversalDirection.LEFT | TraversalDirection.RIGHT) => {
        // Vertical direction
        pixelSizeMeters
      }
      case (TraversalDirection.DOWN_LEFT | TraversalDirection.DOWN_RIGHT |
            TraversalDirection.UP_LEFT | TraversalDirection.UP_RIGHT) => {
        Math.sqrt(2.0 * pixelSizeMeters * pixelSizeMeters)
      }
    }
    (pixelFriction * distanceMeters).toFloat
  }

  // Returns true if newValue < origValue. If origValue is NaN, return true
  // only if newValue is not NaN. If origValue is not NaN, and newValue is
  // NaN, then return false.
  def isValueSmaller(newValue: Float, origValue: Float): Boolean = {
    if (origValue.isNaN)
    {
      return !newValue.isNaN
    }
    if (newValue.isNaN) {
      return false
    }

    // Allow for floating point math inaccuracy
    newValue < (origValue - 1e-7)
  }


  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(zoomLevel)
    out.writeDouble(maxCost)
    out.writeUTF(tileBounds.toCommaString)
  }

  override def readExternal(in: ObjectInput): Unit = {
    zoomLevel = in.readInt()
    maxCost = in.readDouble()
    tileBounds = TMSUtils.TileBounds.fromCommaString(in.readUTF())
  }

  override def registerClasses(): Array[Class[_]] = {
    GeometryFactory.getClasses ++ Array[Class[_]](classOf[FeatureIdWritable], classOf[TMSUtils.Pixel])
  }
}

object TraversalDirection {
  val UP: Byte = CostDistanceMapOp.ABOVE
  val DOWN: Byte = CostDistanceMapOp.BELOW
  val LEFT: Byte = CostDistanceMapOp.LEFT
  val RIGHT: Byte = CostDistanceMapOp.RIGHT
  val DOWN_RIGHT: Byte = CostDistanceMapOp.BELOW_RIGHT
  val DOWN_LEFT: Byte = CostDistanceMapOp.BELOW_LEFT
  val UP_RIGHT: Byte = CostDistanceMapOp.ABOVE_RIGHT
  val UP_LEFT: Byte = CostDistanceMapOp.ABOVE_LEFT
}


//Stores information about points whose cost has changed during processing. CostPoint
//ordering should be in increasing order by cost so that the PriorityQueue processes
//minimum cost elements first.
class CostPoint(var px: Short, var py: Short, var totalCost: Float, var pixelFriction: Float
                 ) extends Ordered[CostPoint] with Externalizable {
  def this() = {
    this(-1, -1, 0.0f, 0.0f)
  }

  def compare(that: CostPoint): Int = {
    totalCost.compare(that.totalCost)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeShort(px)
    out.writeShort(py)
    out.writeFloat(totalCost)
    out.writeFloat(pixelFriction)
  }

  override def readExternal(in: ObjectInput): Unit = {
    px = in.readShort()
    py = in.readShort()
    totalCost = in.readFloat()
    pixelFriction = in.readFloat()
  }
}

// Stores points from a source tile that changed value and forced
// the target tile to be recomputed. The key in the hash map is the
// direction from the source tile to the target tile.
class NeighborChangedPoints extends Externalizable {
  // The collection of point changes is keyed on tile id. The value is a
  // a pair containing the direction and the list of changed points. The
  // direction is from the neighbor tile toward the tile in the key.
  private val changes = new util.HashMap[Long, Array[List[CostPoint]]]()

  def size(): Int = {
    return changes.size()
  }

  def dump(zoomLevel: Int): Unit = {
    val iter = changes.keySet().iterator()
    while (iter.hasNext) {
      val tileId = iter.next()
      println("  tile id: " + TMSUtils.tileid(tileId, zoomLevel))
      val value = changes.get(tileId)
      for (direction <- 0 until value.length) {
        val dir = direction.toByte match {
          case CostDistanceMapOp.ABOVE => "ABOVE"
          case CostDistanceMapOp.ABOVE_LEFT => "ABOVE_LEFT"
          case CostDistanceMapOp.ABOVE_RIGHT => "ABOVE_RIGHT"
          case CostDistanceMapOp.BELOW => "BELOW"
          case CostDistanceMapOp.BELOW_LEFT => "BELOW_LEFT"
          case CostDistanceMapOp.BELOW_RIGHT => "BELOW_RIGHT"
          case CostDistanceMapOp.LEFT => "LEFT"
          case CostDistanceMapOp.RIGHT => "RIGHT"
          case CostDistanceMapOp.SELF => "SELF"
          case _ => "UNKNOWN " + direction
        }
        print("    " + dir)
        if (value(direction) == null) {
          println(" has no changes")
        }
        else {
          println(" has " + value(direction).size + " changes")
        }
      }
    }
  }

  def dumpTotalChanges(): Unit = {
    var changeCount: Long = 0
    val iter = changes.keySet().iterator()
    while (iter.hasNext) {
      val tileId = iter.next()
      val value = changes.get(tileId)
      for (direction <- 0 until value.length) {
        if (value(direction) != null) {
          changeCount += value(direction).size
        }
      }
    }
    CostDistanceMapOp.LOG.error("Total changes: " + changeCount)
  }

  def put(tileId: Long, toDirection: Byte, changedPoints: List[CostPoint]): Unit = {
    var value = changes.get(tileId)
    if (value == null) {
      value = new Array[List[CostPoint]](CostDistanceMapOp.DIRECTION_COUNT)
      changes.put(tileId, value)
    }
    value(toDirection) = changedPoints
  }

  def addPoints(tileId: Long, direction: Byte, points: List[CostPoint]): Unit = {
    var value = get(tileId)
    if (value == null) {
      value = new Array[List[CostPoint]](CostDistanceMapOp.DIRECTION_COUNT)
      changes.put(tileId, value)
    }
    value(direction) = points
  }

  def keySet(): util.Set[Long] = {
    return changes.keySet()
  }

  def get(tileId: Long): Array[List[CostPoint]] = {
    return changes.get(tileId)
  }

  def +=(other: NeighborChangedPoints): NeighborChangedPoints = {
    val numOtherChanges = other.changes.size()
    if (numOtherChanges > 0) {
      val iter = other.changes.keySet().iterator()
      while (iter.hasNext) {
        val tileId = iter.next()
        val value = other.get(tileId)
        val myValue = get(tileId)
        if (myValue == null) {
          changes.put(tileId, value)
        }
        else {
          // We will never have multiple cost point lists for this tile from
          // the same direction (e.g. array index), so we can just perform
          // an assignment.
          value.view.zipWithIndex foreach { case (cpList, index) => {
            if (cpList != null) {
              myValue(index) = cpList
            }
          }
          }
        }
      }
    }
    this
  }

  override def readExternal(in: ObjectInput): Unit = {
    val tileCount = in.readInt()
    for (i <- 0 until tileCount) {
      val tileId = in.readLong()
      for(direction <- 0 until CostDistanceMapOp.DIRECTION_COUNT) {
        val hasChanges = in.readBoolean()
        if (hasChanges) {
          val cpCount = in.readInt()
          var cpList = ListBuffer[CostPoint]()
          cpList.sizeHint(cpCount)
          for (cpIndex <- 0 until cpCount) {
            val cp = new CostPoint()
            cp.readExternal(in)
            cpList += cp
          }
          addPoints(tileId, direction.toByte, cpList.toList)
        }
      }
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    val numChanges = changes.size()
    out.writeInt(numChanges)
    if (numChanges > 0) {
      val iter = changes.keySet().iterator()
      while (iter.hasNext) {
        val tileId = iter.next()
        out.writeLong(tileId)
        val value = changes.get(tileId)
        for (entry <- value) {
          out.writeBoolean(entry != null)
          if (entry != null) {
            out.writeInt(entry.length)
            entry.foreach(cp => {
              cp.writeExternal(out)
            })
          }
        }
      }
    }
  }
}

// An accumulator used within Spark to accumulate changes to all of the tiles
// processed during a single map pass of the cost distance algorithm.
object NeighborChangesAccumulator extends AccumulatorParam[NeighborChangedPoints]
{
  override def addInPlace(r1: NeighborChangedPoints,
                          r2: NeighborChangedPoints): NeighborChangedPoints = {
    r1 += r2
  }

  override def zero(initialValue: NeighborChangedPoints): NeighborChangedPoints = {
    new NeighborChangedPoints
  }
}
