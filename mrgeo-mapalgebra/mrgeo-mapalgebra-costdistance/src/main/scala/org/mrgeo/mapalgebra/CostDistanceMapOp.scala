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

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._

// Cost distance is computed using graph processing by constructing
// a graph in which each vertex is a copy of one tile of imagery from
// current cost for each pixel. Each of the 8 neighboring tiles are connected
// to any given tile, so there are a maximum of 8 neighbors to each tile.
// the source friction surface with one additional band that holds the
//
// The overview of the graph processing is:
// for each tile in which a source point resides {
//   compute new minimum costs from any of those source pixels to each
//     pixel in that tile
//   for each of the neighboring tiles {
//     if any pixels on the edge next to the neighbor tile got a smaller cost {
//       send a message to the neighbor tile with the pixels that changed
//     }
//   }
// }
// while tile messages remain {
//   using the edge pixel changes from the source tile in the message, propagate those
//     pixel cost changes throughout this tile to compute min cost for each pixel
//   for each neighboring tile {
//     if any pixels on the edge next to the neighbor tile got a smaller cost {
//       send a message to the neighbor tile with the pixels that changed
//     }
//   }
// }

object CostDistanceMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("costDistance", "cd")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CostDistanceMapOp(node, variables)
}


class CostDistanceMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None

  // TODO:  Change to VectorMapOp when implemented
  //var sourcePoints:Option[MapOp] = None
  var friction:Option[RasterMapOp] = None
  var frictionZoom:Option[Int] = None
  var requestedBounds:Option[Bounds] = None

  var maxCost:Double = -1
  var zoomLevel:Int = -1

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    val usage: String = "CostDistance takes the following arguments " +
        "(source point, [friction zoom level], friction raster, [maxCost], [minX, minY, maxX, maxY])"

    val numChildren = node.getNumChildren
    if (numChildren < 2 || numChildren > 8) {
      throw new ParserException(usage)
    }

    var nodeIndex: Int = 0
    // Add the source point
    //sourcePoint = node.getChild(nodeIndex)
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
    conf.set("spark.storage.memoryFraction", "0.2") // set the storage amount lower...
    conf.set("spark.shuffle.memoryFraction", "0.30") // set the shuffle higher
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {
    val t0 = System.nanoTime()

    val inputFriction:RasterMapOp = friction getOrElse(throw new IOException("Input MapOp not valid!"))

    val frictionMeta = inputFriction.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + inputFriction.getClass.getName))
    val frictionRDD = inputFriction.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputFriction.getClass.getName))

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

          // TODO:  Uncomment when VectorMapOp is implemented, remove bounds.world
//          sourcePoints match {
//          case Some(sp) => calculateBoundsFromCost(maxCost, sp.getPoints, stats.min)
//          case _ => throw new IOException("Source points missing")
//          }
          Bounds.world
        }
      case Some(rb) => rb
      }
    }

    val tilesize = frictionMeta.getTilesize

    val tileBounds: org.mrgeo.utils.LongRectangle = {
      val requestedTMSBounds = TMSUtils.Bounds.convertOldToNewBounds(outputBounds)
      TMSUtils.boundsToTile(requestedTMSBounds, zoomLevel, tilesize).toLongRectangle
    }
    if (tileBounds == null)
    {
      throw new IllegalArgumentException("No tile bounds for " + frictionMeta.getPyramid + " at zoom level " + zoomLevel)
    }

    // TODO: Uncomment when VectorMapOp is implemented, remove hardcoded startPt
    //val startPt = sourcePoints(0)
    val startPt = (0, 0)
    val width: Short = tilesize.toShort
    val height: Short = tilesize.toShort
    val res = TMSUtils.resolution(zoomLevel, tilesize)
    val nodata = frictionMeta.getDefaultValuesFloat
    val tile: TMSUtils.Tile = TMSUtils.latLonToTile(startPt._2, startPt._1, zoomLevel,
      tilesize)
    val startTileId = TMSUtils.tileid(tile.tx, tile.ty, zoomLevel)
    val startPixel = TMSUtils.latLonToTilePixelUL(startPt._2, startPt._1, tile.tx, tile.ty,
      zoomLevel, tilesize)

    val realVertices = frictionRDD.map(U => {
      val sourceRaster: Raster = RasterWritable.toRaster(U._2)
      val raster: WritableRaster = makeCostDistanceRaster1(U._1.get, sourceRaster,
        zoomLevel, res, width, height, nodata)
      // are no changed points.
      val vt: VertexType = new VertexType(raster, new ChangedPoints(false), U._1.get, zoomLevel)
      (U._1.get(), vt)
    })

    val edges =
      frictionRDD.flatMap{ case (tileId, value) =>
        val tile = TMSUtils.tileid(tileId.get, zoomLevel)
        getEdges(tileBounds, tile.tx, tile.ty, zoomLevel)
      }.map{ case (edge) =>  Edge(edge.fromTileId, edge.toTileId, edge.direction)
      }

    val graph: Graph[VertexType, Byte] = Graph(realVertices/*.repartition(3)*/, edges/*.repartition(3)*/,
      defaultVertexAttr = null.asInstanceOf[VertexType],
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)

    val sssp = runGraph(graph, startTileId, zoomLevel, res,
      startPixel.px.toShort, startPixel.py.toShort, width, height)

    // Be sure to filter out any graph vertices that are null. This occurs when
    // the friction surface has missing tiles. The graph still contains a vertex
    // but its value is null.
    rasterRDD = Some(RasterRDD(sssp.vertices.filter(U => {
        (U._2 != null)
    }).map(U => {
      // Need to convert our raster to a single band raster for output.
      val model = new BandedSampleModel(DataBuffer.TYPE_FLOAT, width, height, 1)
      val singleBandRaster = Raster.createWritableRaster(model, null)
      var infinityCount = 0
      var nanCount = 0
      var min: Float = Float.PositiveInfinity
      var max: Float = Float.NegativeInfinity
      val totalCostBand = U._2.raster.getNumBands - 1
      for (x <- 0 until width) {
        for (y <- 0 until height) {
          val s: Float = U._2.raster.getSampleFloat(x, y, totalCostBand)
          if (s == Float.PositiveInfinity) {
            infinityCount += 1
          }
          if (s.isNaN) {
            nanCount += 1
          }
          else {
            min = Math.min(s, min)
            max = Math.max(s, max)
          }
          singleBandRaster.setSample(x, y, 0, s)
        }
      }
      (new TileIdWritable(U._1), RasterWritable.toWritable(singleBandRaster))
    })))


    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoomLevel, frictionMeta.getDefaultValue(0)))

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
   * @param sourcePoints One or more points from which the cost distance algorithm will
   *                     compute minimum distances to all other pixels.
   * @param minPixelValue The smallest value assigned to a pixel in the friction surface
   *                      being used for the cost distance. Measure in seconds/meter.
   * @return
   */
  def calculateBoundsFromCost(maxCost: Double, sourcePoints:Seq[(Float, Float)],
      minPixelValue: Double): Bounds =
  {
    var minX = Float.MaxValue
    var maxX = Float.MinValue
    var minY = Float.MaxValue
    var maxY = Float.MinValue

    // Locate the MBR of all the source points
    for (pt <- sourcePoints) {
      minX = math.min(pt._1, minX)
      maxX = math.max(pt._1, maxX)
      minY = math.min(pt._2, minY)
      maxY = math.max(pt._2, maxY)
    }
    val distanceInMeters = maxCost / minPixelValue

    // Since we want the distance along the 45 deg diagonal (equal distance above and
    // to the right) of the top right corner of the source points MBR, we can compute
    // the point at a 45 degree bearing from that corner, and use pythagorean for the
    // diagonal distance to use.
    val diagonalDistanceInMeters = Math.sqrt(2) * distanceInMeters
    // Find the coordinates of the point that is distance meters to right and distance
    // meters above the top right corner of the sources points MBR.
    val tr = new LatLng(maxY, maxX)
    val trExpanded = LatLng.calculateCartesianDestinationPoint(tr, diagonalDistanceInMeters, 45.0)

    // Find the coordinates of the point that is distance meters to left and distance
    // meters below the bottom left corner of the sources points MBR.
    val bl = new LatLng(minY, minX)
    val blExpanded = LatLng.calculateCartesianDestinationPoint(bl, diagonalDistanceInMeters, 225.0)

    new Bounds(blExpanded.getLng, blExpanded.getLat, trExpanded.getLng, trExpanded.getLat)
  }

  def getEdges(tileBounds: LongRectangle, tx: Long, ty: Long, zoom: Int): List[CostDistanceEdge] = {

    val edges = new ArrayBuffer[CostDistanceEdge]()
    val dmin: Long = -1
    val dmax: Long = 1
    for(dy <- dmin to dmax) {
      for (dx <- dmin to dmax) {

        if(dx != 0 || dy != 0) {
          //				if(excludedOffsets(dx,dy))
          //					continue;
          val neighborTx: Long = tx + dx
          val neighborTy: Long = ty + dy

          if (neighborTx >= tileBounds.getMinX && neighborTx <= tileBounds.getMaxX &&
              neighborTy >= tileBounds.getMinY && neighborTy <= tileBounds.getMaxY) {

            val direction = (dx, dy) match
            {
            case (-1,  1) => CostDistanceEdgeType.TO_TOP_LEFT
            case ( 0,  1) => CostDistanceEdgeType.TO_TOP
            case ( 1,  1) => CostDistanceEdgeType.TO_TOP_RIGHT
            case ( 1,  0) => CostDistanceEdgeType.TO_RIGHT
            case ( 1, -1) => CostDistanceEdgeType.TO_BOTTOM_RIGHT
            case ( 0, -1) => CostDistanceEdgeType.TO_BOTTOM
            case (-1, -1) => CostDistanceEdgeType.TO_BOTTOM_LEFT
            case (-1,  0) => CostDistanceEdgeType.TO_LEFT
            }
            edges.append(new CostDistanceEdge(TMSUtils.tileid(tx, ty, zoom),
              TMSUtils.tileid(neighborTx, neighborTy, zoom),
              direction))
          }
        }
      }
    }
    edges.toList
  }

  def makeCostDistanceRaster1(tileId: Long, source: Raster,
      zoom: Int,
      res: Double,
      width: Short,
      height: Short,
      nodata: Array[Float]): WritableRaster = {
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
        newValue(numBands - 1) = Float.NaN
        bandedRaster.setPixel(px, py, newValue)
      }
    }
    bandedRaster
  }

  // Check to see if the destination pixel is valid. If so, then compute the
  // cost to that pixel from the source point (using the cost stored in the specified
  // bandIndex). If the cost is smaller than the current total cost for that pixel,
  def addChanges(srcTileId: Long, destTileId: Long, zoom: Int, res: Double, pixelHeightM: Double,
      srcPoint: CostPoint, srcRaster: Raster, destRaster: Raster, pxDest: Short,
      pyDest: Short, direction: Byte, totalCostBandIndex: Short,
      changedPoints: ChangedPoints): Unit = {
    val currDestTotalCost = destRaster.getSampleFloat(pxDest, pyDest, totalCostBandIndex) // destBuf(numBands * (py * width + px) + totalCostBandIndex)
    val srcCost = getPixelCost(srcTileId, zoom, srcPoint.px, srcPoint.py, direction,
        res, pixelHeightM, srcRaster)
    val destCost = getPixelCost(destTileId, zoom, pxDest, pyDest, direction,
      res, pixelHeightM, destRaster)
    // Compute the cost to travel from the center of the source pixel to
    // the center of the destination pixel (the sum of half the cost of
    // each pixel).
    val newTotalCost = srcPoint.cost + srcCost * 0.5f + destCost * 0.5f
    if (isValueSmaller(newTotalCost, currDestTotalCost)) {
      changedPoints.addPoint(new CostPoint(pxDest, pyDest, newTotalCost))
    }
  }

  def runGraph(graph: Graph[VertexType, Byte], startTileId: Long, zoom: Int, res: Double,
      pxStart: Short, pyStart: Short, width: Short, height: Short): Graph[VertexType, Byte] = {
    //    testPregel[ChangedPoints](graph, null, Int.MaxValue, EdgeDirection.Either)(
    val neighborsAbove = Array((-1, DirectionBand.UP_LEFT_BAND),
      (0, DirectionBand.UP_BAND), (1, DirectionBand.UP_RIGHT_BAND))
    val neighborsBelow = Array((-1, DirectionBand.DOWN_LEFT_BAND),
      (0, DirectionBand.DOWN_BAND), (1, DirectionBand.DOWN_RIGHT_BAND))
    val neighborsToLeft = Array((-1, DirectionBand.UP_LEFT_BAND),
      (0, DirectionBand.LEFT_BAND), (1, DirectionBand.DOWN_LEFT_BAND))
    val neighborsToRight = Array((-1, DirectionBand.UP_RIGHT_BAND),
      (0, DirectionBand.RIGHT_BAND), (1, DirectionBand.DOWN_RIGHT_BAND))

    val o: LatLng = new LatLng(0, 0)
    val n: LatLng = new LatLng(res, 0)
    val pixelHeightM: Double = LatLng.calculateGreatCircleDistance(o, n)

    val initialMsg = new ChangedPoints(true)
    initialMsg.addPoint(new CostPoint(pxStart, pyStart, 0.0f))
    CostDistancePregel.run[VertexType, Byte, ChangedPoints](graph, initialMsg,
      Int.MaxValue, EdgeDirection.Out)(
          // Vertex Program
          (id, vertexData, msg) => {
            val t0 = System.nanoTime()
            log.debug("IN VPROG, vertex id is " + TMSUtils.tileid(id, zoomLevel) + " and start id is " + TMSUtils.tileid(startTileId, zoomLevel) + " and msg is " + msg)
            var returnNewVertex = true
            var changedPoints: ChangedPoints = null
            log.debug("  initialMsg = " + msg.isInitial)
            if (!msg.isInitial) {
              if (msg.size > 0) {
                changedPoints = processVertices(id, zoom, res, pixelHeightM, vertexData.raster, msg)
                log.debug("  processVertices on non-initial message returned " + changedPoints.size)
              }
              else {
                log.debug("  no changes to process for non-inital message")
                changedPoints = new ChangedPoints(false)
              }
            }
            else if (id == startTileId) {
              log.debug("  for start tile")
              changedPoints = processVertices(id, zoom, res, pixelHeightM, vertexData.raster, msg)
            }
            else {
              log.debug("  ignoring tile because it is the initial msg, and this is not the start tile")
              changedPoints = new ChangedPoints(false)
              returnNewVertex = false
            }
            log.debug("  returning " + changedPoints.size + " changed points")
            if (returnNewVertex) {
              new VertexType(vertexData.raster, changedPoints, id, zoomLevel)
            }
            else {
              vertexData
            }
          },
          // sendMsg
          triplet => {
            log.debug("SENDMSG src " + TMSUtils.tileid(triplet.srcId, zoomLevel) + " and dest " +
                TMSUtils.tileid(triplet.dstId, zoomLevel) + " contains " + triplet.srcAttr.changedPoints.size + " changed points")
            val t0 = System.nanoTime()
            // The changed points are from the source vertex. Now we need to compute the
            // cost changes in the destination vertex pixels that neighbor each of those
            // changed points. The edge direction indicates the position of the source
            // vertex/tile relative to the destination vertex/tile, so we use that relationship
            // to determine the neighboring pixels in the destination to check.
            var newChanges: ChangedPoints = null
            if (triplet.srcAttr.changedPoints != null && triplet.dstAttr != null) {
              log.debug("IN SENDMSG for src id " + triplet.srcId + " there are " + triplet.srcAttr.changedPoints.size + " changes ")
              if (!triplet.srcAttr.changedPoints.isEmpty) {
                newChanges = new ChangedPoints(false)
                val changedPoints = triplet.srcAttr.changedPoints.getAllPoints
                val numBands: Short = triplet.dstAttr.raster.getNumBands.toShort
                val costBand: Short = (numBands - 1).toShort // 0-based index of the last band is the cost band
                for (srcPoint <- changedPoints) {
                  if (triplet.attr == CostDistanceEdgeType.TO_TOP) {
                    // The destination tile is above the source tile. If any changed pixels in
                    // the source tile are in the top row of the tile, then compute the changes
                    // that would propagate to that pixel's neighbors in the bottom row of the
                    // destination tile and send messages whenever the total cost lowers for any
                    // of those pixels.
                    if (srcPoint.py == 0) {
                      for (n <- neighborsAbove) {
                        val pxNeighbor: Short = (srcPoint.px + n._1).toShort
                        if (pxNeighbor >= 0 && pxNeighbor < width) {
                          addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                            srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                            pxNeighbor, (height - 1).toShort, n._2, costBand, newChanges)
                        }
                      }
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_BOTTOM) {
                    // The destination tile is below the source tile. For any pixels that changed
                    // in the source tile, propagate those changes to the neighboring pixels in the
                    // top row of the destination tile.
                    if (srcPoint.py == height - 1) {
                      for (n <- neighborsBelow) {
                        val pxNeighbor: Short = (srcPoint.px + n._1).toShort
                        if (pxNeighbor >= 0 && pxNeighbor < width) {
                          addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                            srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                            pxNeighbor, 0, n._2, costBand, newChanges)
                        }
                      }
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_LEFT) {
                    // The destination tile is to the left of the source tile. For any pixels that changed
                    // in the source tile, propagate those changes to the neighboring pixels in the
                    // right-most column of the destination tile.
                    if (srcPoint.px == 0) {
                      for (n <- neighborsToLeft) {
                        val pyNeighbor: Short = (srcPoint.py + n._1).toShort
                        if (pyNeighbor >= 0 && pyNeighbor < height) {
                          addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                            srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                            (width - 1).toShort, pyNeighbor, n._2, costBand, newChanges)
                        }
                      }
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_RIGHT) {
                    // The destination tile is to the right of the source tile. For any pixels that changed
                    // in the source tile, propagate those changes to the neighboring pixels in the
                    // left-most column of the destination tile.
                    if (srcPoint.px == width - 1) {
                      for (n <- neighborsToRight) {
                        val pyNeighbor: Short = (srcPoint.py + n._1).toShort
                        if (pyNeighbor >= 0 && pyNeighbor < height) {
                          addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                            srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                            0.toShort, pyNeighbor, n._2, costBand, newChanges)
                        }
                      }
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_TOP_LEFT) {
                    // The destination tile is to the top-left of the source tile. If the top-left
                    // pixel of the source tile changed, propagate that change to the bottom-right
                    // pixel of the destination tile.
                    if (srcPoint.px == 0 && srcPoint.py == 0) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        (width - 1).toShort, (height - 1).toShort,
                        DirectionBand.UP_LEFT_BAND, costBand, newChanges)
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_TOP_RIGHT) {
                    // The destination tile is to the top-right of the source tile. If the top-right
                    // pixel of the source tile changed, propagate that change to the bottom-left
                    // pixel of the destination tile.
                    if (srcPoint.px == width - 1 && srcPoint.py == 0) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        0, (height - 1).toShort,
                        DirectionBand.UP_RIGHT_BAND, costBand, newChanges)
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_BOTTOM_LEFT) {
                    // The destination tile is to the bottom-left of the source tile. If the bottom-left
                    // pixel of the source tile changed, propagate that change to the top-right
                    // pixel of the destination tile.
                    if (srcPoint.px == 0 && srcPoint.py == height - 1) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        (width - 1).toShort, 0,
                        DirectionBand.DOWN_LEFT_BAND, costBand, newChanges)
                    }
                  }
                  else if (triplet.attr == CostDistanceEdgeType.TO_BOTTOM_RIGHT) {
                    // The destination tile is to the bottom-right of the source tile. If the bottom-right
                    // pixel of the source tile changed, propagate that change to the top-left
                    // pixel of the destination tile.
                    if (srcPoint.px == width - 1 && srcPoint.py == height - 1) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        0, 0,
                        DirectionBand.DOWN_RIGHT_BAND, costBand, newChanges)
                    }
                  }
                }
              }
            }
            if (newChanges == null) {
              log.debug("  SENDMSG returning empty from no changes")
              Iterator.empty
            } else {
              if (newChanges.size > 0) {
                log.debug("  SENDMSG returning " + newChanges.size + " changes for destination")
                Iterator((triplet.dstId, newChanges))
              }
              else {
                log.debug("  SENDMSG returning empty after 0 changes")
                Iterator.empty
              }
            }
          },
          // mergeMsg
          // TODO: Merge a and b into a single ChangedPoints object
          (a, b) => {
            val t0 = System.nanoTime()
            var merged = new ChangedPoints(false)
            val aPoints: List[CostPoint] = a.getAllPoints
            for (p <- aPoints) {
              merged.addPoint(p)
            }
            val bPoints: List[CostPoint] = b.getAllPoints
            for (p <- bPoints) {
              merged.addPoint(p)
            }
            merged
          }
        )
  }

  def processVertices(tileId: Long, zoom: Int, res: Double, pixelHeightM: Double,
      raster: WritableRaster, changes: ChangedPoints): ChangedPoints =
  {
    log.debug("PROCESS VERTICES received " + changes.size + " changes")
    val neighborMetadata8Band = Array(
      (-1, -1, DirectionBand.UP_LEFT_BAND)
      , (-1, 0, DirectionBand.LEFT_BAND)
      , (-1, 1, DirectionBand.DOWN_LEFT_BAND)
      , (1, -1, DirectionBand.UP_RIGHT_BAND)
      , (1, 0, DirectionBand.RIGHT_BAND)
      , (1, 1, DirectionBand.DOWN_RIGHT_BAND)
      , (0, -1, DirectionBand.UP_BAND)
      , (0, 1, DirectionBand.DOWN_BAND)
    )
    val numBands = raster.getNumBands
    val totalCostBand = numBands - 1 // cost band is the last band in the raster
  val startTime = System.nanoTime()
    val newChanges: ChangedPoints = new ChangedPoints(false)
    var queue = new java.util.concurrent.PriorityBlockingQueue[CostPoint]()
    for (cp <- changes.getAllPoints) {
      queue.add(cp)
    }
    // Now process each element from the priority queue until empty
    // For each element, check to see if the cost is smaller than the
    // cost in the band3 of the vertex. If it is, then compute the cost
    // to each of its neighbors, check the new cost to see if it's less
    // than the neighbor's current cost, and add a new entry to the queue
    // for the neighbor point. If a point around the perimeter of the tile
    // changes, then add an entry to local changedPoints.

    // Get FloatBuffer from vertex so we can manipulate it directly
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
    for (cp <- changes.getAllPoints) {
      if (cp.px == 0) {
        origLeftEdgeValues(cp.py) = cp.cost
      }
      if (cp.px == width - 1) {
        origRightEdgeValues(cp.py) = cp.cost
      }
      if (cp.py == 0) {
        origTopEdgeValues(cp.px) = cp.cost
      }
      if (cp.py == height - 1) {
        origBottomEdgeValues(cp.px) = cp.cost
      }
    }
    val preProcessingTime: Double = System.nanoTime() - preStart
    var totalEnqueue: Double = 0.0
    var totalDequeue: Double = 0.0
    var maxHeapSize: Int = 0
    var counter: Long = 0L

    // Set up variables used for calculating great circle distances of pixels
    val tile: TMSUtils.Tile = TMSUtils.tileid(tileId, zoom)

    val o: LatLng = new LatLng(0, 0)
    val n: LatLng = new LatLng(res, 0)
    val pixelHeightM: Double = LatLng.calculateGreatCircleDistance(o, n)

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
        if ((maxCost <= 0.0 || currPoint.cost <= (maxCost + 1e-8)) && isValueSmaller(currPoint.cost, currTotalCost)) {
          raster.setSample(currPoint.px, currPoint.py, totalCostBand, currPoint.cost)
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
              val directionBand: Short = metadata._3.toShort
              // Compute the cost increase which is the sum of the distance from the
              // source pixel center point to the neighbor pixel center point.
              val sourcePixelCost = getPixelCost(tileId, zoomLevel, currPoint.px, currPoint.py,
                metadata._3, res, pixelHeightM, raster)
              val neighborPixelCost = getPixelCost(tileId, zoomLevel, pxNeighbor, pyNeighbor,
                metadata._3, res, pixelHeightM, raster)
              if (neighborPixelCost.isNaN) {
                // If the cost to traverse the neighbor is NaN (unreachable), and no
                // total cost has yet been assigned to the neighbor (PositiveInfinity),
                // then set the neighbor's total cost to NaN since it will never be
                // reachable.
                raster.setSample(pxNeighbor, pyNeighbor, totalCostBand, neighborPixelCost)
              }
              else {
                val costIncrease = sourcePixelCost * 0.5f + neighborPixelCost * 0.5f
                val newNeighborCost = currPoint.cost + costIncrease
                if (isValueSmaller(newNeighborCost, currNeighborTotalCost)) {
                  val neighborPoint = new CostPoint(pxNeighbor, pyNeighbor, newNeighborCost)
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
    // Find edge pixels that have changed so we know how to send messages
    for (px <- 0 until width) {
      val currTopCost: Float = raster.getSampleFloat(px, 0, totalCostBand)
      val origTopValue = origTopEdgeValues(px)
      if (isValueSmaller(currTopCost, origTopValue)) {
        newChanges.addPoint(new CostPoint(px.toShort, 0, currTopCost))
      }
      val currBottomCost = raster.getSampleFloat(px, height-1, totalCostBand)
      val origBottomValue = origBottomEdgeValues(px)
      if (isValueSmaller(currBottomCost, origBottomValue)) {
        newChanges.addPoint(new CostPoint(px.toShort, (height-1).toShort, currBottomCost))
      }
    }
    // Don't process corner pixels again (already handled as part of top/bottom
    for (py <- 1 until height-1) {
      val currLeftCost: Float = raster.getSampleFloat(0, py, totalCostBand)
      val origLeftValue = origLeftEdgeValues(py)
      if (isValueSmaller(currLeftCost, origLeftValue)) {
        newChanges.addPoint(new CostPoint(0, py.toShort, currLeftCost))
      }
      val currRightCost = raster.getSampleFloat(width-1, py, totalCostBand)
      val origRightValue = origRightEdgeValues(py)
      if (isValueSmaller(currRightCost, origRightValue)) {
        newChanges.addPoint(new CostPoint((width-1).toShort, py.toShort, currRightCost))
      }
    }
    val totalTime: Double = System.nanoTime() - startTime
    val postProcessingTime: Double = System.nanoTime() - t0
    // After all the points have been processed, assign the local changedPoints
    // list to the vertex change points so it is available in the sendMsg
    // method later.
    if (newChanges.isEmpty)
    {
      log.debug("PROCESS VERTICES for " + TMSUtils.tileid(tileId, zoom) + " newChanges is empty")
    }
    else {
      log.debug("PROCESS VERTICES for " + TMSUtils.tileid(tileId, zoom) + " newChanges contains " + newChanges.size)
    }
    newChanges
  }

  // This method should only be called if the raster has one cost band. This method
  // computes the horizontal, vertical or diagonal cost to traverse this pixel based
  // of the direction passed in and the lat/lon anchor of the tile. It uses
  // great circle distance for the computation.
  private def getPixelCost(tileId: Long, zoom: Int, px: Int, py: Int, direction: Byte,
      res: Double, pixelHeightM: Double, raster: Raster): Float =
  {
    val tile: TMSUtils.Tile = TMSUtils.tileid(tileId, zoom)
    val startPx: Double = tile.tx * raster.getWidth
    // since Rasters have their UL as 0,0, just tile.ty * tileSize does not work
    val startPy: Double = (tile.ty * raster.getHeight) + raster.getHeight - 1

    val lonStart: Double = startPx * res - 180.0
    val latStart: Double = startPy * res - 90.0

    val lonNext: Double = lonStart + res
    val latNext: Double = latStart + res
    val distanceMeters =
      if (direction == DirectionBand.DOWN_BAND || direction == DirectionBand.UP_BAND) {
        // Vertical direction
        val o: LatLng = new LatLng(latStart, lonStart)
        val n: LatLng = new LatLng(latNext, lonStart)
        LatLng.calculateGreatCircleDistance(o, n)
      }
      else if (direction == DirectionBand.LEFT_BAND || direction == DirectionBand.RIGHT_BAND)
      {
        // Horizontal direction
        val o: LatLng = new LatLng(latStart, lonStart)
        val n: LatLng = new LatLng(latStart, lonNext)
        LatLng.calculateGreatCircleDistance(o, n)
      }
      else {
        // Diagonal direction
        val o: LatLng = new LatLng(latStart, lonStart)
        val n: LatLng = new LatLng(latNext, lonNext)
        LatLng.calculateGreatCircleDistance(o, n)
      }
    val pixelValue = raster.getSampleFloat(px, py, 0)
    (pixelValue * distanceMeters).toFloat
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
  }

  override def readExternal(in: ObjectInput): Unit = {
    zoomLevel = in.readInt()
    maxCost = in.readDouble()
  }
}



// This code was copied from the Spark source code and modified to work
// for a very large graph like a cost distance graph. THe only change
// was replacing calls to cache() with calls to persist() to make use
// of memory and disk (rather than expecting that the entire graph can
// be cached in memory). And it includes some new persist() calls.
object CostDistancePregel {

  /**
   * Execute a Pregel-like iterative vertex-parallel abstraction.  The
   * user-defined vertex-program `vprog` is executed in parallel on
   * each vertex receiving any inbound messages and computing a new
   * value for the vertex.  The `sendMsg` function is then invoked on
   * all out-edges and is used to compute an optional message to the
   * destination vertex. The `mergeMsg` function is a commutative
   * associative function used to combine messages destined to the
   * same vertex.
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param graph the input graph.
   *
   * @param initialMsg the message each vertex will receive at the on
   * the first iteration
   *
   * @param maxIterations the maximum number of iterations to run for
   *
   * @param activeDirection the direction of edges incident to a vertex that received a message in
   * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
   * out-edges of vertices that received a message in the previous round will run. The default is
   * `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
   * in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
   * *both* vertices received a message.
   *
   * @param vprog the user-defined vertex program which runs on each
   * vertex and receives the inbound message and computes a new vertex
   * value.  On the first iteration the vertex program is invoked on
   * all vertices and is passed the default message.  On subsequent
   * iterations the vertex program is only invoked on those vertices
   * that receive messages.
   *
   * @param sendMsg a user supplied function that is applied to out
   * edges of vertices that received messages in the current
   * iteration
   *
   * @param mergeMsg a user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type
   * A.  ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def run[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
      (vprog: (VertexId, VD, A) => VD,
          sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
          mergeMsg: (A, A) => A)
  : Graph[VD, ED] =
  {
    val storageLevel = graph.vertices.getStorageLevel
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).persist(storageLevel) //.cache()
  // compute the messages
  var messages = g.mapReduceTriplets(sendMsg, mergeMsg).persist(storageLevel)
    var activeMessages = messages.count()
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).persist(storageLevel) //.cache()
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.persist(storageLevel) //.cache()

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).persist(storageLevel) //.cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.edges.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      // count the iteration
      i += 1
    }

    g
  } // end of apply

} // end of class Pregel

object CostDistanceEdgeType {
  val TO_TOP_LEFT: Byte = 1
  val TO_TOP: Byte = 2
  val TO_TOP_RIGHT: Byte = 3
  val TO_RIGHT: Byte = 4
  val TO_BOTTOM_RIGHT: Byte = 5
  val TO_BOTTOM: Byte = 6
  val TO_BOTTOM_LEFT: Byte = 7
  val TO_LEFT: Byte = 8
}

object DirectionBand {
  def UP_BAND: Byte = 0
  def DOWN_BAND: Byte = 1
  def LEFT_BAND: Byte = 2
  def RIGHT_BAND: Byte = 3
  def DOWN_RIGHT_BAND: Byte = 4
  def DOWN_LEFT_BAND: Byte = 5
  def UP_RIGHT_BAND: Byte = 6
  def UP_LEFT_BAND: Byte = 7
}


class CostDistanceEdge(val fromTileId: Long, val toTileId: Long, val direction: Byte) {
}

//Stores information about points whose cost has changed during processing. CostPoint
//ordering should be in increasing order by cost so that the PriorityQueue processes
//minimum cost elements first.
class CostPoint(var px: Short, var py: Short, var cost: Float
    ) extends Ordered[CostPoint] with Externalizable {
  def this() = {
    this(-1, -1, 0.0f)
  }

  def compare(that: CostPoint): Int = {
    cost.compare(that.cost)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeShort(px)
    out.writeShort(py)
    out.writeFloat(cost)
  }

  override def readExternal(in: ObjectInput): Unit = {
    px = in.readShort()
    py = in.readShort()
    cost = in.readFloat()
  }
}

// Stores a list of points around the edges of a tile that changed while costs
// are computed for a tile.
class ChangedPoints(var initial: Boolean) extends Externalizable {
  private val changes = scala.collection.mutable.HashMap.empty[(Short, Short), CostPoint]

  // This constructor is only used for deserialization, and the correct
  // value for the data members overwrites the default used here as
  // the deserializer executes
  def this() = {
    this(true)
  }

  // Adds a changed pixel. If the pixel already exists, it only adds the passed
  // in point if its cost is less than the point already stored.
  def addPoint(point: CostPoint): Unit = {
    val pixel = (point.px, point.py)
    if (changes contains pixel) {
      val existingPoint = changes(pixel)
      if (existingPoint.cost > point.cost) {
        changes(pixel) = point
      }
    }
    else {
      changes(pixel) = point
    }
  }

  // Returns the list of changed points for the specfied edge or null if there
  // are no changed points for that edge.
  def getAllPoints: List[CostPoint] = {
    changes.values.toList
  }

  def isInitial: Boolean = {
    initial
  }

  def isEmpty: Boolean = {
    changes.isEmpty
  }

  def size: Int = {
    changes.size
  }

  def clear(): Unit = {
    changes.clear()
  }

  def get(px: Short, py: Short): Option[CostPoint] = {
    val pixel = (px, py)
    changes.get(pixel)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeBoolean(initial)
    out.writeInt(changes.size)
    val iter = changes.iterator
    while (iter.hasNext) {
      val cp = iter.next()
      cp._2.writeExternal(out)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    initial = in.readBoolean()
    val length = in.readInt
    for (i <- 0 until length) {
      var cp = new CostPoint(-1, -1, 0.0f)
      cp.readExternal(in)
      changes.put((cp.px, cp.py), cp)
    }
  }
}

//@SerialVersionUID(-7588980448693010399L)
// TODO: Remove tileid and zoom from VertexType after debugging!!!
class VertexType(var raster: WritableRaster,
    var changedPoints: ChangedPoints,
    var tileid: Long,
    var zoom: Int)
    extends Externalizable {

  def this() {
    this(null, null, -1, -1)
  }

  @throws(classOf[IOException])
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(tileid)
    out.writeInt(zoom)
    out.writeBoolean(changedPoints.isInitial)
    changedPoints.writeExternal(out)
    val rasterBytes: Array[Byte] = RasterWritable.toBytes(raster, null)
    out.writeInt(rasterBytes.length)
    out.write(rasterBytes)
  }

  @throws(classOf[IOException])
  override def readExternal(in: ObjectInput): Unit = {
    tileid = in.readLong()
    zoom = in.readInt()
    val initial = in.readBoolean()
    changedPoints = new ChangedPoints()
    changedPoints.readExternal(in)
    val byteCount: Int = in.readInt()
    val rasterBytes: Array[Byte] = new Array[Byte](byteCount)
    var offset: Int = 0
    in.readFully(rasterBytes, offset, byteCount)
    raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(rasterBytes, null))
  }
}
