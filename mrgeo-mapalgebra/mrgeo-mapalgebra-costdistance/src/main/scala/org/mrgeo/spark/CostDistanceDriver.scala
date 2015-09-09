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

package org.mrgeo.spark

import java.awt.image.{BandedSampleModel, Raster, DataBuffer, WritableRaster}
import java.io.{IOException, Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.{ProviderProperties, DataProviderFactory}
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._

// Cost distance is computed using graph processing by constructing
// a graph in which each vertex is a 9-band tile of imagery. The first
// 8 bands represent the cost of traversing each pixel in each of
// 8 horizontal, vertical and diagonal directions. This allows for
// a different cost for example to traverse the pixel from top to
// bottom, than from bottom to top. The last band stores the currently
// minimum calculated cost to get from one of the cost distance source
// points to that pixel. Each of the 8 neighboring tiles are connected
// to any given tile, so there are a maximum of 8 neighbors to each tile.
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

object CostDistanceDriver extends MrGeoDriver with Externalizable {
  val FRICTION_SURFACE_ARG = "frictionSurface"
  val OUTPUT_ARG = "output"
  val ZOOM_LEVEL_ARG = "zoomLevel"
  val SOURCE_POINTS_ARG = "sourcePoints"
  val MAX_COST_ARG = "maxCost"

  def costDistance(frictionSurface: String, output:String, zoomLevel: Int, sourcePoints: String,
                   maxCost: Double, conf:Configuration): Unit = {

    val args =  mutable.Map[String, String]()

    val name = f"CostDistance ($frictionSurface%s, $output%s, $zoomLevel%d, $sourcePoints%s, $maxCost)"

    args += FRICTION_SURFACE_ARG -> frictionSurface
    args += OUTPUT_ARG -> output
    args += ZOOM_LEVEL_ARG -> ("" + zoomLevel)
    args += SOURCE_POINTS_ARG -> sourcePoints
    args += MAX_COST_ARG -> ("" + maxCost)

    run(name, classOf[CostDistanceDriver].getName, args.toMap, conf)
  }

  override def writeExternal(out: ObjectOutput): Unit = {}
  override def readExternal(in: ObjectInput): Unit = {}

  override def setup(job: JobArguments): Boolean = {
    true
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

class CostDistanceDriver extends MrGeoJob with Externalizable {
  var frictionSurface: String = null
  var output:String = null
  var zoomLevel: Int = -1
  val sourcePoints = new mutable.ListBuffer[(Float,Float)]
  var maxCost: Double = 0.0

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    // yuck!  need to register spark private classes
    classes += ClassTag(Class.forName("org.apache.spark.util.collection.CompactBuffer")).wrap.runtimeClass

    classes.result()
  }

  override def execute(context: SparkContext): Boolean = {
    @transient val t0 = System.nanoTime()
    // TODO: The following is hard-coded for ease in testing, but it should be
    // configured by the caller.
    //    val startTileId: VertexId = TMSUtils.tileid(291, 186, 9) // The ultimate source for greece
    //    val startTileId: VertexId = TMSUtils.tileid(2731, 1355, 12) // The ultimate source for small-humvee
    //    val startTileId: VertexId = TMSUtils.tileid(916, 204, 10) // The ultimate source for all-ones

    try {
      // TODO: providerProperties needs to be configured into the job
      @transient val providerProperties: ProviderProperties = null
      // TODO: protectionLevel needs to be configured into the job
      @transient val protectionLevel: String = null
      @transient val pyramidAndMetadata = SparkUtils.loadMrsPyramidAndMetadata(frictionSurface, zoomLevel, context)
//      pyramidAndMetadata._1.foreach(U => {
//        println("Initial vertex: " + TMSUtils.tileid(U._1.get(), zoomLevel) + " -> " + U._2.hashCode())
//      })
      @transient val vertices = pyramidAndMetadata._1 // .repartition(35)
      @transient val metadata = pyramidAndMetadata._2
      //      val pxStart: Short = (metadata.getTilesize - 1).toShort // for small-humvee
      //      val pyStart: Short = (metadata.getTilesize - 1).toShort // for small-humvee
      //    val pxStart: Short = 3
      //    val pyStart: Short = 7
      val bounds = metadata.getBounds
      @transient val tileBounds: org.mrgeo.utils.LongRectangle = metadata.getTileBounds(zoomLevel)
      if (tileBounds == null)
      {
        throw new IllegalArgumentException("No tile bounds for " + frictionSurface + " at zoom level " + zoomLevel)
      }

      val width: Short = metadata.getTilesize.toShort
      val height: Short = metadata.getTilesize.toShort
      val res = TMSUtils.resolution(zoomLevel, metadata.getTilesize)
      val nodata: Array[Float] = metadata.getDefaultValuesFloat
      val tile: TMSUtils.Tile = TMSUtils.latLonToTile(sourcePoints(0)._2, sourcePoints(0)._1, zoomLevel,
        metadata.getTilesize)
      val startTileId = TMSUtils.tileid(tile.tx, tile.ty, zoomLevel)
      val startPixel = TMSUtils.latLonToTilePixelUL(sourcePoints(0)._2, sourcePoints(0)._1, tile.tx, tile.ty,
        zoomLevel, metadata.getTilesize)

      @transient val realVertices = vertices.map(U => {
        val sourceRaster: Raster = RasterWritable.toRaster(U._2)
        val raster: WritableRaster = makeCostDistanceRaster1(U._1.get, sourceRaster,
            zoomLevel, res, width, height, nodata)
        // are no changed points.
        val vt: VertexType = new VertexType(raster, new ChangedPoints(false), U._1.get, zoomLevel)
        (U._1.get(), vt)
      })
//      realVertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
//      realVertices.cache()
//      realVertices.count()
    //      @transient val edges: RDD[Edge[Byte]] = buildEdges(tileBounds, zoom, context)
      @transient val edges: RDD[Edge[Byte]] =
        vertices.flatMap{ case (tileId, value) =>
          val tile = TMSUtils.tileid(tileId.get, zoomLevel)
          getEdges(tileBounds, tile.tx, tile.ty, zoomLevel)
        }.map{ case (edge) =>  Edge(edge.fromTileId, edge.toTileId, edge.direction)
        }
//        for (e <- edges) {
//          println("Edge: " + e.srcId + " to " + e.dstId + ", " + e.attr)
//        }
      // For graph processing, each vertex is a RasterWritable in which band 0 contains the
      // cost of the pixel in the x direction, band 1 contains the cost of the pixel in the
      // y direction, and band 2 contains the current cost to the pixel from the source pixel.
      // Messages contain the set of pixels whose values are changing, including the pixel
      // row and column, and the new value.
//        class CostDistanceMessage(px: Int, py: Int, value: Float) {
//        }

      @transient val graph: Graph[VertexType, Byte] = Graph(realVertices/*.repartition(3)*/, edges/*.repartition(3)*/,
        defaultVertexAttr = null.asInstanceOf[VertexType],
        edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
        vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)

      @transient val sssp = runGraph(graph, startTileId, zoomLevel, res,
        startPixel.px.toShort, startPixel.py.toShort, width, height)
      @transient val vw3: RDD[(Long, VertexType)] = sssp.vertices.sortByKey(ascending = true)
      @transient val verticesWritable = vw3.map(U => {
        // Need to convert our raster to a single band raster for output.
        val model = new BandedSampleModel(DataBuffer.TYPE_FLOAT, width, height, 1)
        var singleBandRaster = Raster.createWritableRaster(model, null)
        var infinityCount = 0
        var nanCount = 0
        var min: Float = Float.PositiveInfinity
        var max: Float = Float.NegativeInfinity
        val totalCostBand = U._2.raster.getNumBands - 1
        for (x <- 0 until width) {
          for (y <- 0 until height) {
            @transient val s: Float = U._2.raster.getSampleFloat(x, y, totalCostBand)
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
//        println("Stats for tile " + TMSUtils.tileid(U._1, zoomLevel))
//        println("  infinity count: " + infinityCount)
//        println("  NaN count: " + nanCount)
//        println("  min: " + min)
//        println("  max: " + max)
        //          val singleBandRaster: Raster = U._2.raster.createChild(0, 0, width, height, 0, 0, Array[Int]{ 8 })
        (new TileIdWritable(U._1), RasterWritable.toWritable(singleBandRaster))
      })
//      verticesWritable.foreach(U => {
//        val loc = "/tmp/cdout/" + U._1 + ".tif"
//        val t = TMSUtils.tileid(U._1.get(), zoomLevel)
//        val r = RasterWritable.toRaster(U._2)
//        GeoTiffExporter.export(RasterUtils.makeBufferedImage(r),
//          TMSUtils.tileBounds(t.tx, t.ty, zoomLevel, metadata.getTilesize).convertNewToOldBounds(),
//          new java.io.File(loc))
////          GeotoolsRasterUtils.saveLocalGeotiff(loc, r, t.tx, t.ty, zoom, metadata.getTilesize, Double.NaN)
//      })
      val dp = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.WRITE, providerProperties)
//      val firstTile = verticesWritable.first()
//      val raster = RasterWritable.toRaster(firstTile._2)
//      SparkUtils.saveMrsPyramid(verticesWritable, dp, output, zoomLevel, raster.getWidth,
      SparkUtils.saveMrsPyramid(verticesWritable, dp, output, zoomLevel, width,
        Array[Double](Double.NaN), context.hadoopConfiguration, DataBuffer.TYPE_FLOAT,
        bounds, 1, protectionLevel, providerProperties)
      true
    }
    finally {
      println("execute took " + ((System.nanoTime() - t0).toDouble / 1000000) + " ms")
      if (context != null) {
        context.stop()
      }
    }
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

          /*
           * OLD TODO - currently there is a bug in our bounds calculation, which sets the
           * max tx/ty to 1 + true max. So our indices stop at tileMax.tx - 1
           *
           * The above message is no longer relevant but I'm keeping it there to be reminded
           * if we run into bounds calculation issues in the future - for now, I'm taking my
           * indices all the way to tileMax.tx and tileMax.ty
           */
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
//              case (badX, badY) =>  {
//                throw new IllegalStateException(String.format("Unexpected dx/dy in EdgeBuilder %d/%d", dx, dy))
//              }
            }
//            neighbors.add(new PositionEdge(position, neighborId))
            // TODO: The following condition allows only vertical and horizontal
            // directions. This is to see how much faster we run that way.
            if (dx == 0 || dy == 0) {
              edges.append(new CostDistanceEdge(TMSUtils.tileid(tx, ty, zoom),
                TMSUtils.tileid(neighborTx, neighborTy, zoom),
                direction))
            }
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
    @transient val sourceBands = source.getNumBands
    @transient val numBands = sourceBands + 1
    @transient val model = new BandedSampleModel(DataBuffer.TYPE_FLOAT, width, height, numBands)
    @transient var bandedRaster = Raster.createWritableRaster(model, null)

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

  // Create a cost raster for one tile. The output of this function will be a
  // 9-band raster in which the first 8 bands represent the cost to traverse
  // each pixel in 8 different directions. The DirectionBand constants define
  // which band corresponds to which direction. The 9th band stores the total
  // cost to arrive at the pixel from the nearest source point.
//  def makeCostDistanceRaster(tileId: Long, source: Raster,
//                             zoom: Int,
//                             res: Double,
//                             width: Short,
//                             height: Short,
//                             nodata: Array[Float]): WritableRaster = {
//    // The raster we use for processing contains one band for cost data in
//    // each of the eight directions within a pixel. See DirectionBand for
//    // which band maps to which direction. This allows for fine-grained
//    // friction surfaces to be provided by the user.
////    println("Running makeCostDistance on tile " + TMSUtils.tileid(tileId, zoom))
//    @transient val sourceBands = source.getNumBands
//    @transient val numBands = 9
//    @transient val model = new BandedSampleModel(DataBuffer.TYPE_FLOAT, width, height, numBands)
//    @transient var bandedRaster = Raster.createWritableRaster(model, null)
////          val bandedRaster: WritableRaster = Raster.createBandedRaster(DataBuffer.TYPE_FLOAT,
////            width, height, numBands, new Point(0, 0))
//
//    @transient val tile: TMSUtils.Tile = TMSUtils.tileid(tileId, zoom)
//
//    @transient val o: LatLng = new LatLng(0, 0)
//    @transient val n: LatLng = new LatLng(res, 0)
//    @transient val pixelHeightM: Double = LatLng.calculateGreatCircleDistance(o, n)
//
//    @transient val startPx: Double = tile.tx * width
//    // since Rasters have their UL as 0,0, just tile.ty * tileSize does not work
//    @transient val startPy: Double = (tile.ty * height) + height - 1
//
//    @transient val lonStart: Double = startPx * res - 180.0
//    @transient val latStart: Double = startPy * res - 90.0
//
//    @transient val lonNext: Double = lonStart + res
//    @transient val latNext: Double = latStart + res
//    o.setLat(latStart)
//    o.setLng(lonStart)
//    n.setLat(latNext)
//    n.setLng(lonNext)
//    @transient val v: Array[Float] = new Array[Float](numBands)
//    @transient val p: Array[Float] = new Array[Float](sourceBands)
//    @transient val horizontalBands: Set[Byte] = Set(DirectionBand.LEFT_BAND, DirectionBand.RIGHT_BAND)
//    @transient val verticalBands = Set(DirectionBand.UP_BAND, DirectionBand.DOWN_BAND)
//    @transient val diagonalBands = Set(DirectionBand.DOWN_LEFT_BAND, DirectionBand.DOWN_RIGHT_BAND,
//      DirectionBand.UP_LEFT_BAND, DirectionBand.UP_RIGHT_BAND)
//
//    for (py <- 0 until source.getHeight) {
//      // since Rasters have their UL as 0,0, but since startPy is based on 0,0 being LL,
//      // we have to do startPy - py instead of startPy + py
//      @transient val lat: Double = (startPy - py) * res - 90.0
//
//      o.setLat(lat)
//      n.setLat(lat)
//      @transient val pixelWidthM: Double = LatLng.calculateGreatCircleDistance(o, n)
//      @transient val pixelDiagonalM: Float = math.sqrt((pixelHeightM * pixelHeightM) +
//        (pixelWidthM * pixelWidthM)).toFloat
//      for (px <- 0 until source.getWidth) {
//        // When the source friction surface contains a single band, we compute
//        // the friction values for each of the eight directions from that single
//        // band value. As a result, all the diagonal directions will have the
//        // same cost, all the vertical directions will have the same cost, and
//        // all the horizontal directions will have the same cost.
//        if (sourceBands == 1) {
//          @transient val s: Float = source.getSampleFloat(px, py, 0)
//          if (s.isNaN) {
//            // If the friction surface contains a NaN value, then the cost surface
//            // should also be NaN since we can't calculate the cost at that point.
//            for (b <- 0 until numBands - 1) {
//              v(b) = Float.NaN
//            }
//          }
//          else {
//            if (!nodata(0).isNaN && s == nodata(0)) {
//              // The friction surface contains a nodata value, then the cost surface
//              // should also be NaN since we can't calculate the cost at that point.
//              for (b <- 0 until numBands - 1) {
//                v(b) = Float.NaN
//              }
//            } else {
//              if (s < 0.0) {
//                throw new RuntimeException("Invalid friction surface. Negative values not allowed.")
//              }
//              // The friction surface has a legitimate value for this pixel. The cost
//              // surface should be initialized to a value of 0 if this pixel is one
//              // of the starting pixels. Otherwise, it should be infinity initially
//              // since the graph processing will always choose the smallest cost for
//              // each pixel.
//              v(DirectionBand.LEFT_BAND) = (s * pixelWidthM).toFloat
//              v(DirectionBand.RIGHT_BAND) = (s * pixelWidthM).toFloat
//              v(DirectionBand.DOWN_BAND) = (s * pixelHeightM).toFloat
//              v(DirectionBand.UP_BAND) = (s * pixelHeightM).toFloat
//              v(DirectionBand.UP_LEFT_BAND) = s * pixelDiagonalM
//              v(DirectionBand.UP_RIGHT_BAND) = s * pixelDiagonalM
//              v(DirectionBand.DOWN_LEFT_BAND) = s * pixelDiagonalM
//              v(DirectionBand.DOWN_RIGHT_BAND) = s * pixelDiagonalM
//            }
//          }
//        } else {
//          // The user supplied an 8-band friction surface, so we compute the cost per
//          // pixel in each of the eight directions.
//          source.getPixel(px, py, p)
//          for (b <- 0 until sourceBands) {
//            if (p(b).isNaN) {
//              // If the friction surface contains a NaN value, then the cost surface
//              // should also be NaN since we can't calculate the cost at that point.
//              v(b) = Float.NaN
//            }
//            else {
//              if (!nodata(b).isNaN && p(b) == nodata(b)) {
//                // The friction surface contains a nodata value, then the cost surface
//                // should also be NaN since we can't calculate the cost at that point.
//                v(b) = Float.NaN
//              } else {
//                if (v(b) < 0.0) {
//                  throw new RuntimeException("Invalid friction surface. Negative values not allowed.")
//                }
//                if (diagonalBands.contains(b.toByte)) {
//                  v(b) = p(b) * pixelDiagonalM
//                } else if (horizontalBands.contains(b.toByte)) {
//                  v(b) = (p(b) * pixelWidthM).toFloat
//                } else {
//                  v(b) = (p(b) * pixelHeightM).toFloat
//                }
//              }
//            }
//          }
//        }
//        v(numBands - 1) = Float.NaN
//        bandedRaster.setPixel(px, py, v)
//      }
//    }
//    bandedRaster
//  }

  // Check to see if the destination pixel is valid. If so, then compute the
  // cost to that pixel from the source point (using the cost stored in the specified
  // bandIndex). If the cost is smaller than the current total cost for that pixel,
  def addChanges(srcTileId: Long, destTileId: Long, zoom: Int, res: Double, pixelHeightM: Double,
                 srcPoint: CostPoint, srcRaster: Raster, destRaster: Raster, pxDest: Short,
                 pyDest: Short, direction: Byte, totalCostBandIndex: Short,
                 changedPoints: ChangedPoints): Unit = {
    @transient val currDestTotalCost = destRaster.getSampleFloat(pxDest, pyDest, totalCostBandIndex) // destBuf(numBands * (py * width + px) + totalCostBandIndex)
    val srcCost = getPixelCost(srcTileId, zoom, srcPoint.px, srcPoint.py, direction,
        res, pixelHeightM, srcRaster)
    val destCost = getPixelCost(destTileId, zoom, pxDest, pyDest, direction,
      res, pixelHeightM, destRaster)
//    @transient val srcCost = srcRaster.getSampleFloat(srcPoint.px, srcPoint.py, costBandIndex) // srcBuf(numBands * (srcPoint.py * width + srcPoint.px) + costBandIndex)
//    @transient val destCost = destRaster.getSampleFloat(pxDest, pyDest, costBandIndex) // destBuf(numBands * (py * width + px) + costBandIndex)
    // Compute the cost to travel from the center of the source pixel to
    // the center of the destination pixel (the sum of half the cost of
    // each pixel).
    @transient val newTotalCost = srcPoint.cost + srcCost * 0.5f + destCost * 0.5f
//    if ((newTotalCost < currDestTotalCost) || (currDestTotalCost.isNaN && !newTotalCost.isNaN)) {
    if (isValueSmaller(newTotalCost, currDestTotalCost)) {
//      if (!currDestTotalCost.isNaN && !newTotalCost.isNaN) {
//        val diff = currDestTotalCost - newTotalCost
//        println("Found change diff = " + diff + " and percent change " + (diff / currDestTotalCost * 100.0))
//      }
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
//    val neighborsAbove = Array((0, DirectionBand.UP_BAND))
//    val neighborsBelow = Array((0, DirectionBand.DOWN_BAND))
//    val neighborsToLeft = Array((0, DirectionBand.LEFT_BAND))
//    val neighborsToRight = Array((0, DirectionBand.RIGHT_BAND))

    @transient val o: LatLng = new LatLng(0, 0)
    @transient val n: LatLng = new LatLng(res, 0)
    @transient val pixelHeightM: Double = LatLng.calculateGreatCircleDistance(o, n)

    @transient val initialMsg = new ChangedPoints(true)
    initialMsg.addPoint(new CostPoint(pxStart, pyStart, 0.0f))
    CostDistancePregel.run[VertexType, Byte, ChangedPoints](graph, initialMsg,
      Int.MaxValue, EdgeDirection.Out)(
//    graph.pregel[ChangedPoints](initialMsg, Int.MaxValue, EdgeDirection.Out)(
      // Vertex Program
      (id, vertexData, msg) => {
        @transient val t0 = System.nanoTime()
          log.warn("IN VPROG, vertex id is " + TMSUtils.tileid(id, zoomLevel) + " and start id is " + TMSUtils.tileid(startTileId, zoomLevel) + " and msg is " + msg)
//        if (msg != null) {
//          for (p <- msg.getAllPoints()) {
//            println("  Changed point: " + p.px + ", " + p.py + " = " + p.cost)
//          }
//        }
//        if (vertexData.changedPoints == null) {
//          vertexData.changedPoints = new ChangedPoints
//        }
//        else {
//          vertexData.changedPoints.clear()
//        }
//        val origChangedPoints = vertexData.changedPoints
        var returnNewVertex = true
        var changedPoints: ChangedPoints = null
//        vertexData.changedPoints = null
        log.warn("  initialMsg = " + msg.isInitial)
        if (!msg.isInitial) {
          if (msg.size > 0) {
//              println("  there are " + msg.size + " changed points in the message")
            changedPoints = processVertices(id, zoom, res, pixelHeightM, vertexData.raster, msg)
            log.warn("  processVertices on non-initial message returned " + changedPoints.size)
          }
          else {
            log.warn("  no changes to process for non-inital message")
            changedPoints = new ChangedPoints(false)
          }
        }
        else if (id == startTileId) {
          log.warn("  for start tile")
//            println("  processing start tile with initial message")
//          changedPoints = new ChangedPoints(false)
//          changedPoints.addPoint(new CostPoint(pxStart, pyStart, 0.0f))
//          changedPoints = new ChangedPoints(false)
//          for (cp <- msg.getAllPoints) {
//            changedPoints.addPoint(cp)
//          }
          changedPoints = processVertices(id, zoom, res, pixelHeightM, vertexData.raster, msg)
//          System.err.println("  returns " + changedPoints.size + " changed points")
        }
        else {
          log.warn("  ignoring tile because it is the initial msg, and this is not the start tile")
          changedPoints = new ChangedPoints(false)
          returnNewVertex = false
        }
//        if (changedPoints != null && changedPoints.size > 0) {
////          println("VPROG returning new vertex with " + changedPoints.size + " changes for " + TMSUtils.tileid(id, zoomLevel))
//          new VertexType(vertexData.raster, changedPoints, id, zoomLevel)
//        }
//        else {
//          if (origChangedPoints == null || origChangedPoints.size == 0) {
//            // No changes, so return the same vertex to prevent serialization
////            println("VPROG returning same vertex for " + TMSUtils.tileid(id, zoomLevel))
//            vertexData
//          }
//          else {
//            // There are no changed points this time around, but we have to return
//            // a new vertex because its list of changed points is now empty where
//            // before it was not.
////            println("VPROG returning new vertex without changes for " + TMSUtils.tileid(id, zoomLevel))
//            new VertexType(vertexData.raster, changedPoints, id, zoomLevel)
//          }
//        }
        log.warn("  returning " + changedPoints.size + " changed points")
        if (returnNewVertex) {
          new VertexType(vertexData.raster, changedPoints, id, zoomLevel)
        }
        else {
          vertexData
        }
      },
      // sendMsg
      triplet => {
          log.warn("SENDMSG src " + TMSUtils.tileid(triplet.srcId, zoomLevel) + " and dest " +
            TMSUtils.tileid(triplet.dstId, zoomLevel) + " contains " + triplet.srcAttr.changedPoints.size + " changed points")
        @transient val t0 = System.nanoTime()
        // The changed points are from the source vertex. Now we need to compute the
        // cost changes in the destination vertex pixels that neighbor each of those
        // changed points. The edge direction indicates the position of the source
        // vertex/tile relative to the destination vertex/tile, so we use that relationship
        // to determine the neighboring pixels in the destination to check.
        var newChanges: ChangedPoints = null
        if (triplet.srcAttr.changedPoints != null) {
          log.warn("IN SENDMSG for src id " + triplet.srcId + " there are " + triplet.srcAttr.changedPoints.size + " changes ")
          if (!triplet.srcAttr.changedPoints.isEmpty) {
            newChanges = new ChangedPoints(false)
            @transient val changedPoints = triplet.srcAttr.changedPoints.getAllPoints
//              val srcDataBuf: DataBufferFloat = triplet.srcAttr.raster.getDataBuffer.asInstanceOf[DataBufferFloat]
//              val srcBuf: Array[Float] = srcDataBuf.getData
//              val destDataBuf: DataBufferFloat = triplet.dstAttr.raster.getDataBuffer.asInstanceOf[DataBufferFloat]
//              val destBuf: Array[Float] = destDataBuf.getData
            @transient val numBands: Short = triplet.dstAttr.raster.getNumBands.toShort
            @transient val costBand: Short = (numBands - 1).toShort // 0-based index of the last band is the cost band
            for (srcPoint <- changedPoints) {
//              println("  Source changed point: " + srcPoint.px + ", " + srcPoint.py + " = " + srcPoint.cost)
              if (triplet.attr == CostDistanceEdgeType.TO_TOP) {
                // The destination tile is above the source tile. If any changed pixels in
                // the source tile are in the top row of the tile, then compute the changes
                // that would propagate to that pixel's neighbors in the bottom row of the
                // destination tile and send messages whenever the total cost lowers for any
                // of those pixels.
                if (srcPoint.py == 0) {
                  for (n <- neighborsAbove) {
                    @transient val pxNeighbor: Short = (srcPoint.px + n._1).toShort
                    if (pxNeighbor >= 0 && pxNeighbor < width) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        pxNeighbor, (height - 1).toShort, n._2, costBand, newChanges)
                    }
                  }
//                  for (px <- math.max(srcPoint.px - 1, width - 1) to math.min(srcPoint.px + 1, width - 1)) {
//                    addChanges(srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster, px.toShort, (height - 1).toShort,
//                      DirectionBand.UP_BAND, costBand, numBands, newChanges)
//                  }
                }
              }
              else if (triplet.attr == CostDistanceEdgeType.TO_BOTTOM) {
                // The destination tile is below the source tile. For any pixels that changed
                // in the source tile, propagate those changes to the neighboring pixels in the
                // top row of the destination tile.
                if (srcPoint.py == height - 1) {
                  for (n <- neighborsBelow) {
                    @transient val pxNeighbor: Short = (srcPoint.px + n._1).toShort
                    if (pxNeighbor >= 0 && pxNeighbor < width) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        pxNeighbor, 0, n._2, costBand, newChanges)
                    }
                  }
//                  for (px <- math.max(srcPoint.px - 1, 0) to math.min(srcPoint.px + 1, width - 1)) {
//                    addChanges(srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster, px.toShort, 0,
//                      DirectionBand.DOWN_BAND, costBand, numBands, newChanges)
//                  }
                }
              }
              else if (triplet.attr == CostDistanceEdgeType.TO_LEFT) {
                // The destination tile is to the left of the source tile. For any pixels that changed
                // in the source tile, propagate those changes to the neighboring pixels in the
                // right-most column of the destination tile.
                if (srcPoint.px == 0) {
                  for (n <- neighborsToLeft) {
                    @transient val pyNeighbor: Short = (srcPoint.py + n._1).toShort
                    if (pyNeighbor >= 0 && pyNeighbor < height) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        (width - 1).toShort, pyNeighbor, n._2, costBand, newChanges)
                    }
                  }
//                  for (py <- math.max(srcPoint.py - 1, 0) to math.min(srcPoint.py + 1, height - 1)) {
//                    addChanges(srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster, (width - 1).toShort, py.toShort,
//                      DirectionBand.LEFT_BAND, costBand, numBands, newChanges)
//                  }
                }
              }
              else if (triplet.attr == CostDistanceEdgeType.TO_RIGHT) {
                // The destination tile is to the right of the source tile. For any pixels that changed
                // in the source tile, propagate those changes to the neighboring pixels in the
                // left-most column of the destination tile.
                if (srcPoint.px == width - 1) {
                  for (n <- neighborsToRight) {
                    @transient val pyNeighbor: Short = (srcPoint.py + n._1).toShort
                    if (pyNeighbor >= 0 && pyNeighbor < height) {
                      addChanges(triplet.srcId, triplet.dstId, zoomLevel, res, pixelHeightM,
                        srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster,
                        0.toShort, pyNeighbor, n._2, costBand, newChanges)
                    }
                  }
//                  for (py <- math.max(srcPoint.py - 1, 0) to math.min(srcPoint.py + 1, height - 1)) {
//                    addChanges(srcPoint, triplet.srcAttr.raster, triplet.dstAttr.raster, 0, py.toShort,
//                      DirectionBand.RIGHT_BAND, costBand, numBands, newChanges)
//                  }
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
//              println("  New changed points after processing srcPoint")
//              for (p <- newChanges.getAllPoints()) {
//                println("    Source changed point: " + p.px + ", " + p.py + " = " + p.cost)
//              }
            }
          }
        }
//          else {
//          println("IN SENDMSG for src id " + triplet.srcId + " there are no changes ")
//          }
        //        println("SENDMSG from " + triplet.srcId + " to " + triplet.dstId + " took " + ((System.nanoTime() - t0).toDouble / 1000000) + " ms")
        //        if (newChanges == null) {
        //          println("  no messages are sent to destination")
        //          Iterator.empty
        //        } else {
        //          if (newChanges.size > 0) {
        ////            println("After SENDMSG for tile " + triplet.srcId + "messages sent: " + newChanges.size)
        //          }
        //          if (triplet.srcAttr.changedPoints.size > 0) {
        //            println("  message count " + newChanges.size)
        //            val emptyChanges: ChangedPoints = new ChangedPoints
        //            Iterator((triplet.srcId, emptyChanges), (triplet.dstId, newChanges))
        ////          Iterator((triplet.dstId, newChanges))
        //          }
        //          else {
        //            println("  message count " + newChanges.size)
        //            Iterator((triplet.dstId, newChanges))
        //          }
        //        }
        if (newChanges == null) {
          log.warn("  SENDMSG returning empty from no changes")
          Iterator.empty
//          if (triplet.srcAttr.changedPoints.size > 0) {
//            val emptyChanges: ChangedPoints = new ChangedPoints
//              println("  returning empty changes for source and no changes for destination")
//            Iterator((triplet.srcId, emptyChanges))
//          }
//          else {
//              println("  returning no changes for destination")
//            Iterator.empty
//          }
        } else {
          if (newChanges.size > 0) {
              log.warn("  SENDMSG returning " + newChanges.size + " changes for destination")
            Iterator((triplet.dstId, newChanges))
          }
          else {
            log.warn("  SENDMSG returning empty after 0 changes")
            Iterator.empty
          }
        }
      },
      // mergeMsg
      // TODO: Merge a and b into a single ChangedPoints object
      (a, b) => {
        @transient val t0 = System.nanoTime()
        @transient var merged = new ChangedPoints(false)
        @transient val aPoints: List[CostPoint] = a.getAllPoints
        for (p <- aPoints) {
          merged.addPoint(p)
        }
        @transient val bPoints: List[CostPoint] = b.getAllPoints
        for (p <- bPoints) {
          merged.addPoint(p)
        }
//          println("MERGEMSG took " + ((System.nanoTime() - t0).toDouble / 1000000) + " ms")
        merged
      }
    )
  }

  def processVertices(tileId: Long, zoom: Int, res: Double, pixelHeightM: Double,
                      raster: WritableRaster, changes: ChangedPoints): ChangedPoints =
  {
    log.warn("PROCESS VERTICES received " + changes.size + " changes")
    @transient val neighborMetadata8Band = Array(
      (-1, -1, DirectionBand.UP_LEFT_BAND)
      , (-1, 0, DirectionBand.LEFT_BAND)
      , (-1, 1, DirectionBand.DOWN_LEFT_BAND)
      , (1, -1, DirectionBand.UP_RIGHT_BAND)
      , (1, 0, DirectionBand.RIGHT_BAND)
      , (1, 1, DirectionBand.DOWN_RIGHT_BAND)
      , (0, -1, DirectionBand.UP_BAND)
      , (0, 1, DirectionBand.DOWN_BAND)
    )
//    @transient val neighborMetadata8Band = Array(
//      (-1, 0, DirectionBand.LEFT_BAND)
//      , (1, 0, DirectionBand.RIGHT_BAND)
//      , (0, -1, DirectionBand.UP_BAND)
//      , (0, 1, DirectionBand.DOWN_BAND)
//    )

//    val neighborMetadata8Band = Array(
//      (-1, -1, 0) // top-left
//      , (-1, 0, 7) // left
//      , (-1, 1, 6) // bottom-left
//      , (1, -1, 2) // top-right
//      , (1, 0, 3) // right
//      , (1, 1, 4) // bottom-right
//      , (0, -1, 1) // top
//      , (0, 1, 5) // bottom
//    )
//    println("Started processing tile " + TMSUtils.tileid(tileId, zoom))
    @transient val numBands = raster.getNumBands
    @transient val totalCostBand = numBands - 1 // cost band is the last band in the raster
    @transient val startTime = System.nanoTime()
    @transient val newChanges: ChangedPoints = new ChangedPoints(false)
//    var queue = new java.util.PriorityQueue(1000, new Comparator[CostPoint] {
//      override def equals(a: Any) = a.equals(this)
//      override def compare(o1: CostPoint, o2: CostPoint): Int = o1.cost.compareTo(o2.cost)
//    })
//    var queue = new org.apache.commons.collections.buffer.PriorityBuffer()
    @transient var queue = new java.util.concurrent.PriorityBlockingQueue[CostPoint]()
    for (cp <- changes.getAllPoints) {
      queue.add(cp)
//      vertex.raster.setSample(cp.px, cp.py, totalCostBand, cp.cost)
    }
//    val queue = new MyQueue[CostPoint]()(Ordering.by(queueValue).reverse)
//    val queue = new MyJavaQueue[CostPoint]()(Ordering.by(queueValue).reverse)
//    var queue = scala.collection.mutable.PriorityQueue[CostPoint]()(Ordering.by(queueValue).reverse)
//    var queue = scala.collection.mutable.PriorityQueue[CostPoint]()
//    var queue: scala.collection.mutable.TreeSet[CostPoint] = scala.collection.mutable.TreeSet[CostPoint]()
//    queue ++= changes.getAllPoints
    // Now process each element from the priority queue until empty
    // For each element, check to see if the cost is smaller than the
    // cost in the band3 of the vertex. If it is, then compute the cost
    // to each of its neighbors, check the new cost to see if it's less
    // than the neighbor's current cost, and add a new entry to the queue
    // for the neighbor point. If a point around the perimeter of the tile
    // changes, then add an entry to local changedPoints.

    // Get FloatBuffer from vertex so we can manipulate it directly
//    val dataBuffer: DataBufferFloat = vertex.raster.getDataBuffer.asInstanceOf[DataBufferFloat]
//    val buf: Array[Float] = dataBuffer.getData
    @transient val width = raster.getWidth
    @transient val height = raster.getHeight
    // Store the edge values in the raster before processing it so we can compare
    // after processing to see which edge pixels changed
    @transient val origTopEdgeValues: Array[Float] = new Array[Float](width)
    @transient val origBottomEdgeValues: Array[Float] = new Array[Float](width)
    @transient val origLeftEdgeValues: Array[Float] = new Array[Float](height)
    @transient val origRightEdgeValues: Array[Float] = new Array[Float](height)
    @transient val preStart: Double = System.nanoTime()
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
    @transient val preProcessingTime: Double = System.nanoTime() - preStart
//    @transient var lastQueueLen = queue.size
    @transient var totalEnqueue: Double = 0.0
    @transient var totalDequeue: Double = 0.0
    @transient var maxHeapSize: Int = 0
    @transient var counter: Long = 0L

    // Set up variables used for calculating great circle distances of pixels
    @transient val tile: TMSUtils.Tile = TMSUtils.tileid(tileId, zoom)

    @transient val o: LatLng = new LatLng(0, 0)
    @transient val n: LatLng = new LatLng(res, 0)
    @transient val pixelHeightM: Double = LatLng.calculateGreatCircleDistance(o, n)

//    @transient val startPx: Double = tile.tx * width
//    // since Rasters have their UL as 0,0, just tile.ty * tileSize does not work
//    @transient val startPy: Double = (tile.ty * height) + height - 1

//    @transient val lonStart: Double = startPx * res - 180.0
//    @transient val latStart: Double = startPy * res - 90.0
//
//    @transient val lonNext: Double = lonStart + res
//    @transient val latNext: Double = latStart + res
//    o.setLat(latStart)
//    o.setLng(lonStart)
//    n.setLat(latNext)
//    n.setLng(lonNext)

    // Process the queue of changed points until it is empty
    breakable {
      while (!queue.isEmpty) {
        //    while (queue.nonEmpty) {
        if (queue.size > maxHeapSize) {
          maxHeapSize = queue.size
        }
        counter += 1
        @transient var t0 = System.nanoTime()
        @transient val currPoint = queue.poll()

        // Stop processing points as soon as the cost goes beyond the max cost
//        if (currPoint.cost > maxCost) {
//          break
//        }
        totalDequeue = totalDequeue + (System.nanoTime() - t0)
        @transient val currTotalCost = raster.getSampleFloat(currPoint.px, currPoint.py, totalCostBand)
        //      if (currPoint.cost <= currTotalCost || currTotalCost.isNaN) {
        if (isValueSmaller(currPoint.cost, currTotalCost)) {
          raster.setSample(currPoint.px, currPoint.py, totalCostBand, currPoint.cost)
          // In the vertex data, set the point's cost to the new smaller value
          //        vertex.raster.setSample(currPoint.px, currPoint.py, totalCostBand, currPoint.cost)
          // Since this point has a new cost, check to see if the cost to each
          // of its neighbors is smaller than the current cost assigned to those
          // neighbors. If so, add those neighbor points to the queue.
          for (metadata <- neighborMetadata8Band) {
            @transient val pxNeighbor: Short = (currPoint.px + metadata._1).toShort
            @transient val pyNeighbor: Short = (currPoint.py + metadata._2).toShort
            if (pxNeighbor >= 0 && pxNeighbor < width && pyNeighbor >= 0 && pyNeighbor < height) {
              @transient val currNeighborTotalCost = raster.getSampleFloat(pxNeighbor, pyNeighbor, totalCostBand)
              @transient val directionBand: Short = metadata._3.toShort
              // Compute the cost increase which is the sum of the distance from the
              // source pixel center point to the neighbor pixel center point.
              @transient val sourcePixelCost = getPixelCost(tileId, zoomLevel, currPoint.px, currPoint.py,
                metadata._3, res, pixelHeightM, raster)
              @transient val neighborPixelCost = getPixelCost(tileId, zoomLevel, pxNeighbor, pyNeighbor,
                metadata._3, res, pixelHeightM, raster)
//              @transient val sourcePixelCost = vertex.raster.getSampleFloat(currPoint.px, currPoint.py, directionBand)
//              @transient val neighborPixelCost = vertex.raster.getSampleFloat(pxNeighbor, pyNeighbor, directionBand)
              if (neighborPixelCost.isNaN) {
                // If the cost to traverse the neighbor is NaN (unreachable), and no
                // total cost has yet been assigned to the neighbor (PositiveInfinity),
                // then set the neighbor's total cost to NaN since it will never be
                // reachable.
//              if (currNeighborTotalCost == Float.PositiveInfinity) {
                raster.setSample(pxNeighbor, pyNeighbor, totalCostBand, neighborPixelCost)
//              }
              }
              else {
                @transient val costIncrease = sourcePixelCost * 0.5f + neighborPixelCost * 0.5f
                @transient val newNeighborCost = currPoint.cost + costIncrease
                if (isValueSmaller(newNeighborCost, currNeighborTotalCost)) {
//              if (newNeighborCost < currNeighborTotalCost || currNeighborTotalCost.isNaN) {
                  @transient val neighborPoint = new CostPoint(pxNeighbor, pyNeighbor, newNeighborCost)
                  t0 = System.nanoTime()
                  queue.add(neighborPoint)
                  totalEnqueue = totalEnqueue + (System.nanoTime() - t0)
//                vertex.raster.setSample(pxNeighbor, pyNeighbor, totalCostBand, newNeighborCost)
                }
              }
            }
          }
        }
      }
    }
//    println("Max heap size for tile " + tileId + " is " + maxHeapSize)
//    println("Loop iterations for tile " + tileId + " is " + counter)
    @transient val t0 = System.nanoTime()
    // Find edge pixels that have changed so we know how to send messages
    for (px <- 0 until width) {
      @transient val currTopCost: Float = raster.getSampleFloat(px, 0, totalCostBand)
      @transient val origTopValue = origTopEdgeValues(px)
      if (isValueSmaller(currTopCost, origTopValue)) {
//      if (currTopCost < origTopValue || origTopValue.isNaN) {
//        println(px + ", 0 orig = " + origTopEdgeValues(px) + ", new = " + currCost)
        newChanges.addPoint(new CostPoint(px.toShort, 0, currTopCost))
      }
      @transient val currBottomCost = raster.getSampleFloat(px, height-1, totalCostBand)
      @transient val origBottomValue = origBottomEdgeValues(px)
      if (isValueSmaller(currBottomCost, origBottomValue)) {
//      if (currBottomCost < origBottomValue || origBottomValue.isNaN) {
//        println(px + ", " + (height-1) + " orig = " + origTopEdgeValues(px) + ", new = " + currCost)
        newChanges.addPoint(new CostPoint(px.toShort, (height-1).toShort, currBottomCost))
      }
    }
    // Don't process corner pixels again (already handled as part of top/bottom
    for (py <- 1 until height-1) {
      @transient val currLeftCost: Float = raster.getSampleFloat(0, py, totalCostBand)
      @transient val origLeftValue = origLeftEdgeValues(py)
      if (isValueSmaller(currLeftCost, origLeftValue)) {
//      if (currLeftCost < origLeftValue || origLeftValue.isNaN) {
//        println("0, " + py + " orig = " + origLeftEdgeValues(py) + ", new = " + currCost)
        newChanges.addPoint(new CostPoint(0, py.toShort, currLeftCost))
      }
      @transient val currRightCost = raster.getSampleFloat(width-1, py, totalCostBand)
      @transient val origRightValue = origRightEdgeValues(py)
      if (isValueSmaller(currRightCost, origRightValue)) {
//      if (currRightCost < origRightValue || origRightValue.isNaN) {
//        println((width-1) + ", " + py + " orig = " + origRightEdgeValues(py) + ", new = " + currCost)
        newChanges.addPoint(new CostPoint((width-1).toShort, py.toShort, currRightCost))
      }
    }
    @transient val totalTime: Double = System.nanoTime() - startTime
    @transient val postProcessingTime: Double = System.nanoTime() - t0
//    println("Done processing tile " + tileId + " took " + (totalTime.toDouble / 1000000.0))
//    println("  Enqueue took " + (totalEnqueue / 1000000.0))
//    println("  Dequeue took " + (totalDequeue / 1000000.0))
//    println("  Other took " + ((totalTime - totalEnqueue - totalDequeue) / 1000000.0))
//    println("  Pre processing took " + (postProcessingTime / 1000000.0))
//    println("  Post processing took " + (postProcessingTime / 1000000.0))
    // After all the points have been processed, assign the local changedPoints
    // list to the vertex change points so it is available in the sendMsg
    // method later.
    if (newChanges.isEmpty)
    {
      log.warn("PROCESS VERTICES for " + TMSUtils.tileid(tileId, zoom) + " newChanges is empty")
    }
    else {
      log.warn("PROCESS VERTICES for " + TMSUtils.tileid(tileId, zoom) + " newChanges contains " + newChanges.size)
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
    @transient val tile: TMSUtils.Tile = TMSUtils.tileid(tileId, zoom)
    @transient val startPx: Double = tile.tx * raster.getWidth
    // since Rasters have their UL as 0,0, just tile.ty * tileSize does not work
    @transient val startPy: Double = (tile.ty * raster.getHeight) + raster.getHeight - 1

    @transient val lonStart: Double = startPx * res - 180.0
    @transient val latStart: Double = startPy * res - 90.0

    @transient val lonNext: Double = lonStart + res
    @transient val latNext: Double = latStart + res
    val distanceMeters =
      if (direction == DirectionBand.DOWN_BAND || direction == DirectionBand.UP_BAND) {
        // Vertical direction
        @transient val o: LatLng = new LatLng(latStart, lonStart)
        @transient val n: LatLng = new LatLng(latNext, lonStart)
        LatLng.calculateGreatCircleDistance(o, n)
      }
      else if (direction == DirectionBand.LEFT_BAND || direction == DirectionBand.RIGHT_BAND)
      {
        // Horizontal direction
        @transient val o: LatLng = new LatLng(latStart, lonStart)
        @transient val n: LatLng = new LatLng(latStart, lonNext)
        LatLng.calculateGreatCircleDistance(o, n)
      }
      else {
        // Diagonal direction
        @transient val o: LatLng = new LatLng(latStart, lonStart)
        @transient val n: LatLng = new LatLng(latNext, lonNext)
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
      return false;
    }
    // Allow for floating point math inaccuracy
    return newValue < (origValue - 1e-7)
  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = {

    if (job.hasSetting(CostDistanceDriver.FRICTION_SURFACE_ARG)) {
      frictionSurface = job.getSetting(CostDistanceDriver.FRICTION_SURFACE_ARG)
    }
    else {
      val name: String = CostDistanceDriver.FRICTION_SURFACE_ARG
      throw new Exception(s"Missing required argument '$name'")
    }

    if (job.hasSetting(CostDistanceDriver.OUTPUT_ARG)) {
      output = job.getSetting(CostDistanceDriver.OUTPUT_ARG)
    }
    else {
      val name: String = CostDistanceDriver.OUTPUT_ARG
      throw new Exception(s"Missing required argument '$name'")
    }

    if (job.hasSetting(CostDistanceDriver.ZOOM_LEVEL_ARG)) {
      zoomLevel = job.getSetting(CostDistanceDriver.ZOOM_LEVEL_ARG).toInt
    }
    else {
      val name: String = CostDistanceDriver.ZOOM_LEVEL_ARG
      throw new Exception(s"Missing required argument '$name'")
    }

    if (job.hasSetting(CostDistanceDriver.MAX_COST_ARG)) {
      maxCost = job.getSetting(CostDistanceDriver.MAX_COST_ARG).toDouble
    }
    else {
      val name: String = CostDistanceDriver.MAX_COST_ARG
      throw new Exception(s"Missing required argument '$name'")
    }

    conf.set("spark.storage.memoryFraction", "0.2") // set the storage amount lower...
    conf.set("spark.shuffle.memoryFraction", "0.30") // set the shuffle higher

    if (job.hasSetting(CostDistanceDriver.SOURCE_POINTS_ARG)) {
      val pointsArg = job.getSetting(CostDistanceDriver.SOURCE_POINTS_ARG)
      val pointsArrayArg = pointsArg.split(";")
      val wktReader = new WKTReader()
      pointsArrayArg.foreach(U => {
        val strPoint = if (U.startsWith("'"))
        {
          U.substring(1, U.length - 1)
        }
        else{
          U
        }
        val point = wktReader.read(strPoint)
        val jtsPoint = point.asInstanceOf[Point]
        sourcePoints.append((jtsPoint.getX.toFloat, jtsPoint.getY.toFloat))
//        val coords = U.split(",")
//        if (coords.size != 2) {
//          throw new Exception(s"Invalid input point '$U': expected 'longitude,latitude'")
//        }
//        val lon: Float = coords(0).toFloat
//        val lat: Float = coords(1).toFloat
//        sourcePoints.append((lon, lat))
      })
    }
    else {
      val name: String = CostDistanceDriver.SOURCE_POINTS_ARG
      throw new Exception(s"Missing required argument '$name' in the format longitude,latitude with multiple points separated by a semi-colon")
    }

    true
  }


  override def teardown(job: JobArguments, conf:SparkConf): Boolean = {
    true
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(zoomLevel)
  }

  override def readExternal(in: ObjectInput): Unit = {
    zoomLevel = in.readInt()
  }
}

class CostDistanceEdge(val fromTileId: Long, val toTileId: Long, val direction: Byte) {
}

//Stores information about points whose cost has changed during processing. CostPoint
//ordering should be in increasing order by cost so that the PriorityQueue processes
//minimum cost elements first.
class CostPoint(@transient var px: Short, @transient var py: Short, @transient var cost: Float
                 ) extends Ordered[CostPoint] with Externalizable with KryoSerializable {
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

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeShort(px)
    output.writeShort(py)
    output.writeFloat(cost)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    px = input.readShort()
    py = input.readShort()
    cost = input.readFloat()
  }
}

//class CostPoint(val px: Short, val py: Short, val cost: Float
//                 ) extends Ordered[CostPoint] with Serializable {
//  def compare(that: CostPoint): Int = {
//    cost.compare(that.cost)
//  }
//}

// Stores a list of points around the edges of a tile that changed while costs
// are computed for a tile.
class ChangedPoints(var initial: Boolean) extends Externalizable with KryoSerializable {
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
    return initial
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
      val cp = iter.next
//      out.writeShort(cp._1._1)
//      out.writeShort(cp._1._2)
      cp._2.writeExternal(out)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    initial = in.readBoolean()
    val length = in.readInt
    for (i <- 0 until length) {
//      val px = in.readShort()
//      val py = in.readShort()
      var cp = new CostPoint(-1, -1, 0.0f)
      cp.readExternal(in)
      changes.put((cp.px, cp.py), cp)
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(changes.size)
    val iter = changes.iterator
    while (iter.hasNext) {
      val cp = iter.next
//      output.writeShort(cp._1._1)
//      output.writeShort(cp._1._2)
      cp._2.write(kryo, output)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val length = input.readInt
    for (i <- 0 until length) {
//      val px = input.readShort()
//      val py = input.readShort()
      var cp = new CostPoint(-1, -1, 0.0f)
      cp.read(kryo, input)
      changes.put((cp.px, cp.py), cp)
    }
  }
}

//@SerialVersionUID(-7588980448693010399L)
// TODO: Remove tileid and zoom from VertexType after debugging!!!
class VertexType(@transient var raster: WritableRaster,
                 @transient var changedPoints: ChangedPoints,
                  @transient var tileid: Long,
                  @transient var zoom: Int)
  extends Externalizable with KryoSerializable {

  def this() {
    this(null, null, -1, -1)
  }

  @throws(classOf[IOException])
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(tileid)
    out.writeInt(zoom)
//    if (changedPoints == null) {
//      out.writeInt(-1)
//    }
//    else {
    out.writeBoolean(changedPoints.isInitial)
    changedPoints.writeExternal(out)
//      out.writeInt(changedPoints.size)
//      for (cp <- changedPoints.getAllPoints) {
//        out.writeShort(cp.px)
//        out.writeShort(cp.py)
//        out.writeFloat(cp.cost)
//      }
//    }
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
//    changedPoints = new ChangedPoints(initial)
//    val changedPointCount = in.readInt()
//    println("num changed points is " + changedPointCount)
//    for (i <- 0 until changedPointCount) {
//      val px = in.readShort()
//      val py = in.readShort()
//      val cost = in.readFloat()
//      changedPoints.addPoint(new CostPoint(px, py, cost))
//    }
    val byteCount: Int = in.readInt()
    val rasterBytes: Array[Byte] = new Array[Byte](byteCount)
    var offset: Int = 0
    in.readFully(rasterBytes, offset, byteCount)
    raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(rasterBytes, null))
  }

  override def write(kryo: Kryo, output: Output): Unit = {
//    if (changedPoints == null) {
//      output.writeInt(0)
//    }
//    else {
      output.writeBoolean(changedPoints.isInitial)
      output.writeInt(changedPoints.size)
      for (cp <- changedPoints.getAllPoints) {
        output.writeShort(cp.px)
        output.writeShort(cp.py)
        output.writeFloat(cp.cost)
      }
//    }
    val rasterBytes: Array[Byte] = RasterWritable.toBytes(raster, null)
    output.writeInt(rasterBytes.length)
    output.write(rasterBytes)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val initial = input.readBoolean()
    changedPoints = new ChangedPoints(initial)
    val changedPointCount = input.readInt()
    for (i <- 0 until changedPointCount) {
      val px = input.readShort()
      val py = input.readShort()
      val cost = input.readFloat()
      changedPoints.addPoint(new CostPoint(px, py, cost))
    }
    val byteCount: Int = input.readInt()
    val rasterBytes: Array[Byte] = new Array[Byte](byteCount)
    var offset: Int = 0
    input.readBytes(rasterBytes, offset, byteCount)
    raster = RasterWritable.toRaster(rasterBytes, null).asInstanceOf[WritableRaster]
    //    raster = RasterUtils.makeRasterWritable(RasterWritable.toRaster(rasterBytes, null))
  }
}
