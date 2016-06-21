/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.mapalgebra

import java.io.{ByteArrayOutputStream, DataOutputStream, Externalizable}

import com.vividsolutions.jts.geom.Envelope
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.{Geometry, GeometryFactory}
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.utils.tms.{Bounds, TMSUtils, TileBounds}

import scala.collection.mutable.ListBuffer

object RasterizeVectorMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("rasterizevector", "rasterize")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizeVectorMapOp(node, variables)

  def create(vector: VectorMapOp, aggregator:String, cellsize:String, column:String = null) =
  {
    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column, null.asInstanceOf[String])
  }

}


class RasterizeVectorMapOp extends AbstractRasterizeVectorMapOp with Externalizable
{

  def this(vector: Option[VectorMapOp], aggregator:String, cellsize:String, column:String, bounds:String) = {
    this()

    initialize(vector, aggregator, cellsize, Left(bounds), column)
  }

  def this(vector: Option[VectorMapOp], aggregator:String, cellsize:String, column:String,
           rasterForBounds: Option[RasterMapOp]) = {
    this()

    initialize(vector, aggregator, cellsize, Right(rasterForBounds), column)
  }


  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def registerClasses(): Array[Class[_]] = {
    GeometryFactory.getClasses
  }

  override def rasterize(vectorRDD: VectorRDD): RDD[(TileIdWritable, RasterWritable)] =
  {
    val tiledVectors = vectorsToTiledRDD(vectorRDD)
    val localRdd = new PairRDDFunctions(tiledVectors)
    val groupedGeometries = localRdd.groupByKey()
    rasterize(groupedGeometries)
  }

  def rasterize(groupedGeometries: RDD[(TileIdWritable, Iterable[Geometry])]): RDD[(TileIdWritable, RasterWritable)] = {
    val result = groupedGeometries.map(U => {
      val tileId = U._1
      val rvp = new VectorPainter(zoom,
        aggregationType,
        column match {
        case Some(c) => c
        case None => null
        },
        tilesize)
      rvp.beforePaintingTile(tileId.get)
      for (geom <- U._2) {
        rvp.paintGeometry(geom)
      }
      val raster = rvp.afterPaintingTile()
      (tileId, raster)
    })
    result
  }

  /**
    * This method iterates through each of the features in the vectorRDD input and
    * returns a new RDD of TileIdWritable and Geometry tuples. The idea is that for
    * each feature, it identifies which tiles that feature intersects and then adds
    * a tuple to the resulting RDD for each of this tiles paired with that feature.
    * For example, if a feature intersects 5 tiles, then it adds 5 records for that
    * feature to the returned RDD.
    */
  def vectorsToTiledRDD(vectorRDD: VectorRDD): RDD[(TileIdWritable, Geometry)] = {
    val sizeAccumulator = vectorRDD.context.accumulator(0)(MaxSizeAccumulator)
    val tiledVectors = vectorRDD.flatMap(U => {
      val geom = U._2
      var result = new ListBuffer[(TileIdWritable, Geometry)]
      // For each geometry, compute the tile(s) that it intersects and output the
      // the geometry to each of those tiles.
      val envelope: Envelope = calculateEnvelope(geom)
      val baos = new ByteArrayOutputStream(1024)
      val dos = new DataOutputStream(baos)
      try {
        geom.write(dos)
        sizeAccumulator.add(baos.size())
      }
      finally {
        dos.close()
      }
      val b: Bounds = new Bounds(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)

      bounds match {
      case Some(filterBounds) =>
        if (filterBounds.intersects(b)) {
          val tiles: List[TileIdWritable] = getOverlappingTiles(zoom, tilesize, b)
          for (tileId <- tiles) {
            result += ((tileId, geom))
          }
        }
      case None =>
        val tiles: List[TileIdWritable] = getOverlappingTiles(zoom, tilesize, b)
        for (tileId <- tiles) {
          result += ((tileId, geom))
        }
      }
      result
    })
    // An individual partition cannot serialize to more than Int.MaxValue bytes
    // because Spark cannot to load the partition into memory, and it results in
    // java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE
    // To prevent that, we re-partition based on the maximum serialized size of
    // the input geometries. This is not ideal because there will likely be a lot
    // of partially filled partitions

    // Need to materialize the RDD in order for the accumulator to compute the
    // max geometry size.
    val count = tiledVectors.count()
    val maxSize = sizeAccumulator.value
    log.info("Max geometry serialized size is " + maxSize)
    // The divide by 2 is not scientific. It simply doubles the number of partitions
    // which, during testing, significatnly improved performance.
    val geomsPerPartition = Integer.MAX_VALUE / maxSize / 2
    log.info("Can fit " + geomsPerPartition + " geometries in a partition")
    val partitions = (count / geomsPerPartition).toInt + 1
    log.info("Using " + partitions + " partitions for RasterizeVector")
    tiledVectors.repartition(partitions)
  }

  def calculateEnvelope(f: Geometry): Envelope = {
    f.toJTS.getEnvelopeInternal
  }

  def getOverlappingTiles(zoom: Int, tileSize: Int, bounds: Bounds): List[TileIdWritable] = {
    var tiles = new ListBuffer[TileIdWritable]
    val tb: TileBounds = TMSUtils.boundsToTile(bounds, zoom, tileSize)
    var tx: Long = tb.w
    while (tx <= tb.e) {
      var ty: Long = tb.s
      while (ty <= tb.n) {
        val tile: TileIdWritable = new TileIdWritable(TMSUtils.tileid(tx, ty, zoom))
        tiles += tile
        ty += 1
      }
      tx += 1
    }
    tiles.toList
  }

  @SuppressFBWarnings(value = Array("NM_FIELD_NAMING_CONVENTION"), justification = "Scala generated code")
  object MaxSizeAccumulator extends AccumulatorParam[Int]
  {
    override def addInPlace(r1: Int, r2: Int): Int = {
      Math.max(r1, r2)
    }

    override def zero(initialValue: Int): Int = {
      initialValue
    }
  }
}
