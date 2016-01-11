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

import java.io.Externalizable

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.Geometry
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.utils._

import scala.collection.mutable.ListBuffer

object RasterizeVectorMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("rasterizevector", "rasterize")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizeVectorMapOp(node, variables)

  def create(vector: VectorMapOp, aggregator:String, cellsize:String, column:String = null) =
  {
    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column, null)
  }

}

object RasterizeVectorExactMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("rasterizevectorexact", "rasterizeexact")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizeVectorMapOp(node, variables)

  def create(vector: VectorMapOp, aggregator:String, cellsize:String,
      w:Double, s:Double, e:Double, n:Double, column:String = null) =
  {
    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column,
      new TMSUtils.Bounds(w, n, e, s).toCommaString)
  }

//  def create(vector: VectorMapOp, aggregator:String, cellsize:String, bounds:String, column:String = null) =
//  {
//    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column, bounds)
//  }

}

class RasterizeVectorMapOp extends AbstractRasterizeVectorMapOp with Externalizable
{

  def this(vector: Option[VectorMapOp], aggregator:String, cellsize:String, column:String, bounds:String) = {
    this()

    initialize(vector, aggregator, cellsize, bounds, column)
  }


  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
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
    vectorRDD.flatMap(U => {
      val geom = U._2
      var result = new ListBuffer[(TileIdWritable, Geometry)]
      // For each geometry, compute the tile(s) that it intersects and output the
      // the geometry to each of those tiles.
      val envelope: Envelope = calculateEnvelope(geom)
      val b: TMSUtils.Bounds = new TMSUtils.Bounds(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)

      bounds match {
      case Some(filterBounds) =>
        if (filterBounds.intersect(b)) {
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
  }

  def calculateEnvelope(f: Geometry): Envelope = {
    f.toJTS.getEnvelopeInternal
  }

  def getOverlappingTiles(zoom: Int, tileSize: Int, bounds: TMSUtils.Bounds): List[TileIdWritable] = {
    var tiles = new ListBuffer[TileIdWritable]
    val tb: TMSUtils.TileBounds = TMSUtils.boundsToTile(bounds, zoom, tileSize)
    for (tx <- tb.w to tb.e) {
      for (ty <- tb.s to tb.n) {
        val tile: TileIdWritable = new TileIdWritable(TMSUtils.tileid(tx, ty, zoom))
        tiles += tile
      }
    }
    tiles.toList
  }
}
