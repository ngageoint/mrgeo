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
import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.Geometry
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.utils._

import scala.collection.mutable.ListBuffer

object RasterizeVectorMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("RasterizeVector")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizeVectorMapOp(node, variables)
}

class RasterizeVectorMapOp extends AbstractRasterizeVectorMapOp with Externalizable
{
  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def rasterize(groupedGeometries: RDD[(TileIdWritable, Iterable[Geometry])]): RDD[(TileIdWritable, RasterWritable)] = {
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

  override def vectorsToTiledRDD(vectorRDD: VectorRDD): RDD[(TileIdWritable, Geometry)] = {
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
