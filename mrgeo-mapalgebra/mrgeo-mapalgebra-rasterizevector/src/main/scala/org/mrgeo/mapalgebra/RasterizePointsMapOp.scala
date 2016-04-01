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

import java.awt.image.{WritableRaster, DataBuffer}
import java.io.Externalizable

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.Point
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.utils.tms.TMSUtils


object RasterizePointsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("rasterizepoints")
  }

  def create(vector: VectorMapOp, aggregator:String, cellsize:String, column:String = null) =
  {
    new RasterizePointsMapOp(Some(vector), aggregator, cellsize, column, null)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizePointsMapOp(node, variables)
}


/**
  * This class is an optimization of the RasterizeVectorMapOp capability specifically
  * when the input vector data contains all points. There are two performance
  * optimizations compared to the RasterizeVectorMapOp:
  *  - each point intersects exactly one tile, so we can use a "map" operation on
  *    the vector RDD rather than flatMap
  *  - we can easily compute which pixel in a tile corresponds to any lat/lon coordinate
  *    on the planet, so there is no need to use the overhead of the VectorPainter - we
  *    can just set the pixel value directly in the output raster
  */
class RasterizePointsMapOp extends AbstractRasterizeVectorMapOp with Externalizable
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

  def rasterize(groupedGeometries: RDD[(TileIdWritable,
    Iterable[(Double, Double, Double, Boolean)])]): RDD[(TileIdWritable, RasterWritable)] =
  {
    val result = groupedGeometries.map(U => {
      val tileId = U._1
      val raster = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_DOUBLE, Double.NaN)
      val countRaster = if (aggregationType == VectorPainter.AggregationType.AVERAGE) {
        RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_DOUBLE, 0.0)
      }
      else {
        null
      }
      for (geomEntry <- U._2) {
        val tile = TMSUtils.tileid(tileId.get(), zoom)
        val pixel = TMSUtils.latLonToTilePixelUL(geomEntry._1, geomEntry._2,
          tile.tx, tile.ty, zoom, tilesize)
        updateRaster(raster, countRaster, pixel, geomEntry._1, geomEntry._2, geomEntry._3, geomEntry._4)
      }
      if (aggregationType == VectorPainter.AggregationType.AVERAGE) {
        var x: Int = 0
        while (x < raster.getWidth) {
          var y: Int = 0
          while (y < raster.getHeight) {
            val v = raster.getSampleDouble(x, y, 0)
            if (!v.isNaN) {
              val c = countRaster.getSampleDouble(x, y, 0)
              raster.setSample(x, y, 0, v / c)
            }
            y += 1
          }
          x += 1
        }
      }
      (tileId, RasterWritable.toWritable(raster))
    })
    result
  }

  def vectorsToTiledRDD(vectorRDD: VectorRDD): RDD[(TileIdWritable, (Double, Double, Double, Boolean))] = {
    val filtered = if (bounds.nonEmpty) {
      val filterBounds = bounds.get
      vectorRDD.filter(U => {
        U._2 match {
        case pt: Point =>
          filterBounds.contains(pt.getX, pt.getY)
        case _ =>
          false
        }
      })
    }
    else {
      vectorRDD
    }

    filtered.map(U => {
      U._2 match {
      case pt: Point =>
        val tile = TMSUtils.latLonToTile(pt.getY, pt.getX, zoom, tilesize)
        (new TileIdWritable(TMSUtils.tileid(tile.tx, tile.ty, zoom)),
            (pt.getY, pt.getX,
                if (column.isEmpty) {
                  0.0
                }
                else {
                  pt.getAttribute(column.get).toDouble
                },
                column.isDefined))
      case _ =>
        throw new IllegalArgumentException(
          "Cannot use RasterizePoints map algebra for non-point geometry: " +
              U._2.getClass.getName)
      }
    })
  }

  private def updateRaster(raster: WritableRaster, countRaster: WritableRaster,
                           pixel: TMSUtils.Pixel, latitude: Double,
                          longitude: Double, columnValue: Double,
                          validColumnValue: Boolean): Unit = {
    aggregationType match {
      case VectorPainter.AggregationType.AVERAGE =>
        if (!columnValue.isNaN) {
          val c = countRaster.getSampleDouble(pixel.px.toInt, pixel.py.toInt, 0) + 1
          countRaster.setSample(pixel.px.toInt, pixel.py.toInt, 0, c)
          var v = raster.getSampleDouble(pixel.px.toInt, pixel.py.toInt, 0)
          if (v.isNaN) {
            v = 0.0
          }
          v = v + columnValue
          raster.setSample(pixel.px.toInt, pixel.py.toInt, 0, v)
        }

      case VectorPainter.AggregationType.MASK =>
        raster.setSample(pixel.px.toInt, pixel.py.toInt, 0, 0.0)

      case VectorPainter.AggregationType.MAX =>
        if (!columnValue.isNaN) {
          var v = raster.getSampleDouble(pixel.px.toInt, pixel.py.toInt, 0)
          if (v.isNaN) {
            v = columnValue
          }
          else {
            v = Math.max(v, columnValue)
          }
          raster.setSample(pixel.px.toInt, pixel.py.toInt, 0, v)
        }

      case VectorPainter.AggregationType.MIN =>
        if (!columnValue.isNaN) {
          var v = raster.getSampleDouble(pixel.px.toInt, pixel.py.toInt, 0)
          if (v.isNaN) {
            v = columnValue
          }
          else {
            v = Math.min(v, columnValue)
          }
          raster.setSample(pixel.px.toInt, pixel.py.toInt, 0, v)
        }

      case VectorPainter.AggregationType.SUM =>
        // For SUM, if the user provided a numeric field, sum the values
        // of of that field across all features. Otherwise, just sum the
        // number of points that map to the pixel.
        val colVal = if (validColumnValue) {
          columnValue
        } else {
          1.0
        }
        if (!colVal.isNaN) {
          var v = raster.getSampleDouble(pixel.px.toInt, pixel.py.toInt, 0)
          if (v.isNaN) {
            v = colVal
          }
          else {
            v = v + colVal
          }
          raster.setSample(pixel.px.toInt, pixel.py.toInt, 0, v)
        }
      case _ =>
    }
  }
}
