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

import java.awt.image.{WritableRaster, DataBuffer}
import java.io.Externalizable

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.{Point, Geometry}
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.utils.TMSUtils

object RasterizePointsMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("RasterizePoints")
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
  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def rasterize(groupedGeometries: RDD[(TileIdWritable, Iterable[Geometry])]): RDD[(TileIdWritable, RasterWritable)] = {
    val result = groupedGeometries.map(U => {
      val tileId = U._1
      val raster = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_DOUBLE, Double.NaN)
      val countRaster = if (aggregationType == VectorPainter.AggregationType.AVERAGE) {
        RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_DOUBLE, 0.0)
      }
      else {
        null
      }
      for (geom <- U._2) {
        if (geom.isInstanceOf[Point]) {
          val pt = geom.asInstanceOf[Point]
          val tile = TMSUtils.tileid(tileId.get(), zoom)
          val pixel = TMSUtils.latLonToTilePixelUL(pt.getY, pt.getX, tile.tx, tile.ty, zoom, tilesize)
          updateRaster(raster, countRaster, pixel, pt)
        }
      }
      if (aggregationType == VectorPainter.AggregationType.AVERAGE) {
        for (x <- 0 until raster.getWidth) {
          for (y <- 0 until raster.getHeight) {
            val v = raster.getSampleDouble(x, y, 0)
            if (!v.isNaN) {
              val c = countRaster.getSampleDouble(x, y, 0)
              raster.setSample(x, y, 0, v / c)
            }
          }
        }
      }
      (tileId, RasterWritable.toWritable(raster))
    })
    result
  }

  override def vectorsToTiledRDD(vectorRDD: VectorRDD): RDD[(TileIdWritable, Geometry)] = {
    val filtered = if (bounds.nonEmpty) {
      val filterBounds = bounds.get
      vectorRDD.filter(U => {
        if (U._2.isInstanceOf[Point]) {
          val pt = U._2.asInstanceOf[Point]
          filterBounds.contains(pt.getX, pt.getY)
        }
        else {
          false
        }
      })
    }
    else {
      vectorRDD
    }

    filtered.map(U => {
      if (U._2.isInstanceOf[Point]) {
        val pt = U._2.asInstanceOf[Point]
        val tile = TMSUtils.latLonToTile(pt.getY(), pt.getX(), zoom, tilesize)
        (new TileIdWritable(TMSUtils.tileid(tile.tx, tile.ty, zoom)), U._2)
      }
      else {
        throw new IllegalArgumentException(
          "Cannot use RasterizePoints map algebra for non-point geometry: " +
            U._2.getClass.getName)
      }
    })
  }

  private def updateRaster(raster: WritableRaster, countRaster: WritableRaster,
                           pixel: TMSUtils.Pixel, geom: Point): Unit = {
    aggregationType match {
      case VectorPainter.AggregationType.AVERAGE =>
        val columnValue = geom.getAttribute(this.column.get).toDouble
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
        val columnValue = geom.getAttribute(this.column.get).toDouble
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
        val columnValue = geom.getAttribute(this.column.get).toDouble
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
        val columnValue = column match {
          case Some(c) =>
            geom.getAttribute(this.column.get).toDouble
          case None =>
            1.0
        }
        if (!columnValue.isNaN) {
          var v = raster.getSampleDouble(pixel.px.toInt, pixel.py.toInt, 0)
          if (v.isNaN) {
            v = columnValue
          }
          else {
            v = v + columnValue
          }
          raster.setSample(pixel.px.toInt, pixel.py.toInt, 0, v)
        }
    }
  }
}
