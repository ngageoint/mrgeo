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

import java.awt.image.{Raster, WritableRaster}
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, Graph}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.utils.{Bounds, TMSUtils}

import scala.collection.mutable.ListBuffer

object FocalBuilder extends Logging {

  def create(tiles:RDD[(TileIdWritable, RasterWritable)],
      bufferX:Int, bufferY:Int, bounds:Bounds, zoom:Int, nodatas:Array[Number], context:SparkContext):RDD[(TileIdWritable, RasterWritable)] = {

    val sample:Raster = RasterWritable.toRaster(tiles.first()._2)

    val tilesize = sample.getWidth

    val offsetX = (bufferX / tilesize) + 1
    val offsetY = (bufferY / tilesize) + 1

    val dstW = sample.getWidth + bufferX * 2
    val dstH = sample.getHeight + bufferY * 2

    val tb = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(bounds), zoom, tilesize)
    val minX = tb.w
    val minY = tb.s
    val maxX = tb.e
    val maxY = tb.n

    val pieces = new PairRDDFunctions[TileIdWritable, (Int, Int, Int, Int, RasterWritable)](tiles.flatMap(tile => {
      val pieces = ListBuffer[(TileIdWritable, (Int, Int, Int, Int, RasterWritable))]()
      val from = TMSUtils.tileid(tile._1.get(), zoom)

      val src = RasterWritable.toRaster(tile._2)
      val srcW = src.getWidth
      val srcH = src.getHeight

      for (y <- -offsetY to offsetY) {
        for (x <- -offsetX to offsetX) {
          val to = new TMSUtils.Tile(from.tx + x, from.ty + y)
          if (to.ty >= minY && to.ty <= maxY && to.tx >= minX && to.tx <= maxX) {
            var srcX = -1
            var dstX = -1

            var width = bufferX
            if (x == offsetX) {
              srcX = srcW - width
              dstX = 0
            }
            else if (x == -offsetX) {
              srcX = 0
              dstX = dstW - width
            }
            else {
              srcX = 0
              dstX = bufferX + (x * srcW)
              width = srcW
            }

            var srcY = -1
            var dstY = -1

            var height = bufferY
            if (y == -offsetY) {
              srcY = srcH - height
              dstY = 0
            }
            else if (y == offsetY) {
              srcY = 0
              dstY = dstH - height
            }
            else {
              srcY = 0
              dstY = bufferY + (y * srcH)
              height = srcH
            }

            val piece = src.createChild(srcX, srcY, width, height, 0, 0, null)
            pieces.append((new TileIdWritable(TMSUtils.tileid(to.tx, to.ty, zoom)), (dstX, dstY, width, height, RasterWritable.toWritable(piece))))
          }
        }
      }
      pieces.iterator

    })).groupByKey()

    val focal = pieces.map(tile => {
      val first = RasterWritable.toRaster(tile._2.head._5)
      val dst: WritableRaster = RasterUtils.createCompatibleEmptyRaster(first, dstW, dstH, nodatas)

      for (piece <- tile._2) {
        val x = piece._1
        val y = piece._2
        val w = piece._3
        val h = piece._4
        val src = RasterWritable.toRaster(piece._5)

        dst.setDataElements(x, y, w, h, src.getDataElements(0, 0, src.getWidth, src.getHeight, null))
      }

      (new TileIdWritable(tile._1), RasterWritable.toWritable(dst))
    })

    focal
  }

  def create2(tiles:RDD[(TileIdWritable, RasterWritable)],
      bufferX:Int, bufferY:Int, zoom:Int, nodatas:Array[Number], context:SparkContext):RDD[(Long, RasterWritable)] = {

    val sample:Raster = RasterWritable.toRaster(tiles.first()._2)

    val tilesize = sample.getWidth

    val offsetX = (bufferX / tilesize) + 1
    val offsetY = (bufferY / tilesize) + 1

    def buildEdges(tiles: RDD[(TileIdWritable, RasterWritable)],
        offsetX: Int, offsetY: Int, zoom: Int): RDD[Edge[EdgeDirection]] = {

      val maxid = TMSUtils.tileid(TMSUtils.maxTileId(zoom), zoom)
      val maxX = maxid.tx
      val maxY = maxid.ty

      tiles.flatMap(tile => {
        val edges = ListBuffer[Edge[EdgeDirection]]()

        val from = TMSUtils.tileid(tile._1.get(), zoom)
        for (y <- (from.ty - offsetY) to (from.ty + offsetY)) {
          if (y >= 0 && y <= maxY) {
            for (x <- (from.tx - offsetX) to (from.tx + offsetX)) {
              if (x >= 0 && x <= maxX) {
                val to = TMSUtils.tileid(x, y, zoom)
                edges.append(new Edge(to, tile._1.get, EdgeDirection.In))
              }
            }
          }
        }

        edges.iterator
      })
    }

    def buildFocal(ec: EdgeContext[RasterWritable, EdgeDirection, RasterWritable]) = {

      val srcId = TMSUtils.tileid(ec.srcId, zoom)
      val dstId = TMSUtils.tileid(ec.dstId, zoom)

      val x = (srcId.tx - dstId.tx).toInt // left to right
      val y = (dstId.ty - srcId.ty).toInt // bottom to top


      val src = RasterWritable.toRaster(ec.srcAttr)
      val srcW = src.getWidth
      val srcH = src.getHeight

      val dstW = srcW + bufferX * 2
      val dstH = srcH + bufferY * 2

      var srcCol = -1
      var dstCol = -1

      var w = bufferX
      if (x == -offsetX) {
        srcCol = srcW - w
        dstCol = 0
      }
      else if (x == offsetX) {
        srcCol = 0
        dstCol = dstW - w
      }
      else {
        srcCol = 0
        dstCol = bufferX
        w = srcW
      }

      var srcRow = -1
      var dstRow = -1

      var h = bufferY
      if (y == -offsetY) {
        srcRow = srcH - h
        dstRow = 0
      }
      else if (y == offsetY) {
        srcRow = 0
        dstRow = dstH - h
      }
      else {
        srcRow = 0
        dstRow = bufferY
        h = srcH
      }

      //      println("src: id: " + ec.srcId + " tx: " + srcId.tx + " ty: " + srcId.ty)
      //      println("dst: id: " + ec.dstId + " tx: " + dstId.tx + " ty: " + dstId.ty)
      //      println("x: " + x + " y: " + y)
      //      println("src: x: " + srcCol + " y: " + srcRow + " w: " + w + " h: " + h)
      //      println("dst: x: " + dstCol + " y: " + dstRow + " w: " + w + " h: " + h)
      //      println("***")

      try {

        val dst: WritableRaster = RasterUtils.createCompatibleEmptyRaster(src, dstW, dstH, nodatas)
        dst.setDataElements(dstCol, dstRow, w, h, src.getDataElements(srcCol, srcRow, w, h, null))

        //        val fn:String = "/data/export/slope-test/" + ec.dstId + "-" + ec.srcId + ".tif"
        //        GDALUtils.saveRaster(dst, fn, dstId.tx, dstId.ty, zoom, 512, nodatas(0).doubleValue())

        ec.sendToDst(RasterWritable.toWritable(dst))
      }
      catch {
        case e: ArrayIndexOutOfBoundsException =>
          logError("src: id: " + ec.srcId + " tx: " + srcId.tx + " ty: " + srcId.ty)
          logError("dst: id: " + ec.dstId + " tx: " + dstId.tx + " ty: " + dstId.ty)
          logError("x: " + x + " y: " + y)
          logError("src: x: " + srcCol + " y: " + srcRow + " w: " + w + " h: " + h)
          logError("dst: x: " + dstCol + " y: " + dstRow + " w: " + w + " h: " + h)
          throw e
      }
    }

    def mergeFocal(a: RasterWritable, b: RasterWritable):RasterWritable = {
      val dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(a))
      val src = RasterWritable.toRaster(b)

      //var fn:String = "/data/export/slope-test/b.tif"
      //GDALUtils.saveRaster(src, fn, 0, 0, zoom, 512, nodatas(0).doubleValue())

      //fn = "/data/export/slope-test/b.tif"
      //GDALUtils.saveRaster(dst, fn, 0, 0, zoom, 512, nodatas(0).doubleValue())

      RasterUtils.mosaicTile(src, dst, nodatas)

      //fn = "/data/export/slope-test/m.tif"
      //GDALUtils.saveRaster(dst, fn, 0, 0, zoom, 512, nodatas(0).doubleValue())

      RasterWritable.toWritable(dst)
    }


    val edges = buildEdges(tiles, offsetX, offsetY, zoom)

    // map the tiles so the key is the tileid as a long
    val vertices = tiles.map(tile => {
      // NOTE:  Some record readers reuse key/value objects, so we make these unique here
      (tile._1.get(), new RasterWritable(tile._2))
    })

    val defaultVertex =
      RasterWritable.toWritable(RasterUtils.createEmptyRaster(tilesize, tilesize, sample.getNumBands,
        sample.getTransferType, nodatas))

    val graph = Graph(vertices, edges, defaultVertex,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)

    val focal = graph.aggregateMessages[RasterWritable](
      sendMsg = buildFocal,
      mergeMsg = mergeFocal)

    focal
  }

}


class FocalNeighborhood extends Externalizable {
  var offsetX:Int = Int.MinValue
  var offsetY:Int = Int.MinValue
  var writable:RasterWritable = null

  def this(offsetX: Int, offsetY: Int, writable:RasterWritable) {
    this()
    this.offsetX = offsetX
    this.offsetY = offsetY
    this.writable = writable
  }

  override def readExternal(in: ObjectInput): Unit = {
    offsetX = in.readInt()
    offsetY = in.readInt()
    val len = in.readInt()
    val bytes = Array.ofDim[Byte](len)
    in.read(bytes)
    writable = new RasterWritable(bytes)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(offsetX)
    out.writeInt(offsetY)

    val bytes = writable.getBytes
    out.writeInt(bytes.length)
    out.write(bytes)
  }
}
