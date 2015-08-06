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

import java.awt.image.{WritableRaster, Raster}
import java.io.{ObjectInput, ObjectOutput, Externalizable}

import org.apache.spark.graphx.{EdgeContext, Graph, EdgeDirection, Edge}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.utils.TMSUtils

import scala.collection.mutable.ListBuffer

object FocalBuilder extends Logging {

  def create(tiles:RDD[(TileIdWritable, RasterWritable)],
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
        for (y <- (from.ty + offsetY) to (from.ty - offsetY)) {
          if (y >= 0 && y <= maxY) {
            for (x <- (from.tx + offsetX) to (from.tx - offsetX)) {
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

      val x = (srcId.tx - dstId.tx).toInt - offsetX // left to right
      val y = (dstId.ty - srcId.ty).toInt - offsetY // bottom to top


      try {
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
          srcCol = bufferX
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
          srcRow = bufferY
          dstRow = bufferY
          h = srcH
        }

        val dst: WritableRaster = RasterUtils.createCompatibleEmptyRaster(src, dstW, dstH, nodatas)
        dst.setDataElements(dstCol, dstRow, w, h, src.getDataElements(srcCol, srcRow, w, h, null))

        ec.sendToDst(RasterWritable.toWritable(dst))
      }
      catch {
        case e: ArrayIndexOutOfBoundsException =>
          logError("src: id: " + ec.srcId + " tx: " + srcId.tx + " ty: " + srcId.ty)
          logError("dst: id: " + ec.dstId + " tx: " + dstId.tx + " ty: " + dstId.ty)
          logError("offset: x: " + offsetX + " y: " + offsetY)
          logError("x: " + x + " y: " + y)

          throw e
      }
    }

    def mergeFocal(a: RasterWritable, b: RasterWritable):RasterWritable = {
      val dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(a))
      val src = RasterWritable.toRaster(b)

      RasterUtils.mosaicTile(src, dst, nodatas)

      RasterWritable.toWritable(src)
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
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

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
