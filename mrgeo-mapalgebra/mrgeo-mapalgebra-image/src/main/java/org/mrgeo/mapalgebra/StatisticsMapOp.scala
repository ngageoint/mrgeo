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

import java.awt.image.{DataBuffer, Raster, WritableRaster}
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{MrsPyramidMapOp, RasterMapOp}
import org.mrgeo.utils.SparkUtils

import scala.util.Sorting

object StatisticsMapOp extends MapOpRegistrar {

  override def register: Array[String] = {
    Array[String]("statistics", "stats")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new StatisticsMapOp(node, variables)
}

class StatisticsMapOp extends RasterMapOp with Externalizable {
  private val Count = "count"
  private val Max = "max"
  private val Mean = "mean"
  private val Median = "median"
  private val Min = "min"
  private val Mode = "mode"
  private val StdDev = "stddev"
  private val Sum = "sum"

  private val methods = Array[String](Min, Max, Mean, Mode, StdDev, Sum, Count)

  private var method: String = null
  private var inputs: Option[Array[Either[Option[RasterMapOp], Option[String]]]] = None

  private var rasterRDD: Option[RasterRDD] = None

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 2) {
      throw new ParserException("Usage: statistics(\"method\", raster1, raster2, ...)")
    }

    method = {
      val m = MapOp.decodeString(node.getChild(0), variables)

      if (m.isEmpty || !methods.exists(_.equals(m.get.toLowerCase))) {
        throw new ParserException("Invalid stastics method")
      }
      m.get.toLowerCase
    }

    val inputbuilder = Array.newBuilder[Either[Option[RasterMapOp], Option[String]]]
    for (i <- 1 until node.getNumChildren) {
      try {
        val raster = RasterMapOp.decodeToRaster(node.getChild(i), variables)
        inputbuilder += Left(raster)
      }
      catch {
        case e:ParserException => {
          try {
            val str = MapOp.decodeString(node.getChild(i), variables)
            inputbuilder += Right(str)
          }
          catch {
            case pe:ParserException => {
              throw new ParserException(node.getChild(i).getName + " is not a string or raster")
            }
          }

        }
      }
    }
    inputs = if (inputbuilder.result().length > 0) Some(inputbuilder.result()) else None
  }

  override def rdd(): Option[RasterRDD] = rasterRDD


  var providerProperties:ProviderProperties = null


  override def execute(context: SparkContext): Boolean = {

    val mapopbuilder = Array.newBuilder[RasterMapOp]
    val nodatabuilder = Array.newBuilder[Double]

    var zoom = 0
    var tilesize = -1
    var layers:Option[Array[String]] = None
    if (inputs.isDefined) {}
    inputs.get.foreach {
      case Left(left) =>
        left match {
        case Some(raster) =>
          mapopbuilder += raster

          val meta = raster.metadata() getOrElse (throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName))

          if (zoom <= 0 || zoom > meta.getMaxZoomLevel) {
            zoom = meta.getMaxZoomLevel
          }

          if (tilesize < 0) {
            tilesize = meta.getTilesize
          }

          nodatabuilder += meta.getDefaultValue(0)
        case _ =>
        }
      case Right(right) =>
        right match {
        case Some(str) =>
          val regex = str.replace("?", ".?").replace("*", ".*?");
          val hits = layers.getOrElse({
            layers = Some(DataProviderFactory.listImages(providerProperties))
            layers.get
          }).filter(_.matches(regex))

          hits.foreach(layer => {
            val dp = DataProviderFactory.getMrsImageDataProvider(layer, AccessMode.READ, providerProperties)

            val raster = MrsPyramidMapOp.apply(dp)
            raster.context(context)

            mapopbuilder += raster

            val meta = raster.metadata() getOrElse (throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName))

            if (zoom <= 0 || zoom > meta.getMaxZoomLevel) {
              zoom = meta.getMaxZoomLevel
            }
            if (tilesize < 0) {
              tilesize = meta.getTilesize
            }

            nodatabuilder += meta.getDefaultValue(0)
          })
        case _ =>
        }
    }

    val nodatas = nodatabuilder.result()

    val pyramids = mapopbuilder.result().map(_.rdd(zoom) getOrElse (throw new IOException("Can't load RDD! Ouch! " + getClass.getName)))

    // cogroup needs a partitioner, so we'll give one here...
    var maxpartitions = 0
    val partitions = pyramids.foreach(p => {
      if (p.partitions.length > maxpartitions) {
        maxpartitions = p.partitions.length
      }
    })

    val groups = new CoGroupedRDD(pyramids, new HashPartitioner(maxpartitions))

    rasterRDD = Some(RasterRDD(groups.map(tile => {

      val epsilon = 1e-8

      def count(raster: Raster, result: WritableRaster, nodata: Double) = {
        for (y <- 0 until raster.getHeight) {
          for (x <- 0 until raster.getWidth) {
            val px = raster.getSampleDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val cnt = result.getSampleDouble(x, y, 0)
              if (RasterMapOp.isNodata(cnt, Float.NaN)) {
                result.setSample(x, y, 0, 1)
              }
              else {
                result.setSample(x, y, 0, cnt + 1)
              }
            }
          }
        }
      }

      def min(raster: Raster, result: WritableRaster, nodata: Double) = {
        for (y <- 0 until raster.getHeight) {
          for (x <- 0 until raster.getWidth) {
            val px = raster.getSampleDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getSampleDouble(x, y, 0)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setSample(x, y, 0, px)
              }
              else {
                result.setSample(x, y, 0, Math.min(src, px))
              }
            }
          }
        }
      }

      def max(raster: Raster, result: WritableRaster, nodata: Double) = {
        for (y <- 0 until raster.getHeight) {
          for (x <- 0 until raster.getWidth) {
            val px = raster.getSampleDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getSampleDouble(x, y, 0)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setSample(x, y, 0, px)
              }
              else {
                result.setSample(x, y, 0, Math.max(src, px))
              }
            }
          }
        }
      }

      def mean(raster: Raster, result: WritableRaster, nodata: Double) = {
        for (y <- 0 until raster.getHeight) {
          for (x <- 0 until raster.getWidth) {
            val px = raster.getSampleDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getSampleDouble(x, y, 0)
              val cnt = result.getSampleDouble(x, y, 1)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setSample(x, y, 0, px)
                result.setSample(x, y, 1, 1)
              }
              else {
                result.setSample(x, y, 0, src + px)
                result.setSample(x, y, 1, cnt + 1)
              }
            }
          }
        }
      }

      def sum(raster: Raster, result: WritableRaster, nodata: Double) = {
        for (y <- 0 until raster.getHeight) {
          for (x <- 0 until raster.getWidth) {
            val px = raster.getSampleDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getSampleDouble(x, y, 0)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setSample(x, y, 0, px)
              }
              else {
                result.setSample(x, y, 0, src + px)
              }
            }
          }
        }
      }

      def mode(tiles: Array[Iterable[_]], result: WritableRaster, nodatas: Array[Double]) = {
        val rasterbuilder = Array.newBuilder[Raster]
        tiles.foreach(wr => {
          if (wr != null && wr.nonEmpty) {
            rasterbuilder += RasterWritable.toRaster(wr.asInstanceOf[Seq[RasterWritable]].head)
          }
          else {
            rasterbuilder += null
          }
        })

        val rasters = rasterbuilder.result()

        for (y <- 0 until result.getHeight) {
          for (x <- 0 until result.getWidth) {
            val valuebuilder = Array.newBuilder[Double]

            for (ndx <- rasters.indices) {
              if (rasters(ndx) != null) {
                val px = rasters(ndx).getSampleDouble(x, y, 0)
                if (RasterMapOp.isNotNodata(px, nodatas(ndx))) {
                  valuebuilder += px
                }
              }
            }

            val values = valuebuilder.result
            if (values.length > 0) {
              Sorting.quickSort(values)

              var mode = values(1)
              var modecount = 1

              var curval = mode
              var curcount = modecount

              for (i <- 1 until values.length) {
                if (values(i) > (curval - epsilon) && values(i) < (curval + epsilon)) {
                  curcount += 1
                }
                else {
                  if (curcount > modecount) {
                    mode = curval
                    modecount = curcount
                  }
                  curval = values(i)
                  curcount = 1
                }
              }
              if (curcount > modecount) {
                mode = curval
              }

              result.setSample(x, y, 0, mode)
            }
          }
        }
      }

      def median(tiles: Array[Iterable[_]], result: WritableRaster, nodatas: Array[Double]) = {
        val rasterbuilder = Array.newBuilder[Raster]
        tiles.foreach(wr => {
          if (wr != null && wr.nonEmpty) {
            rasterbuilder += RasterWritable.toRaster(wr.asInstanceOf[Seq[RasterWritable]].head)
          }
          else {
            rasterbuilder += null
          }
        })

        val rasters = rasterbuilder.result()

        for (y <- 0 until result.getHeight) {
          for (x <- 0 until result.getWidth) {
            val valuebuilder = Array.newBuilder[Double]

            for (ndx <- rasters.indices) {
              if (rasters(ndx) != null) {
                val px = rasters(ndx).getSampleDouble(x, y, 0)
                if (RasterMapOp.isNotNodata(px, nodatas(ndx))) {
                  valuebuilder += px
                }
              }
            }

            val values = valuebuilder.result
            if (values.length > 0) {
              result.setSample(x, y, 0, values.length / 2)
            }
          }
        }
      }

      def stddev(tiles: Array[Iterable[_]], result: WritableRaster, nodatas: Array[Double]) = {
        val rasterbuilder = Array.newBuilder[Raster]
        tiles.foreach(wr => {
          if (wr != null && wr.nonEmpty) {
            rasterbuilder += RasterWritable.toRaster(wr.asInstanceOf[Seq[RasterWritable]].head)
          }
          else {
            rasterbuilder += null
          }
        })

        val rasters = rasterbuilder.result()

        for (y <- 0 until result.getHeight) {
          for (x <- 0 until result.getWidth) {

            var sum1:Double = 0
            var sum2:Double = 0

            var cnt = 0
            for (ndx <- rasters.indices) {
              if (rasters(ndx) != null) {
                val px = rasters(ndx).getSampleDouble(x, y, 0)
                if (RasterMapOp.isNotNodata(px, nodatas(ndx))) {
                  sum1 += px
                  sum2 += Math.pow(px, 2)
                  cnt += 1
                }
              }
            }
            val stddev = Math.sqrt(cnt * sum2 - Math.pow(sum1, 2)) / cnt
            result.setSample(x, y, 0, stddev)
          }
        }
      }

      val bands = method match {
      case Mean => 2
      case _ => 1
      }

      val result = RasterUtils.createEmptyRaster(tilesize, tilesize, bands, DataBuffer.TYPE_FLOAT, Float.NaN)

      var ndx: Int = 0
      method match {
      case Mode => mode(tile._2, result, nodatas)
      case Median => median(tile._2, result, nodatas)
      case StdDev => stddev(tile._2, result, nodatas)
      case _ =>
        tile._2.foreach(wr => {

          val modebuilder = Array.newBuilder[Double]
          if (wr != null && wr.nonEmpty) {
            val raster = RasterWritable.toRaster(wr.asInstanceOf[Seq[RasterWritable]].head)

            method match {
            case Count => count(raster, result, nodatas(ndx))
            case Max => max(raster, result, nodatas(ndx))
            case Mean => mean(raster, result, nodatas(ndx))
            case Min => min(raster, result, nodatas(ndx))
            case Sum => sum(raster, result, nodatas(ndx))

            }
          }
          ndx += 1
        })
      }

      (tile._1, method match {
      case Mean =>
        val mean = RasterUtils.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT, Float.NaN)
        for (y <- 0 until result.getHeight) {
          for (x <- 0 until result.getWidth) {
            val total = result.getSampleDouble(x, y, 0)
            if (RasterMapOp.isNotNodata(total, Float.NaN)) {
              mean.setSample(x, y, 0, total / result.getSampleDouble(x, y, 1))
            }
          }
        }
        RasterWritable.toWritable(mean)
      case _ => RasterWritable.toWritable(result)
      })

    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, Float.NaN, calcStats = false))

    true
  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = {
    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit =
  {
    method = in.readUTF()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(method)
  }

}
