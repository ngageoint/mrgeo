/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.mapalgebra

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{MrsPyramidMapOp, RasterMapOp}
import org.mrgeo.utils.SparkUtils

import scala.collection.mutable
import scala.util.Sorting

object StatisticsMapOp extends MapOpRegistrar {
  private val Count = "count"
  private val Max = "max"
  private val Mean = "mean"
  private val Median = "median"
  private val Min = "min"
  private val Mode = "mode"
  private val StdDev = "stddev"
  private val Sum = "sum"

  private val methods = Array[String](Min, Max, Mean, Median, Mode, StdDev, Sum, Count)

  override def register:Array[String] = {
    Array[String]("statistics", "stats")
  }

  def create(first:RasterMapOp, method:String, rasters:Array[RasterMapOp]):MapOp = {
    val inputs:Seq[Either[Option[RasterMapOp], Option[String]]] =
      (List(first) ++ rasters.toList).flatMap(raster => {
        List(Left(Some(raster)))
      })

    if (method.isEmpty || !methods.exists(_.equals(method.toLowerCase))) {
      throw new ParserException("Invalid stastics method")
    }


    new StatisticsMapOp(inputs.toArray, method)
  }

  // When generating Python bindings for this map op, it cannot distinguish between
  // passing an array of RasterMapOp and and Array of String. So we are commenting
  // out the String version of the method. Python won't need this anyway because it
  // can do its own wildcarding based on names after calling MrGeo.listImages.
  //  def create(method:String, first:RasterMapOp, names:Array[String]):MapOp = {
  //    val inputs:Seq[Either[Option[RasterMapOp], Option[String]]] =
  //      List(Left(Some(first))) ++ (names.toList).flatMap(name => {
  //        List(Right(Some(name)))
  //      })
  //
  //    if (method.isEmpty || !methods.exists(_.equals(method.toLowerCase))) {
  //      throw new ParserException("Invalid stastics method")
  //    }
  //
  //
  //    new StatisticsMapOp(inputs.toArray, method)
  //  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new StatisticsMapOp(node, variables)
}

class StatisticsMapOp extends RasterMapOp with Externalizable {


  var providerProperties:ProviderProperties = null
  private var method:String = null
  private var inputs:Option[Array[Either[Option[RasterMapOp], Option[String]]]] = None
  private var rasterRDD:Option[RasterRDD] = None

  override def rdd():Option[RasterRDD] = rasterRDD

  private def getLayerMapOps(mapopbuilder: mutable.ArrayBuilder[RasterMapOp],
                             nodatabuilder: mutable.ArrayBuilder[Double],
                             context: SparkContext) =
  {
    var zoom: Option[Int] = None
    var tilesize = -1
    var layers:Option[Array[String]] = None
    if (inputs.isDefined) {
      inputs.get.foreach {
        case Left(left) =>
          left match {
            case Some(raster) =>
              mapopbuilder += raster

              val meta = raster.metadata() getOrElse
                (throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName))

              zoom match {
                case Some(z) => {
                  if (z != meta.getMaxZoomLevel) {
                    throw new IOException("Mismatched zoom levels in stats inputs")
                  }
                }
                case None => zoom = Some(meta.getMaxZoomLevel)
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
              val regex = str.replace("?", ".?").replace("*", ".*?")
              val hits = layers.getOrElse({
                layers = Some(DataProviderFactory.listImages(providerProperties))
                layers.get
              }).filter(_.matches(regex))

              hits.foreach(layer => {
                val dp = DataProviderFactory.getMrsImageDataProvider(layer, AccessMode.READ, providerProperties)

                val raster = MrsPyramidMapOp.apply(dp)
                raster.context(context)

                mapopbuilder += raster

                val meta = raster.metadata() getOrElse
                  (throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName))

                zoom match {
                  case Some(z) => {
                    if (z != meta.getMaxZoomLevel) {
                      throw new IOException("Mismatched zoom levels in stats inputs")
                    }
                  }
                  case None => zoom = Some(meta.getMaxZoomLevel)
                }
                if (tilesize < 0) {
                  tilesize = meta.getTilesize
                }

                nodatabuilder += meta.getDefaultValue(0)
              })
            case _ =>
          }
      }
    }
    zoom match {
      case Some(z) => (z, tilesize)
      case None => throw new IOException("No inputs to the stats operation")
    }
  }

  override def getZoomLevel(): Int = {
    val mapopbuilder = Array.newBuilder[RasterMapOp]
    val nodatabuilder = Array.newBuilder[Double]

    val (zoom, _) = getLayerMapOps(mapopbuilder, nodatabuilder, context())
    zoom
  }

  override def execute(context:SparkContext):Boolean = {

    val mapopbuilder = Array.newBuilder[RasterMapOp]
    val nodatabuilder = Array.newBuilder[Double]

    val (zoom, tilesize) = getLayerMapOps(mapopbuilder, nodatabuilder, context)
    val nodatas = nodatabuilder.result()

    val pyramids = mapopbuilder.result()
        .map(_.rdd(zoom) getOrElse (throw new IOException("Can't load RDD! Ouch! " + getClass.getName)))

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

      def count(raster:MrGeoRaster, result:MrGeoRaster, nodata:Double) = {
        var y:Int = 0
        while (y < raster.height) {
          var x:Int = 0
          while (x < raster.width()) {
            val px = raster.getPixelDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val cnt = result.getPixelDouble(x, y, 0)
              if (RasterMapOp.isNodata(cnt, Float.NaN)) {
                result.setPixel(x, y, 0, 1)
              }
              else {
                result.setPixel(x, y, 0, cnt + 1)
              }
            }
            x += 1
          }
          y += 1
        }
      }

      def min(raster:MrGeoRaster, result:MrGeoRaster, nodata:Double) = {
        var y:Int = 0
        while (y < raster.height()) {
          var x:Int = 0
          while (x < raster.width()) {
            val px = raster.getPixelDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getPixelDouble(x, y, 0)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setPixel(x, y, 0, px)
              }
              else {
                result.setPixel(x, y, 0, Math.min(src, px))
              }
            }
            x += 1
          }
          y += 1
        }
      }

      def max(raster:MrGeoRaster, result:MrGeoRaster, nodata:Double) = {
        var y:Int = 0
        while (y < raster.height()) {
          var x:Int = 0
          while (x < raster.width()) {
            val px = raster.getPixelDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getPixelDouble(x, y, 0)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setPixel(x, y, 0, px)
              }
              else {
                result.setPixel(x, y, 0, Math.max(src, px))
              }
            }
            x += 1
          }
          y += 1
        }
      }

      def mean(raster:MrGeoRaster, result:MrGeoRaster, nodata:Double) = {
        var y:Int = 0
        while (y < raster.height()) {
          var x:Int = 0
          while (x < raster.width()) {
            val px = raster.getPixelDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getPixelDouble(x, y, 0)
              val cnt = result.getPixelDouble(x, y, 1)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setPixel(x, y, 0, px)
                result.setPixel(x, y, 1, 1)
              }
              else {
                result.setPixel(x, y, 0, src + px)
                result.setPixel(x, y, 1, cnt + 1)
              }
            }
            x += 1
          }
          y += 1
        }
      }

      def sum(raster:MrGeoRaster, result:MrGeoRaster, nodata:Double) = {
        var y:Int = 0
        while (y < raster.height()) {
          var x:Int = 0
          while (x < raster.width()) {
            val px = raster.getPixelDouble(x, y, 0)

            if (RasterMapOp.isNotNodata(px, nodata)) {
              val src = result.getPixelDouble(x, y, 0)
              if (RasterMapOp.isNodata(src, Float.NaN)) {
                result.setPixel(x, y, 0, px)
              }
              else {
                result.setPixel(x, y, 0, src + px)
              }
            }
            x += 1
          }
          y += 1
        }
      }

      def mode(tiles:Array[Iterable[_]], result:MrGeoRaster, nodatas:Array[Double]) = {
        val rasterbuilder = Array.newBuilder[MrGeoRaster]
        tiles.foreach(wr => {
          if (wr != null && wr.nonEmpty) {
            rasterbuilder += RasterWritable.toMrGeoRaster(wr.asInstanceOf[Seq[RasterWritable]].head)
          }
          else {
            rasterbuilder += null
          }
        })

        val rasters = rasterbuilder.result()

        val valuebuilder = Array.newBuilder[Double]
        valuebuilder.sizeHint(rasters.length)
        var y:Int = 0
        while (y < result.height()) {
          var x:Int = 0
          while (x < result.width()) {
            valuebuilder.clear

            var ndx:Int = 0
            while (ndx < rasters.length) {
              if (rasters(ndx) != null) {
                val px = rasters(ndx).getPixelDouble(x, y, 0)
                if (RasterMapOp.isNotNodata(px, nodatas(ndx))) {
                  valuebuilder += px
                }
              }
              ndx += 1
            }

            val values = valuebuilder.result
            if (values.length > 0) {
              Sorting.quickSort(values)

              var mode = values(1)
              var modecount = 1

              var curval = mode
              var curcount = modecount

              var i:Int = 1
              while (i < values.length) {
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
                i += 1
              }
              if (curcount > modecount) {
                mode = curval
              }

              result.setPixel(x, y, 0, mode)
            }
            x += 1
          }
          y += 1
        }
      }

      def median(tiles:Array[Iterable[_]], result:MrGeoRaster, nodatas:Array[Double]) = {
        val rasterbuilder = Array.newBuilder[MrGeoRaster]
        tiles.foreach(wr => {
          if (wr != null && wr.nonEmpty) {
            rasterbuilder += RasterWritable.toMrGeoRaster(wr.asInstanceOf[Seq[RasterWritable]].head)
          }
          else {
            rasterbuilder += null
          }
        })

        val rasters = rasterbuilder.result()

        val valuebuilder = Array.newBuilder[Double]
        valuebuilder.sizeHint(rasters.length)
        var y:Int = 0
        while (y < result.height()) {
          var x:Int = 0
          while (x < result.width()) {
            valuebuilder.clear

            var ndx:Int = 0
            while (ndx < rasters.length) {
              if (rasters(ndx) != null) {
                val px = rasters(ndx).getPixelDouble(x, y, 0)
                if (RasterMapOp.isNotNodata(px, nodatas(ndx))) {
                  valuebuilder += px
                }
              }
              ndx += 1
            }

            val values = valuebuilder.result
            if (values.length > 0) {
              result.setPixel(x, y, 0, values.length / 2)
            }
            x += 1
          }
          y += 1
        }
      }

      def stddev(tiles:Array[Iterable[_]], result:MrGeoRaster, nodatas:Array[Double]) = {
        val rasterbuilder = Array.newBuilder[MrGeoRaster]
        tiles.foreach(wr => {
          if (wr != null && wr.nonEmpty) {
            rasterbuilder += RasterWritable.toMrGeoRaster(wr.asInstanceOf[Seq[RasterWritable]].head)
          }
          else {
            rasterbuilder += null
          }
        })

        val rasters = rasterbuilder.result()

        var y:Int = 0
        while (y < result.height()) {
          var x:Int = 0
          while (x < result.width()) {

            var sum1:Double = 0
            var sum2:Double = 0

            var cnt = 0
            var ndx:Int = 0
            while (ndx < rasters.length) {
              if (rasters(ndx) != null) {
                val px = rasters(ndx).getPixelDouble(x, y, 0)
                if (RasterMapOp.isNotNodata(px, nodatas(ndx))) {
                  sum1 += px
                  sum2 += Math.pow(px, 2)
                  cnt += 1
                }
              }
              ndx += 1
            }
            val stddev = Math.sqrt(cnt * sum2 - Math.pow(sum1, 2)) / cnt
            result.setPixel(x, y, 0, stddev)
            x += 1
          }
          y += 1
        }
      }

      val bands = method match {
        case StatisticsMapOp.Mean => 2
        case _ => 1
      }

      val result = MrGeoRaster.createEmptyRaster(tilesize, tilesize, bands, DataBuffer.TYPE_FLOAT, Float.NaN)

      var ndx:Int = 0
      method match {
        case StatisticsMapOp.Mode => mode(tile._2, result, nodatas)
        case StatisticsMapOp.Median => median(tile._2, result, nodatas)
        case StatisticsMapOp.StdDev => stddev(tile._2, result, nodatas)
        case _ =>
          tile._2.foreach(wr => {

            val modebuilder = Array.newBuilder[Double]
            if (wr != null && wr.nonEmpty) {
              val raster = RasterWritable.toMrGeoRaster(wr.asInstanceOf[Seq[RasterWritable]].head)

              method match {
                case StatisticsMapOp.Count => count(raster, result, nodatas(ndx))
                case StatisticsMapOp.Max => max(raster, result, nodatas(ndx))
                case StatisticsMapOp.Mean => mean(raster, result, nodatas(ndx))
                case StatisticsMapOp.Min => min(raster, result, nodatas(ndx))
                case StatisticsMapOp.Sum => sum(raster, result, nodatas(ndx))

              }
            }
            ndx += 1
          })
      }

      (tile._1, method match {
        case StatisticsMapOp.Mean =>
          val mean = MrGeoRaster.createEmptyRaster(tilesize, tilesize, 1, DataBuffer.TYPE_FLOAT, Float.NaN)
          var y:Int = 0
          while (y < result.height()) {
            var x:Int = 0
            while (x < result.width()) {
              val total = result.getPixelDouble(x, y, 0)
              if (RasterMapOp.isNotNodata(total, Float.NaN)) {
                mean.setPixel(x, y, 0, total / result.getPixelDouble(x, y, 1))
              }
              x += 1
            }
            y += 1
          }
          RasterWritable.toWritable(mean)
        case _ => RasterWritable.toWritable(result)
      })

    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, Float.NaN,
      bounds = null, calcStats = false))

    true
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {
    method = in.readUTF()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeUTF(method)
  }

  private[mapalgebra] def this(inputs:Array[Either[Option[RasterMapOp], Option[String]]], method:String) = {
    this()
    this.inputs = Some(inputs)
    this.method = method
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 2) {
      throw new ParserException("Usage: statistics(\"method\", raster1, raster2, ...)")
    }

    method = {
      val m = MapOp.decodeString(node.getChild(0), variables)

      if (m.isEmpty || !StatisticsMapOp.methods.exists(_.equals(m.get.toLowerCase))) {
        throw new ParserException("Invalid stastics method")
      }
      m.get.toLowerCase
    }

    val inputbuilder = Array.newBuilder[Either[Option[RasterMapOp], Option[String]]]
    var i:Int = 1
    while (i < node.getNumChildren) {
      try {
        val raster = RasterMapOp.decodeToRaster(node.getChild(i), variables)
        inputbuilder += Left(raster)
      }
      catch {
        case e:ParserException =>
          try {
            val str = MapOp.decodeString(node.getChild(i), variables)
            inputbuilder += Right(str)
          }
          catch {
            case pe:ParserException =>
              throw new ParserException(node.getChild(i).getName + " is not a string or raster")
          }
      }
      i += 1
    }
    inputs = if (inputbuilder.result().length > 0) {
      Some(inputbuilder.result())
    }
    else {
      None
    }
  }

}
