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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.{Bounds, SparkUtils, TMSUtils}

import scala.collection.mutable

object FillMapOp extends MapOpRegistrar {
  private[mapalgebra] val Fill = "fill"
  private[mapalgebra] val FillBounds = "fillbounds"

  override def register: Array[String] = {
    Array[String](Fill, FillBounds)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new FillMapOp(node, variables)
}

class FillMapOp extends RasterMapOp with Externalizable {
  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  private var fillMapOp:Option[RasterMapOp] = None
  private var constFill:Option[Double] = None
  private var bounds:Option[Bounds] = None

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    // get values unique for each function
    node.getName match {
    case FillMapOp.Fill =>
      if (node.getNumChildren != 2) {
        throw new ParserException("Usage: fill(raster, fill value)")
      }
    case FillMapOp.FillBounds =>
      if (node.getNumChildren != 6) {
        throw new ParserException("Usage: fill(raster, fill value, w, s, n, e)")
      }

      bounds = Some(new Bounds(MapOp.decodeDouble(node.getChild(2), variables).get,
        MapOp.decodeDouble(node.getChild(3), variables).get,
        MapOp.decodeDouble(node.getChild(4), variables).get,
        MapOp.decodeDouble(node.getChild(5), variables).get))
    }

    // these are common between functions
    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    val childA = node.getChild(1)

    childA match {
    case const:ParserConstantNode => constFill = MapOp.decodeDouble(const)
    case func:ParserFunctionNode => fillMapOp = func.getMapOp match {
    case raster:RasterMapOp => Some(raster)
    case _ =>  throw new ParserException("First term \"" + childA + "\" is not a raster input")
    }
    case variable:ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const:ParserConstantNode => constFill = MapOp.decodeDouble(const)
      case func:ParserFunctionNode => fillMapOp = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("First term \"" + childA + "\" is not a raster input")
      }
      }
    }

  }


  override def rdd(): Option[RasterRDD] = rasterRDD
  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {

    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val nodata = meta.getDefaultValue(0)

    //rasterRDD = Some(RasterRDD(rdd.filter(tile => tile._1.get() % 2 == 0)))
    val tb = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(bounds match {
    case Some(b) => b
    case None => meta.getBounds
    }), zoom, meta.getTilesize)

    val tileBuilder = Array.newBuilder[(TileIdWritable, RasterWritable)]
    for (ty <- tb.s to tb.n) {
      for (tx <- tb.w to tb.e) {
        val id = TMSUtils.tileid(tx, ty, zoom)

        val tuple = (new TileIdWritable(id), new RasterWritable())
        tileBuilder += tuple
      }
    }

    val test = context.parallelize(tileBuilder.result())

    rasterRDD = Some(RasterRDD(constFill match {
    case Some(const) =>
      val src = RasterWritable.toRaster(rdd.first()._2)
      val constRaster = RasterWritable.toWritable(RasterUtils.createCompatibleEmptyRaster(src, const))

      val joined = new PairRDDFunctions(test).leftOuterJoin(rdd)
      joined.map(tile => {
        // if we have a tile, use it, otherwise (None case), make a new tile with the constant value
        tile._2._2 match {
        case Some(s) =>
          (tile._1, s)
        case None =>
          (tile._1, new RasterWritable(constRaster))
        }
      })
    case None =>
      val fill:RasterMapOp = fillMapOp getOrElse(throw new IOException("Input MapOp not valid!"))
      val fillrdd = fill.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

      val src = RasterWritable.toRaster(rdd.first()._2)
      val nodataRaster = RasterWritable.toWritable(RasterUtils.createCompatibleEmptyRaster(src, nodata))

      val joined = new PairRDDFunctions(test).cogroup(rdd, fillrdd)
      joined.map(tile => {

        // if the src tile is not empty, use it
        if (tile._2._2.nonEmpty) {
          (tile._1, tile._2._2.head)
        }
        // if the fill tile is not empty, use it
        else if (tile._2._3.nonEmpty) {
          (tile._1, tile._2._3.head)
        }
        else {
          //the src and fill tiles are emtpy, now what? (nodata?)
          (tile._1, new RasterWritable(nodataRaster))
        }
      })
    }))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, meta.getDefaultValues, calcStats = false))

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

}
