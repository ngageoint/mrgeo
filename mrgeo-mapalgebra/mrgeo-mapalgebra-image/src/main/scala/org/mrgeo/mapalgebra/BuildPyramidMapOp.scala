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

import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.aggregators.{AggregatorRegistry, MeanAggregator}
import org.mrgeo.buildpyramid.BuildPyramid
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.{DataProviderFactory, DataProviderNotFound, ProviderProperties}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.pyramid.MrsPyramidMetadata
import org.mrgeo.pyramid.MrsPyramidMetadata.Classification

object BuildPyramidMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("buildpyramid", "bp")
  }

  def create(raster: RasterMapOp, aggregator:String = "MEAN") =
    new BuildPyramidMapOp(Some(raster), Some(aggregator))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new BuildPyramidMapOp(node, true, variables)
}

class BuildPyramidMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  private var aggregator: Option[String] = None

  var providerProperties:ProviderProperties = null

  private[mapalgebra] def this(raster:Option[RasterMapOp], aggregator:Option[String]) = {
    this()

    inputMapOp = raster
    this.aggregator = aggregator
  }

  private[mapalgebra] def this(node: ParserNode, isSlope: Boolean, variables: String => Option[ParserNode]) = {
    this()

    if ((node.getNumChildren < 1) || (node.getNumChildren > 2)) {
      throw new ParserException(
        "buildpyramid usage: buildpyramid(source raster, [aggregation type])")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    if (node.getNumChildren == 2) {
      aggregator = Some(MapOp.decodeString(node.getChild(1)) match {
      case Some(s) =>
        val clazz = AggregatorRegistry.aggregatorRegistry.get(s.toUpperCase)
        if (clazz != null) {
          s.toUpperCase
        }
        else {
          throw new ParserException("Invalid aggregator " + s)
        }
      case _ => throw new ParserException("Can't decode string")
      })
    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean = {
    val input: RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    rasterRDD = input.rdd()
    val meta = new MrsPyramidMetadata(input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName)))

    // Need to see if this is a saved pyramid.  If it is, we can buildpyramids, otherwise it is
    // a temporary RDD and we need to error out
    if (meta.getPyramid == null || meta.getPyramid.length == 0) {
      throw new DataProviderNotFound("Pyramid must exist (saved) before buildPyramid)! (unnamed layer)")
    }
    try {
      val dp = DataProviderFactory.getMrsImageDataProvider(meta.getPyramid, AccessMode.READ, providerProperties)

      if (aggregator.isDefined) {
        meta.setResamplingMethod(aggregator.get)
      }

      metadata(meta)

      val agg = {
        val clazz = AggregatorRegistry.aggregatorRegistry.get(meta.getResamplingMethod)

        if (clazz != null) {
          clazz.newInstance()
        }
        else {
          // if there is no aggregator and we are continuous, use the mean aggregator
          meta.getClassification match {
          case Classification.Continuous => new MeanAggregator
          case _ =>
            throw new IOException("Invalid aggregator " + meta.getResamplingMethod)
          }
        }
      }

      BuildPyramid.build(meta.getPyramid, agg, context, providerProperties)
      true
    }
    catch {
      case dpnf:DataProviderNotFound => throw new DataProviderNotFound("Pyramid must exist (saved) before buildPyramid)! (" + meta.getPyramid + ")", dpnf)
    }
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {

    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))
    true
  }
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

}
