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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.GeometryFactory
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp

object LeastCostPathMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("leastCostPath", "lcp")
  }

  def create(pointsMapOp: VectorMapOp, raster:RasterMapOp): MapOp = {

    new LeastCostPathMapOp(pointsMapOp, raster)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new LeastCostPathMapOp(node, variables)
}

class LeastCostPathMapOp extends VectorMapOp with Externalizable
{
  var costDistanceMapOp: Option[RasterMapOp] = None
  var pointsMapOp: Option[VectorMapOp] = None
  var vectorrdd: Option[VectorRDD] = None

  def this(pointsMapOp: VectorMapOp, costDistanceMapOp: RasterMapOp) = {
    this()

    this.costDistanceMapOp = Some(costDistanceMapOp)
    this.pointsMapOp = Some(pointsMapOp)
  }

  def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()
    if (node.getNumChildren != 2)
    {
      throw new ParserException(
        "LeastCostPath takes the following arguments (cost raster, destination points")
    }

    costDistanceMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    pointsMapOp = VectorMapOp.decodeToVector(node.getChild(1), variables)
  }

  override def registerClasses(): Array[Class[_]] = {
    GeometryFactory.getClasses ++ Array[Class[_]](classOf[FeatureIdWritable])
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {
    val destrdd = pointsMapOp match {
      case Some(pmo) => pmo.rdd().getOrElse(throw new IOException("Invalid RDD for points input"))
      case None => throw new IOException("Invalid points input")
    }

    vectorrdd = Some(LeastCostPathCalculator.run(costDistanceMapOp.get, destrdd, context))
    true
  }

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}

  override def rdd(): Option[VectorRDD] = vectorrdd
}