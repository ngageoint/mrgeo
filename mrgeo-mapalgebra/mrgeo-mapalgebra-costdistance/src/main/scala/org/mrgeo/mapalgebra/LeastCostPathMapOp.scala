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

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.GeometryFactory
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp

object LeastCostPathMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("leastCostPath", "lcp")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new LeastCostPathMapOp(node, variables)
}

class LeastCostPathMapOp extends VectorMapOp with Externalizable
{
  var costDistanceMapOp: Option[RasterMapOp] = None
  var costDistanceMetadata: MrsImagePyramidMetadata = null;
  var pointsMapOp: Option[VectorMapOp] = None
  var zoom: Int = -1
  var vectorrdd: Option[VectorRDD] = None

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()
    if (node.getNumChildren != 2 && node.getNumChildren != 3)
    {
      throw new ParserException(
        "LeastCostPath takes the following arguments ([cost zoom level], cost raster, destination points")
    }

    var nodeIndex: Int = 0
    if (node.getNumChildren == 3)
    {
      zoom = MapOp.decodeInt(node.getChild(nodeIndex), variables).getOrElse(
        throw new ParserException("Invalid zoom specified for least cost path: " +
          MapOp.decodeString(node.getChild(nodeIndex), variables))
      )
      nodeIndex += 1
    }
    costDistanceMapOp = RasterMapOp.decodeToRaster(node.getChild(nodeIndex), variables)
    nodeIndex += 1
    pointsMapOp = VectorMapOp.decodeToVector(node.getChild(nodeIndex), variables)
  }

  override def registerClasses(): Array[Class[_]] = {
    GeometryFactory.getClasses ++ Array[Class[_]](classOf[FeatureIdWritable])
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
//    conf.set("spark.kryo.registrationRequired", "true")
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {
    var destPoints: String = null
    costDistanceMetadata =
      costDistanceMapOp.getOrElse(throw new IOException("Invalid cost distance input")).
        metadata().getOrElse(throw new IOException("Missing metadata for cost distance input"))
    if (zoom < 0)
    {
      zoom = costDistanceMetadata.getMaxZoomLevel()
    }
    //TODO: Need to instantiate and run LeastCostPathCalculator here
    // It currently writes the output tsv file directly. That should ideally
    // be done by the VectorDataProvider, and the LCP calculator (and this map op)
    // should only create a VectorRDD
    val cdrdd = costDistanceMapOp.getOrElse(throw new IOException("Invalid cost distance input"))
      .rdd(zoom).getOrElse(throw new IOException("Invalid RDD for cost distance input"))
    val destrdd = pointsMapOp.getOrElse(throw new IOException("Invalid points input"))
      .rdd().getOrElse(throw new IOException("Invalid RDD for points input"))
    vectorrdd = Some(LeastCostPathCalculator.run(cdrdd, costDistanceMetadata, zoom, destrdd, context))
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    zoom = in.readInt()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(zoom)
  }

  override def rdd(): Option[VectorRDD] = vectorrdd
}