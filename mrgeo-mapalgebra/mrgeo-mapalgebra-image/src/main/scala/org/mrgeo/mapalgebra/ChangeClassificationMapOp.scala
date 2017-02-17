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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.aggregators.AggregatorRegistry
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsPyramidMetadata.Classification
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp

object ChangeClassificationMapOp extends MapOpRegistrar {

  override def register:Array[String] = {
    Array[String]("changeClassification")
  }

  def create(raster:RasterMapOp, classification:String):MapOp =
    new ChangeClassificationMapOp(Some(raster), Some(classification))

  def create(raster:RasterMapOp, classification:String, aggregator:String):MapOp =
    new ChangeClassificationMapOp(Some(raster), Some(classification), Some(aggregator))

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new ChangeClassificationMapOp(node, variables)
}

@SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
class ChangeClassificationMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD:Option[RasterRDD] = None

  private var inputMapOp:Option[RasterMapOp] = None
  private var classification:Option[Classification] = None
  private var aggregator:Option[String] = None

  override def rdd():Option[RasterRDD] = rasterRDD

  override def setup(job:JobArguments, conf:SparkConf):Boolean = true

  override def execute(context:SparkContext):Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse
               (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    rasterRDD = Some(
      input.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName)))

    meta.setClassification(
      classification.getOrElse(throw new IOException("Can't get classification! Ouch! " + input.getClass.getName)))

    if (aggregator.isDefined) {
      meta.setResamplingMethod(aggregator.get)
    }

    metadata(meta)
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}

  private[mapalgebra] def this(raster:Option[RasterMapOp], classification:Option[String],
                               aggregator:Option[String] = None) = {
    this()
    inputMapOp = raster

    this.aggregator = aggregator match {
      case Some(s) =>
        val clazz = AggregatorRegistry.aggregatorRegistry.get(s.toUpperCase)
        if (clazz != null) {
          Some(s.toUpperCase)
        }
        else {
          throw new ParserException("Invalid aggregator " + s)
        }
      case _ => None
    }

    this.classification = Some(classification match {
      case Some(s) => s.toLowerCase match {
        case "categorical" => Classification.Categorical
        case _ => Classification.Continuous
      }
      case _ => throw new ParserException("Can't decode string")
    })

  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if ((node.getNumChildren < 2) || (node.getNumChildren > 3)) {
      throw new ParserException(
        "ChangeClassificationMapOp usage: changeClassification(source raster, classification, [aggregation type])")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    classification = Some(MapOp.decodeString(node.getChild(1), variables) match {
      case Some(s) => s.toLowerCase match {
        case "categorical" => Classification.Categorical
        case _ => Classification.Continuous
      }
      case _ => throw new ParserException("Can't decode string")
    })


    if (node.getNumChildren == 3) {
      aggregator = Some(MapOp.decodeString(node.getChild(2)) match {
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

}
