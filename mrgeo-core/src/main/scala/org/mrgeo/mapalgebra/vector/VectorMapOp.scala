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

package org.mrgeo.mapalgebra.vector

import org.apache.spark.SparkContext
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.VectorDataProvider
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser.{ParserException, ParserFunctionNode, ParserNode, ParserVariableNode}
import org.mrgeo.utils.SparkVectorUtils

object VectorMapOp {

  val EPSILON: Double = 1e-8

  def isNodata(value:Double, nodata:Double):Boolean = {
    if (nodata.isNaN) {
      value.isNaN
    }
    else {
      nodata == value
    }
  }
  def isNotNodata(value:Double, nodata:Double):Boolean = {
    if (nodata.isNaN) {
      !value.isNaN
    }
    else {
      nodata != value
    }
  }

  def nearZero(v:Double):Boolean = {
    if (v >= -EPSILON && v <= EPSILON) {
      true
    }
    else {
      false
    }
  }

  def decodeToVector(node:ParserNode, variables: String => Option[ParserNode]): Option[VectorMapOp] = {
    node match {
      case func: ParserFunctionNode => func.getMapOp match {
        case vector: VectorMapOp => Some(vector)
        case _ => throw new ParserException("Term \"" + node + "\" is not a vector input")
      }
      case variable: ParserVariableNode =>
        MapOp.decodeVariable(variable, variables).getOrElse(throw new ParserException("Variable \"" + node + " has not been assigned")) match {
          case func: ParserFunctionNode => func.getMapOp match {
            case raster: VectorMapOp => Some(raster)
            case _ => throw new ParserException("Term \"" + node + "\" is not a vector input")
          }
          case _ => throw new ParserException("Term \"" + node + "\" is not a vector input")
        }
      case _ => throw new ParserException("Term \"" + node + "\" is not a vector input")
    }
  }

}
abstract class VectorMapOp extends MapOp {

//  private var meta:VectorMetadata = null


  def rdd():Option[VectorRDD]

//  def metadata():Option[VectorMetadata] =  Option(meta)
//  def metadata(meta:VectorMetadata) = { this.meta = meta}

  def save(output: String, providerProperties:ProviderProperties, context:SparkContext) = {
    rdd() match {
      case Some(rdd) =>
        val provider: VectorDataProvider =
          DataProviderFactory.getVectorDataProvider(output, AccessMode.OVERWRITE, providerProperties)
        SparkVectorUtils.save(rdd, provider, context, providerProperties)
//        metadata() match {
//          case Some(metadata) =>
//            val meta = metadata
//            SparkVectorUtils.save(rdd, provider, metadata,
//              context.hadoopConfiguration, providerproperties =  providerProperties)
//          case _ =>
//        }
      case _ =>
    }
  }

}
