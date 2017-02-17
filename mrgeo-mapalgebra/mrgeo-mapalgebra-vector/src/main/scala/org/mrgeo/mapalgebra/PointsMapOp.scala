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

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.{Geometry, GeometryFactory}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.vector.VectorMapOp

import scala.collection.mutable.ListBuffer

object PointsMapOp {
  def apply(coords:Array[Double]) = {
    new PointsMapOp(coords)
  }

  def apply(mapop:MapOp):Option[PointsMapOp] =
    mapop match {
      case pmo:PointsMapOp => Some(pmo)
      case _ => None
    }
}

class PointsMapOp extends VectorMapOp with Externalizable {
  var vectorrdd:Option[VectorRDD] = None
  var srcCoordinates:Option[Array[Double]] = None

  override def execute(context:SparkContext):Boolean = {
    true
  }

  def getCoordCount():Int = {
    srcCoordinates match {
      case Some(coords) => {
        coords.length
      }
      case None => -1
    }
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {
    val coordCount = in.readInt()
    srcCoordinates = if (coordCount < 0) {
      None
    }
    else {
      val coords = Array.ofDim[Double](coordCount)
      var i:Int = 0
      while (i < coordCount) {
        coords(i) = in.readDouble()
        i += 1
      }
      Some(coords)
    }
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    srcCoordinates match {
      case Some(coords) => {
        out.writeInt(coords.length)
        coords.foreach(c => out.writeDouble(c))
      }
      case None => {
        out.writeInt(-1)
      }
    }
  }

  override def rdd():Option[VectorRDD] = {
    load()
    vectorrdd
  }

  private[mapalgebra] def this(coords:Array[Double]) = {
    this()

    this.srcCoordinates = Some(coords)
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()
    if (node.getNumChildren % 2 != 0) {
      throw new ParserException(
        "points takes a list of coordinates \"lon, lat, lon, lat, ...\"")
    }

    val numCoords = node.getNumChildren
    val coords = Array.ofDim[Double](numCoords)
    for (i <- 0 until numCoords) {
      coords(i) = MapOp.decodeDouble(node.getChild(i))
          .getOrElse(throw new ParserException("Invalid coordinate " + node.getChild(i).getName))
    }
    srcCoordinates = Some(coords)
  }

  private def load():Unit = {
    if (vectorrdd.isEmpty) {
      val pointsrdd = srcCoordinates match {
        case Some(coords) => {
          // Convert the array of lon/let pairs to a VectorRDD
          var recordData = new ListBuffer[(FeatureIdWritable, Geometry)]()
          for (i <- 0 until coords.length by 2) {
            val geom = GeometryFactory.createPoint(coords(i).toFloat, coords(i + 1).toFloat)
            recordData += ((new FeatureIdWritable(i / 2), geom))
          }
          VectorRDD(context.parallelize(recordData))
        }
        case None => throw new IOException("Invalid points input")
      }
      vectorrdd = Some(pointsrdd)
    }
  }
}
