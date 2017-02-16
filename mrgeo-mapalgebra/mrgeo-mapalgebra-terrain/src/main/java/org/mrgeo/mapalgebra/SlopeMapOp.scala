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

import javax.vecmath.Vector3d

import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp

object SlopeMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("slope")
  }

  def create(raster:RasterMapOp, units:String = "rad"):MapOp = {
    new SlopeMapOp(Some(raster), units)
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new SlopeMapOp(node, variables)
}

class SlopeMapOp extends SlopeAspectMapOp {
  val up = new Vector3d(0, 0, 1.0) // z (up) direction

  override def computeTheta(normal:(Double, Double, Double)):Double = {
    Math.acos(up.dot(new Vector3d(normal._1, normal._2, normal._3)))
  }

  private[mapalgebra] def this(inputMapOp:Option[RasterMapOp], units:String) = {
    this()

    initialize(inputMapOp, units)
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires at least one argument")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " takes only one or two arguments")
    }

    val inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    var units:String = "rad"
    if (node.getNumChildren >= 2) {
      units = MapOp.decodeString(node.getChild(1)) match {
        case Some(s) => s
        case _ => throw new ParserException("Error decoding string")
      }
    }
    initialize(inputMapOp, units)
  }
}
