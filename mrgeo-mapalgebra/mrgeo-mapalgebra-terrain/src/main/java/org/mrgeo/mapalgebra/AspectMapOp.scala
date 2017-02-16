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

import java.io.{ObjectInput, ObjectOutput}

import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.FloatUtils

object AspectMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("aspect")
  }

  def create(raster:RasterMapOp, units:String = "rad", flatValue:Double = -1.0):MapOp = {
    new AspectMapOp(Some(raster), units, flatValue)
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new AspectMapOp(node, variables)
}

class AspectMapOp extends SlopeAspectMapOp {
  final val TWO_PI:Double = 2 * Math.PI
  final val THREE_PI_OVER_2:Double = (3.0 * Math.PI) / 2.0

  var flatValue:Double = -1.0

  override def computeTheta(normal:(Double, Double, Double)):Double = {
    // if the z component of the normal is 1.0, the cell is flat, so the aspect is undefined.
    // For now, we'llset it to 0.0, but another value could be more appropriate.
    if (FloatUtils.isEqual(normal._3, 1.0)) {
      flatValue
    }
    else {
      // change from (-Pi to Pi) to ( [0 to 2Pi) ), make 0 deg north (+ 3pi/2)
      // convert to clockwise
      val t = TWO_PI - (Math.atan2(normal._2, normal._1) + THREE_PI_OVER_2) % TWO_PI
      if (FloatUtils.isEqual(t, TWO_PI)) {
        flatValue
      }
      else {
        t
      }
    }
  }

  override def readExternal(in:ObjectInput):Unit = {
    super.readExternal(in)
    flatValue = in.readDouble()
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    super.writeExternal(out)
    out.writeDouble(flatValue)
  }

  private[mapalgebra] def this(inputMapOp:Option[RasterMapOp], units:String, flatValue:Double) = {
    this()

    initialize(inputMapOp, units)
    this.flatValue = flatValue
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires at least one argument")
    }
    else if (node.getNumChildren > 4) {
      throw new ParserException(node.getName + " takes no more than four arguments")
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
    if (node.getNumChildren >= 3) {
      flatValue = MapOp.decodeDouble(node.getChild(2)) match {
        case Some(d) => d
        case _ => throw new ParserException("Error decoding double from " + node.getChild(2).getName)
      }
    }
  }
}
