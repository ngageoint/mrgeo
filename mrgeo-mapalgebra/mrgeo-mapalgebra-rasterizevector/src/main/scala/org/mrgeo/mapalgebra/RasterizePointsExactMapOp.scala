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

import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.utils.tms.Bounds

object RasterizePointsExactMapOp extends MapOpRegistrar {
  override def register:Array[String] = {
    Array[String]("rasterizepointsexact")
  }

  def create(vector:VectorMapOp, aggregator:String, cellsize:String, w:Double, s:Double, e:Double, n:Double,
             column:String = null):RasterizePointsMapOp = {
    new RasterizePointsMapOp(Some(vector), aggregator, cellsize, column, new Bounds(w, n, e, s).toCommaString)
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new RasterizePointsMapOp(node, variables)
}

class RasterizePointsExactMapOp extends RasterizePointsMapOp {}