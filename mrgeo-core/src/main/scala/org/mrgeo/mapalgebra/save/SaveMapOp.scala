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

package org.mrgeo.mapalgebra.save

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{RasterMapOp, SaveRasterMapOp}
import org.mrgeo.mapalgebra.vector.{SaveVectorMapOp, VectorMapOp}
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object SaveMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("save")
  }

  @SuppressFBWarnings(value = Array("BC_UNCONFIRMED_CAST"), justification = "Scala generated code")
  def create(mapop:MapOp, name:String, publishImage:Boolean = false):MapOp = {
    mapop match {
    case raster: RasterMapOp => new SaveRasterMapOp (Some(raster), name, publishImage)
    case vector: VectorMapOp => new SaveVectorMapOp (Some(vector), name)
    case _ => throw new ParserException("MapOp must be a RasterMapOp or VectorMapOp")
    }
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp = {

    if (node.getNumChildren < 2) {
      throw new ParserException(node.getName + " takes 2 arguments")
    }

    // are we a raster, if so, create a SaveRasterMapOp
    try {
      val raster = RasterMapOp.decodeToRaster(node.getChild(0), variables)
      new SaveRasterMapOp(node, variables)
    }
    catch {
      case pe: ParserException => {
        val vector = VectorMapOp.decodeToVector(node.getChild(0), variables)
        new SaveVectorMapOp(node, variables)
      }
    }
  }
}

// Dummy class definition to allow the python reflection to find the SaveMapOp
abstract class SaveMapOp extends MapOp {
}

