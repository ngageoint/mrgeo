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

package org.mrgeo.mapalgebra.save

import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{RasterMapOp, SaveRasterMapOp}
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapalgebra.{MapOp, MapOpRegistrar}

object SaveMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("save")
  }
  override def registerWithParams: Array[String] = {
    Array[String]("save(MapOp, String)")
  }

  def create(mapop:MapOp, name:String):MapOp = {
    mapop match {
    case raster:RasterMapOp => new SaveRasterMapOp(Some(raster), name)
    // TODO:  Uncomment this!
    //case vector:VectorMapOp => SaveVectorMapOp(vector, name)
    case _ => throw new ParserException("MapOp must be a RasterMapOp or VectorMapOp")
    }
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp = {

    if (node.getNumChildren != 2) {
      throw new ParserException(node.getName + " takes 2 arguments")
    }

    // are we a raster, if so, create a SaveRasterMapOp
    try {
      val raster = RasterMapOp.decodeToRaster(node.getChild(0), variables)
      new SaveRasterMapOp(node, variables)
    }
    catch {
      case pe: ParserException =>
        throw pe
      // TODO:  Uncomment this!
      //        val vector = VectorMapOp.decodeToRaster(node.getChild(0), variables)
      //        new SaveVectorMapOp(node, true, variables, protectionLevel)
      //      }

    }
  }
}

// Dummy class definition to allow the python reflection to find the SaveMapOp
abstract class SaveMapOp extends MapOp {
}

