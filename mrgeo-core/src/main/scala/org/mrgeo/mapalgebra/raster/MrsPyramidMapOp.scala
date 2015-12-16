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

package org.mrgeo.mapalgebra.raster

import java.io.IOException

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.utils.SparkUtils

object MrsPyramidMapOp {
  def apply(dataprovider: MrsImageDataProvider) = {
    new MrsPyramidMapOp(dataprovider)
  }

  def apply(mapop:MapOp):Option[MrsPyramidMapOp] = {
    mapop match {
    case rmo:MrsPyramidMapOp => Some(rmo)
    case _ => None
    }
  }
}

class MrsPyramidMapOp private[raster] (dataprovider: MrsImageDataProvider) extends RasterMapOp {
  private var rasterRDD:Option[RasterRDD] = None
  private var zoomForRDD: Option[Int] = None
  private var maxZoomForRDD: Option[Int] = None

  override def rdd(zoom:Int):Option[RasterRDD]  = {
    load(zoom)
    rasterRDD
  }

  def rdd():Option[RasterRDD] = {
    load()
    rasterRDD
  }

  private def load(zoom:Int = -1)  = {

    if (context == null) {
      throw new IOException("Error creating RasterRDD, can not create an RDD without a SparkContext")
    }

    // If we haven't loaded anything yet
    if (rasterRDD.isEmpty || zoomForRDD.isEmpty || maxZoomForRDD.isEmpty) {
      val data = if (zoom <= 0) {
        SparkUtils.loadMrsPyramidAndMetadata(dataprovider, context())
      }
      else {
        SparkUtils.loadMrsPyramidAndMetadata(dataprovider, zoom, context())
      }

      metadata(data._2)
      rasterRDD = Some(data._1)
      maxZoomForRDD = Some(data._2.getMaxZoomLevel)
      zoomForRDD = Some(if (zoom > 0) zoom else data._2.getMaxZoomLevel)
    }
    // if we sent in a zoom and it is different than the current loaded one
    else if (zoom > 0 && zoom != zoomForRDD.get) {
      rasterRDD = Some(SparkUtils.loadMrsPyramid(dataprovider, zoom, context()))
      zoomForRDD = Some(zoom)
    }
    // if we didn't pass a zoom and it is not max zoom
    else if (zoomForRDD.get != maxZoomForRDD.get) {
      rasterRDD = Some(SparkUtils.loadMrsPyramid(dataprovider, maxZoomForRDD.get, context()))
      zoomForRDD = Some(maxZoomForRDD.get)
    }

  }

  override def metadata():Option[MrsImagePyramidMetadata] =  {
    load()
    super.metadata()
  }

  // nothing to do here...
  override def setup(job: JobArguments, conf: SparkConf): Boolean = true
  override def execute(context: SparkContext): Boolean = true
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

}
