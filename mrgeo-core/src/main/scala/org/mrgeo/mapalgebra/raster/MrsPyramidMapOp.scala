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
  private var rasterRDD:RasterRDD = null
  private var zoomForRDD: Int = -1

  override def rdd(zoom:Int):Option[RasterRDD]  = {
    load(zoom)
    Some(rasterRDD)
  }

  def rdd():Option[RasterRDD] = {
    load()
    Some(rasterRDD)
  }

  private def load(zoom:Int = -1)  = {

    if (rasterRDD == null || zoom != zoomForRDD) {
      if (context == null) {
        throw new IOException("Error creating RasterRDD, can not create an RDD without a SparkContext")
      }

      if (zoom <= 0) {
        metadata(dataprovider.getMetadataReader.read())
        val maxZoom = super.metadata().get.getMaxZoomLevel
        rasterRDD = SparkUtils.loadMrsPyramid(dataprovider, maxZoom, context())
        zoomForRDD = maxZoom
      }
      else {
        val data = SparkUtils.loadMrsPyramidAndMetadata(dataprovider, zoom, context())

        rasterRDD = data._1
        zoomForRDD = zoom
        metadata(data._2)
      }
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
