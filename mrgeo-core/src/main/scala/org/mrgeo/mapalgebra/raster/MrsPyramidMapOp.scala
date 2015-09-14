package org.mrgeo.mapalgebra.raster

import java.io.IOException

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.spark.job.JobArguments
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
  private var rasterrdd:RasterRDD = null

  def rdd(zoom:Int):Option[RasterRDD]  = {
    load(zoom)
    Some(rasterrdd)
  }

  def rdd():Option[RasterRDD] = {
    load()
    Some(rasterrdd)
  }

  private def load(zoom:Int = -1)  = {

    if (rasterrdd == null) {
      if (context == null) {
        throw new IOException("Error creating RasterRDD, can not create an RDD without a SparkContext")
      }

      if (zoom <= 0) {
        metadata(dataprovider.getMetadataReader.read())
        rasterrdd = SparkUtils.loadMrsPyramid(dataprovider, super.metadata().get.getMaxZoomLevel, context())
      }
      else {
        val data = SparkUtils.loadMrsPyramidAndMetadata(dataprovider, zoom, context())

        rasterrdd = data._1
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
