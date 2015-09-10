package org.mrgeo.mapalgebra.raster

import java.io.IOException

import org.apache.spark.SparkContext
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.utils.SparkUtils

object RasterMapOp {
  def apply(dataprovider: MrsImageDataProvider) = {
    new RasterMapOp(dataprovider)
  }

  def apply(mapop:MapOp):Option[RasterMapOp] = {
    mapop match {
    case rmo:RasterMapOp => Option(rmo)
    case _ => None
    }
  }
}

class RasterMapOp private[raster] (dataprovider: MrsImageDataProvider, val context:SparkContext = null) extends MapOp {
  private var rasterrdd:RasterRDD = null

  def rdd(zoom:Int):RasterRDD  = {
    if (context != null) {
      throw new IOException("Error creating RasterRDD, can not create an RDD without a SparkContext")
    }
    if (rasterrdd == null) {
      rasterrdd = SparkUtils.loadMrsPyramid(dataprovider, zoom, context)
    }

    rasterrdd
  }

  def rdd():RasterRDD = {
    val metadata = dataprovider.getMetadataReader.read()

    rdd(metadata.getMaxZoomLevel)
  }

  // nothing to do.  Really shouldn't be here...
  override def execute(context: SparkContext): Boolean = {
    true
  }
}
