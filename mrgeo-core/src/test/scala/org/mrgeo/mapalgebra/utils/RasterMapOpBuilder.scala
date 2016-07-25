package org.mrgeo.mapalgebra.utils

import java.awt.image.Raster

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.Bounds

/**
  * Created by ericwood on 7/1/16.
  */
object RasterMapOpBuilder {
  def apply() = new RasterMapOpBuilder(createLocalContext)
  def apply(context: SparkContext) = new RasterMapOpBuilder(context)

  private def createLocalContext: SparkContext = {
    val conf = new SparkConf()
        .setMaster("local")
        .setAppName("MrGeo Local Mapalgebra Test")
        .set("spark.ui.enabled","false")
    new SparkContext(conf)
  }
}

/**
  * This class is used to build a RasterMapOp given a Raster in order to serve as input data for a map operation
  */
class RasterMapOpBuilder private (var context:SparkContext, numPartitions: Int = 1) {

  private var rasterMap = Map[TileIdWritable, RasterWritable]()
  private var zoomLevel: Int = 1
  private var tileSize: Int = 512
  private var imageNodata: Array[Double] = Array()
  private var bounds: Bounds = _

  def raster(tileId: Long, raster: Raster): RasterMapOpBuilder = {
    rasterMap = rasterMap + (new TileIdWritable(tileId) -> RasterWritable.toWritable(raster))
    this
  }

  def zoomLevel(zoomLevel: Int): RasterMapOpBuilder = {
    this.zoomLevel = zoomLevel
    this
  }

  def tileSize(tileSize: Int): RasterMapOpBuilder = {
    this.tileSize = tileSize
    this
  }

  def imageNoData(imageNoData: Array[Double]): RasterMapOpBuilder = {
    this.imageNodata = imageNoData
    this
  }

  def bounds(bounds: Bounds): RasterMapOpBuilder = {
    this.bounds = bounds
    this
  }

  def build:org.mrgeo.mapalgebra.raster.RasterMapOp = {
    val rasterMapOp = new RasterMapOp(context.makeRDD(this.rasterMap.toSeq, this.numPartitions))
    val metadata = new MrsPyramidMetadata()
    metadata.setMaxZoomLevel(zoomLevel)
    metadata.setTilesize(tileSize)
    metadata.setDefaultValues(imageNodata)
    metadata.setBounds(bounds)
    rasterMapOp.metadata(metadata)
    rasterMapOp
  }

  private class RasterMapOp(wrappedRDD: RDD[(TileIdWritable, RasterWritable)]) extends org.mrgeo.mapalgebra.raster.RasterMapOp {
    override def rdd() = {Some(RasterRDD(this.wrappedRDD))}

    // Noop these since this op exists only to wrap data, not to manipulate it.
    def execute(context: org.apache.spark.SparkContext): Boolean = ???
    def setup(job: org.mrgeo.job.JobArguments,conf: org.apache.spark.SparkConf): Boolean = ???
    def teardown(job: org.mrgeo.job.JobArguments,conf: org.apache.spark.SparkConf): Boolean = ???
  }
}
