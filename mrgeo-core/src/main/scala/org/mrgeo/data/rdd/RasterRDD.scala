package org.mrgeo.data.rdd

import org.apache.spark.rdd.RDD
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable

object RasterRDD {
  def apply(parent: MrGeoRDD[TileIdWritable, RasterWritable]): RasterRDD = {
    new RasterRDD(parent)
  }
  def apply(parent: RDD[(TileIdWritable, RasterWritable)]): RasterRDD = {
    new RasterRDD(parent)
  }
}

class RasterRDD(parent: RDD[(TileIdWritable, RasterWritable)]) extends MrGeoRDD[TileIdWritable, RasterWritable](parent) {

}
