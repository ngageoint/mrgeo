package org.mrgeo.ingest

import java.io.Externalizable

import org.apache.hadoop.mapreduce.{InputFormat, Job}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.ingest.ImageIngestDataProvider
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.utils.HadoopUtils

class IngestLocalSpark extends IngestImageSpark with Externalizable {

  override def execute(context: SparkContext): Boolean = {

    val input = inputs(0) // there is only 1 input here...
    val provider: ImageIngestDataProvider = DataProviderFactory.getImageIngestDataProvider(input, AccessMode.READ)

    val format = provider.getTiledInputFormat
    val inputFormatClass: Class[InputFormat[TileIdWritable, RasterWritable]] =
      format.getInputFormat(input).getClass.asInstanceOf[Class[InputFormat[TileIdWritable, RasterWritable]]]

    val job: Job = new Job(HadoopUtils.createConfiguration())

    format.setupJob(job, providerproperties)

    val rawtiles = context.newAPIHadoopRDD(job.getConfiguration,
      inputFormatClass,
      classOf[TileIdWritable],
      classOf[RasterWritable])

    val tiles = rawtiles.map(tile => {
      (new TileIdWritable(tile._1), new RasterWritable(tile._2))
    }).reduceByKey((r1, r2) => {
       mergeTile(r1, r2)
    })

    saveRDD(tiles)

    true
  }

}