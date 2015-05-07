package org.mrgeo.ingest

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.spark.job.{JobArguments, MrGeoJob, MrGeoDriver}
import org.mrgeo.utils.Bounds

import scala.collection.JavaConversions._
import scala.collection.mutable


object IngestImageSpark extends MrGeoDriver with Externalizable {

  def ingest(inputs: Array[String], output: String,
      categorical: Boolean, conf: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Number, bands: Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: Properties):Boolean = {

    val args =  mutable.Map[String, String]()

    val in = inputs.mkString(",")

    val name = "IngestImage"

    args += "inputs" -> in
    args += "output" -> output
    args += "bounds" -> bounds.toDelimitedString
    args += "zoom" -> zoomlevel.toString
    args += "tilesize" -> tilesize.toString
    args += "nodata" -> nodata.toString
    args += "bands" -> bands.toString

    var t:String = ""
    tags.foreach(kv => {
      if (t.length > 0) {
        t += ","
      }
      t +=  kv._1 + "=" + kv._2
    })

    args += "tags" -> t
    args += "protection" -> protectionLevel

    var p:String = ""
    providerProperties.foreach(kv => {
      if (p.length > 0) {
        p += ","
      }
      p +=  kv._1 + "=" + kv._2
    })
    args += "providerproperties" -> p


    run(name, classOf[IngestImageSpark].getName, args.toMap, conf)

    true
  }

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}
}

class IngestImageSpark extends MrGeoJob with Externalizable {
  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes.result()

  }

  override def setup(job: JobArguments): Boolean = {
    true
  }

  override def execute(context: SparkContext): Boolean = {
    true
  }

  override def teardown(job: JobArguments): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}
