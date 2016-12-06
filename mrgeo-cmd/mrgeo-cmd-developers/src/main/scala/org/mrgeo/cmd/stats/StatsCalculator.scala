package org.mrgeo.cmd.stats

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils.SparkUtils

import scala.collection.mutable


object StatsCalculator extends MrGeoDriver with Externalizable {

  private val Pyramid = "pyramid"
  private val ProviderProperties = "provider.properties"

  def calculate(pyramid:String, conf:Configuration, providerProperties:ProviderProperties):Boolean = {
    val args = mutable.Map[String, String]()

    val name = "StatsCalculator"

    args += Pyramid -> pyramid
    if (providerProperties != null) {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
    }
    else {
      args += ProviderProperties -> ""
    }

    run(name, classOf[StatsCalculator].getName, args.toMap, conf)

    true
  }

  override def setup(job:JobArguments):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}
}


class StatsCalculator extends MrGeoJob with Externalizable {
  var input:String = _
  var providerProperties:ProviderProperties = _

  override def registerClasses():Array[Class[_]] = {
    Array.empty[Class[_]]
  }


  override def execute(context:SparkContext):Boolean = {

    val dp = DataProviderFactory.getMrsImageDataProvider(input, AccessMode.READ, providerProperties)
    val metadata = dp.getMetadataReader.read()

    var zoom = metadata.getMaxZoomLevel
    while (zoom > 0) {
      val rdd = SparkUtils.loadMrsPyramid(dp, zoom, context)


      val stats = SparkUtils.calculateStats(rdd, metadata.getBands, metadata.getDefaultValues)

      metadata.setImageStats(zoom, stats)
      if (zoom == metadata.getMaxZoomLevel) {
        metadata.setStats(stats)
      }
      zoom -= 1
    }

    dp.getMetadataWriter.write(metadata)
    true
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    input = job.getSetting(StatsCalculator.Pyramid)
    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(StatsCalculator.ProviderProperties))
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {

  }

  override def writeExternal(out:ObjectOutput):Unit = {

  }
}
