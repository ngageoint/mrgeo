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

package org.mrgeo.job

import java.io.{IOException, FileInputStream, InputStreamReader, File}
import java.util.Properties

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Logging, SparkException, SparkConf, SparkContext}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.ImageStats
import org.mrgeo.job.yarn.MrGeoYarnJob
import org.mrgeo.utils.SparkUtils

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

object PrepareJob extends Logging {



  final def prepareJob(job: JobArguments): SparkConf = {

    val conf = SparkUtils.getConfiguration

    logInfo("spark.app.name: " + conf.get("spark.app.name", "<not set>") + "  job.name: " + job.name)
    conf.setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
    //.registerKryoClasses(registerClasses())

    if (job.isYarn) {
      // running in "cluster" mode, the driver runs within a YARN process
      conf.setMaster(job.YARN + "-cluster")

      conf.set("spark.yarn.preserve.staging.files", "true")
      conf.set("spark.eventLog.overwrite", "true") // overwrite event logs

      var path:String = ""
      if (conf.contains("spark.driver.extraLibraryPath")) {
        path = ":" + conf.get("spark.driver.extraLibraryPath")
      }
      conf.set("spark.driver.extraLibraryPath",
        MrGeoProperties.getInstance.getProperty(MrGeoConstants.GDAL_PATH, "") + path)

      if (conf.contains("spark.executor.extraLibraryPath")) {
        path = ":" + conf.get("spark.executor.extraLibraryPath")
      }
      conf.set("spark.executor.extraLibraryPath",
        MrGeoProperties.getInstance.getProperty(MrGeoConstants.GDAL_PATH, ""))

      conf.set("spark.driver.memory", SparkUtils.kbtohuman(job.executorMemKb, "m"))
      conf.set("spark.driver.cores", "1")

      val exmem = SparkUtils.kbtohuman(job.memoryKb , "m")
      conf.set("spark.executor.memory", exmem)
      conf.set("spark.executor.instances", (job.executors - 1).toString)
      conf.set("spark.executor.cores", job.cores.toString)

    }
    else if (job.isSpark) {
      conf.set("spark.driver.memory", if (job.memoryKb > 0) {
        SparkUtils.kbtohuman(job.memoryKb, "m")
      }
      else {
        "128m"
      })
          .set("spark.driver.cores", if (job.cores > 0) {
        job.cores.toString
      }
      else {
        "1"
      })
    }


    conf
  }

  def setupSerializer(mrgeoJob: MrGeoJob, job:JobArguments, conf:SparkConf) = {
    // we need to check the serializer property, there is a bug in the registerKryoClasses in Spark < 1.3.0 that
    // causes a ClassNotFoundException.  So we need to add a config property to use/ignore kryo
    if (MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_USE_KRYO, "false").equals("true")) {
      // Check and invoke for registerKryoClasses() with reflection, because isn't in pre Spark 1.2.0
      try {
        val method = conf.getClass.getMethod("registerKryoClasses", classOf[Array[Class[_]]])
        val classes = Array.newBuilder[Class[_]]

        // automatically include common classes
        classes += classOf[TileIdWritable]
        classes += classOf[RasterWritable]

        classes += classOf[Array[(TileIdWritable, RasterWritable)]]

        classes += classOf[ImageStats]
        classes += classOf[Array[ImageStats]]

        // include the old TileIdWritable & RasterWritable
        classes += classOf[org.mrgeo.core.mapreduce.formats.TileIdWritable]
        classes += classOf[org.mrgeo.core.mapreduce.formats.RasterWritable]


        // TODO:  Need to call DataProviders to register classes
        classes += classOf[FileSplitInfo]

        classes ++= mrgeoJob.registerClasses()

        method.invoke(conf, classes.result())
      }
      catch {
        case nsme: NoSuchMethodException => conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        case e: Exception => e.printStackTrace()
      }

    }
    else {
      conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    }

  }

}
