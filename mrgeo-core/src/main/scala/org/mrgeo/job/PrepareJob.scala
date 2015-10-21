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

import org.apache
import org.apache.spark
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{Logging, SparkConf}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo
import org.mrgeo.image.ImageStats
import org.mrgeo.utils.{Bounds, SparkUtils}

import scala.collection.mutable

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

  def setupSerializer(mrgeoJob: MrGeoJob, conf:SparkConf) = {
    val classes = Array.newBuilder[Class[_]]

    // automatically include common classes
    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes += classOf[Array[(TileIdWritable, RasterWritable)]]
    classes += classOf[Bounds]

    classes += classOf[ImageStats]
    classes += classOf[Array[ImageStats]]

    // include the old TileIdWritable & RasterWritable
    classes += classOf[org.mrgeo.core.mapreduce.formats.TileIdWritable]
    classes += classOf[org.mrgeo.core.mapreduce.formats.RasterWritable]

    // context.parallelize() calls create a WrappedArray.ofRef()
    classes += classOf[mutable.WrappedArray.ofRef[_]]

    // TODO:  Need to call DataProviders to register classes
    classes += classOf[FileSplitInfo]

    classes ++= mrgeoJob.registerClasses()

    registerClasses(classes.result(), conf)
  }

  def registerClasses(classes:Array[Class[_]], conf:SparkConf) = {
    if (MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_USE_KRYO, "false").equals("true")) {
      try {
        val all = mutable.HashSet.empty[String]
        all ++= conf.get("spark.kryo.classesToRegister", "").split(",").filter(!_.isEmpty)

        all ++= classes.filter(!_.getName.isEmpty).map(_.getName)

        conf.set("spark.kryo.classesToRegister", all.mkString(","))
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
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
