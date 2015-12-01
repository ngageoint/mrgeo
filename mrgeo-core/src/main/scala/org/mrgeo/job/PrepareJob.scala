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

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{NodeReport, NodeState}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{Logging, SparkConf}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo
import org.mrgeo.image.ImageStats
import org.mrgeo.utils.{Bounds, HadoopUtils, SparkUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable

object PrepareJob extends Logging {



  final def prepareJob(job: JobArguments): SparkConf = {

    val conf = SparkUtils.getConfiguration

    println("spark.app.name: " + conf.get("spark.app.name", "<not set>") + "  job.name: " + job.name)
    logInfo("spark.app.name: " + conf.get("spark.app.name", "<not set>") + "  job.name: " + job.name)
    conf.setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
    //.registerKryoClasses(registerClasses())

    if (job.isYarn) {
      loadYarnSettings(job)

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

  private def loadYarnSettings(job:JobArguments) = {
    val conf = HadoopUtils.createConfiguration()

    val res = calculateYarnResources()

    job.cores = 1 // 1 task per executor
    job.executors = res._1 / job.cores

    val sparkConf = SparkUtils.getConfiguration

    val mem = res._3


    // this is not only a min memory, but a "unit of allocation", each allocation a multiple of this number
    val minmemory = conf.getLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB)
    val maxmemory = conf.getLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB)

    val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", 384)

    logInfo("Initial values:  min memory: " + minmemory + "  (" + SparkUtils.kbtohuman(minmemory * 1024, "m") +
        ") max memory: " + maxmemory + "  (" + SparkUtils.kbtohuman(maxmemory * 1024, "m") +
        ") overhead: " + executorMemoryOverhead + "  (" + SparkUtils.kbtohuman(executorMemoryOverhead * 1024, "m") +
        ") executors: " + job.executors +
        " cluster memory: " + mem + " (" + SparkUtils.kbtohuman(mem * 1024, "m") + ")")

    var mult = 1.0

    if (job.isMemoryIntensive ||
        MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_FORCE_MEMORYINTENSIVE, "false").toBoolean) {
      mult = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_MEMORYINTENSIVE_MULTIPLIER, "2.0").toDouble

      logInfo("Memory intensive job.  multiplier: " + mult + " min memory now: " + (minmemory * mult).toInt +
          "  (" + SparkUtils.kbtohuman((minmemory * mult).toInt * 1024, "m") + ")")
    }

    // memory is allocated in units on minmemory (YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB).
    var confmem = configureYarnMemory(res._1, res._2, res._3, minmemory, (minmemory * mult).toInt, maxmemory)

    var actualoverhead = if ((confmem._2 * 0.1) > executorMemoryOverhead) (confmem._2 * 0.1).toLong else executorMemoryOverhead

    // if we are sucking up more than 1/2 memory in overhead, expand the memory
    while (confmem._2 < actualoverhead * 2) {
      mult += 1
      confmem = configureYarnMemory(res._1, res._2, res._3, minmemory, (minmemory * mult).toInt, maxmemory)
      actualoverhead = if ((confmem._2 * 0.1) > executorMemoryOverhead) (confmem._2 * 0.1).toLong else executorMemoryOverhead
    }

    job.executors = confmem._1
    job.executorMemKb = (confmem._2 - actualoverhead)  * 1024 // mb to kb
    job.memoryKb = mem * 1024 // mem is in mb, convert to kb

    logInfo("Configuring job (" + job.name + ") with " + job.executors + " tasks and " + SparkUtils.kbtohuman(job.memoryKb, "m") +
        " total memory, " + SparkUtils.kbtohuman(job.executorMemKb + (actualoverhead * 1024), "m") + " per worker (" +
        SparkUtils.kbtohuman(job.executorMemKb, "m") + " + " +
        SparkUtils.kbtohuman(actualoverhead * 1024, "m") + " overhead per task)" )

  }


  private def configureYarnMemory(cores:Int, nodes:Int, memory:Long, unitMemory:Long, minMemory:Long, maxMemory:Long) = {

    val rawMemoryPerNode = memory / nodes
    val rawExecutorsPerNode = cores / nodes
    val rawMemPerExecutor = rawMemoryPerNode / rawExecutorsPerNode

    val rawUnits = Math.floor(rawMemPerExecutor.toDouble / unitMemory)

    val units = {
      val r = rawUnits * unitMemory
      if (r > maxMemory)
      // Make this is a multiple of unitMemory
        Math.floor(maxMemory.toDouble / unitMemory.toDouble).toInt
      else if (r < minMemory)
      // Make this is a multiple of unitMemory
        Math.ceil(minMemory.toDouble / unitMemory.toDouble).toInt
      else
        rawUnits
    }

    val executorMemory = units * unitMemory

    val executorsPerNode = (rawMemoryPerNode.toDouble / executorMemory).toInt
    val executors = executorsPerNode * nodes

    (executors.toInt, executorMemory.toLong)
  }

  private def calculateYarnResources():(Int, Int, Long) = {

    val cl = getClass.getClassLoader

    val client:Class[_] =
      try {
        cl.loadClass("org.apache.hadoop.yarn.client.api.YarnClient")
      }
      catch {
        // YarnClient was here in older versions of YARN
        case cnfe: ClassNotFoundException =>
          cl.loadClass("org.apache.hadoop.yarn.client.YarnClient")
        case _:Throwable => null
      }

    val create = client.getMethod("createYarnClient")
    val init = client.getMethod("init", classOf[Configuration])
    val start = client.getMethod("start")


    val getNodeReports = client.getMethod("getNodeReports", classOf[Array[NodeState]])
    val stop = client.getMethod("stop")


    val conf = HadoopUtils.createConfiguration()
    val yc = create.invoke(null)
    init.invoke(yc, conf)
    start.invoke(yc)

    val na = new Array[NodeState](1)
    na(0) = NodeState.RUNNING

    val nr = getNodeReports.invoke(yc, na).asInstanceOf[util.ArrayList[NodeReport]]

    var cores:Int = 0
    var memory:Long = 0

    nr.foreach(rep => {
      val res = rep.getCapability

      memory = memory + res.getMemory
      cores = cores + res.getVirtualCores
    })

    val nodes:Int = nr.length

    stop.invoke(yc)

    (cores, nodes, memory)
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
