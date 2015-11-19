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

import java.io.File
import java.net.URL
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ClassUtil
import org.apache.hadoop.yarn.api.records.{NodeReport, NodeState}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.Logging
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.job.yarn.MrGeoYarnDriver
import org.mrgeo.utils._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

abstract class MrGeoDriver extends Logging {

  def setup(job: JobArguments): Boolean

  def run(name:String, driver:String = this.getClass.getName, args:Map[String, String] = Map[String, String](),
      hadoopConf:Configuration, additionalClasses: Option[Seq[Class[_]]] = None) = {
    val job = new JobArguments()

    job.driverClass = driver

    job.name = name
    job.setAllSettings(args)
    job.addMrGeoProperties()
    val dpfProperties = DataProviderFactory.getConfigurationFromProviders
    job.setAllSettings(dpfProperties.toMap)


    logInfo("Configuring application")

    // setup dependencies, but save the local deps to use in the classloader
    val local = setupDependencies(job, hadoopConf, additionalClasses)

    var parentLoader:ClassLoader = Thread.currentThread().getContextClassLoader
    if (parentLoader == null) {
      parentLoader = getClass.getClassLoader
    }

    val urls = local.map(jar => {
      new URL(FileUtils.resolveURL(jar))
    })

    val cl = new URLClassLoader(urls.toSeq, parentLoader)

    setupDriver(job, cl)


    setup(job)

    if (HadoopUtils.isLocal(hadoopConf)) {
      if (!job.isDebug) {
        job.useLocal()
      }
      else {
        job.useDebug()
      }
    }
    else {
      val cluster = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_CLUSTER, "local")

      cluster.toLowerCase match {
      case "yarn" =>
        job.useYarn()
        loadYarnSettings(job, cl)
        addYarnClasses(cl)

      case "spark" =>
        val conf = PrepareJob.prepareJob(job)
        val master = conf.get("spark.master", "spark://localhost:7077")
        job.useSpark(master)
      case _ => job.useLocal()
      }
    }

    val conf = PrepareJob.prepareJob(job)

    // yarn needs to be run in its own client code, so we'll set up it up separately
    if (job.isYarn) {

      val jobclass = cl.loadClass(classOf[MrGeoYarnDriver].getCanonicalName)
      val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoYarnDriver]

      jobinstance.run(job, cl, conf)
    }
    else {
      val jobclass = cl.loadClass(job.driverClass)
      val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoJob]

      jobinstance.run(job, conf)
    }
  }

  private def setupDriver(job:JobArguments, cl: URLClassLoader) = {
    if (job.driverClass == null)
    {
      job.driverClass = getClass.getName.replaceAll("\\$","") .replaceAll("\\$","")
    }

    if (job.driverJar == null || !job.driverJar.endsWith(".jar")) {
      val clazz = Class.forName(job.driverClass)
      val jar: String = ClassUtil.findContainingJar(clazz)

      if (jar != null) {
        job.driverJar = jar
      }
      else {
        job.driverJar = SparkUtils.jarForClass(job.driverClass, cl)
      }
    }
  }

  private def addYarnClasses(cl: URLClassLoader) = {
    // need to get the Yarn config by reflection, since we support non YARN setups
    val clazz = getClass.getClassLoader.loadClass("org.apache.hadoop.yarn.conf.YarnConfiguration")

    if (clazz != null) {
      val conf = clazz.newInstance()
      val get = clazz.getMethod("get", classOf[String], classOf[String])

      var cp = get.invoke(conf, "yarn.application.classpath", "").asInstanceOf[String]

      //    val conf:YarnConfiguration = new YarnConfiguration
      //
      //    // get the yarn classpath from the configuration...
      //    var cp = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH, "")
      //
      // replace any variables $<xxx> with their environmental value
      val envMap = System.getenv()
      for (entry <- envMap.entrySet()) {
        val key = entry.getKey
        val value = entry.getValue

        cp = cp.replaceAll("\\$" + key, value)
      }

      // add the urls to the classloader
      for (str <- cp.split(",")) {
        val url = new File(str.trim).toURI.toURL
        cl.addURL(url)
      }
    }
  }


  private def loadYarnSettings(job:JobArguments, cl: URLClassLoader) = {
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


  protected def setupDependencies(job:JobArguments, hadoopConf:Configuration,
      additionalClasses: Option[Seq[Class[_]]] = None): mutable.Set[String] = {

    val dependencies = DependencyLoader.getDependencies(getClass)
    val qualified = DependencyLoader.copyDependencies(dependencies, hadoopConf)
    val jars:StringBuilder = new StringBuilder

    for (jar <- qualified) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      //if (!jar.contains("spark-yarn")) {
      if (jars.nonEmpty) {
        jars ++= ","
      }
      jars ++= jar
      //}
    }

    val dpfDependencies = DataProviderFactory.getDependencies
    val dpfQualified = DependencyLoader.copyDependencies(dpfDependencies, hadoopConf)
    for (jar <- dpfQualified) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      //if (!jar.contains("spark-yarn")) {
      if (jars.nonEmpty) {
        jars ++= ","
      }
      jars ++= jar
      //}
    }

    if (additionalClasses.isDefined) {
      var additionalDependencies = Set.newBuilder[String]
      additionalClasses.get.foreach(additionalClass => {
        additionalDependencies ++= DependencyLoader.getDependencies(additionalClass)
      })
      val additionalQualified = DependencyLoader.copyDependencies(additionalDependencies.result(), hadoopConf)
      additionalQualified.foreach(jar => {
        if (jars.nonEmpty) {
          jars ++= ","
        }
        jars ++= jar
      })
    }
    job.setJars(jars.toString())

    dependencies
  }

}
