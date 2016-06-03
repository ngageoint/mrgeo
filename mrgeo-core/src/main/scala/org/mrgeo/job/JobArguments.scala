/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.job

import java.util

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{NodeReport, NodeState}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.Logging
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.mrgeo.core.MrGeoProperties
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.utils.{FileUtils, HadoopUtils, Memory, SparkUtils}

import scala.collection.JavaConversions.{asScalaSet, _}
import scala.collection.mutable.ArrayBuffer

@SuppressFBWarnings(value=Array("UUF_UNUSED_FIELD"), justification = "Scala generated code")
@SuppressFBWarnings(value=Array("UPM_UNCALLED_PRIVATE_METHOD"), justification = "Scala constant")
class JobArguments() extends Logging {

  final private val Name:String =  "name"
  final private val Cluster:String =  "cluster"
  final private val Driver:String = "driver"
  final private val Jars:String =  "jars"
  final private val DriverJar:String = "driverjar"
  final private val Verbose:String =  "verbose"
  final private val Cores:String =  "cores"
  final private val Executors:String =  "executors"
  final private val Memory:String =  "memory"
  final private val MrGeoConfPrefix = "MrGeoConf:"

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  final val YARN:String = "yarn"
  final val LOCAL:String = "local"
  final val SPARK:String = "spark://"

  val windowsDrive = "([a-zA-Z])".r
  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS
  var name: String = "foo" // null
  var cluster: String = "local[1]"
  var driverClass: String = null
  var driverJar: String = null

  var memoryKb:Long = -1
  //var driverMem:String = null
  var executors:Int = -1
  var executorMemKb:Long = -1
  var cores:Int = -1
  val params = collection.mutable.Map[String, String]()

  var jars: Array[String] = null
  var verbose: Boolean = false

  var isMemoryIntensive:Boolean = false

  def this(args: Seq[String]) {
    this()
    parse(args.toList)
    DataProviderFactory.setConfigurationForProviders(params)
  }


  def hasSetting(name: String): Boolean = {
    params.contains(name)
  }

  def getSetting(name: String): String = {
    params(name)
  }
  def getSetting(name: String, default:String): String = {
    if (params.contains(name)) {
      params(name)
    }
    else
    {
      default
    }
  }

  def addMrGeoProperties(): Unit =
  {
    val props = MrGeoProperties.getInstance()
    props.stringPropertyNames().foreach(U => {
      val key = MrGeoConfPrefix + U
      params += key -> props.getProperty(U)
    })
  }

  def setSetting(key:String, value:String) = {
    if (key.startsWith(MrGeoConfPrefix))
    {
      val mrGeoKey = key.substring(MrGeoConfPrefix.length)
      MrGeoProperties.getInstance().setProperty(mrGeoKey, value)
    }
    else
    {
      params += key -> value
    }
  }

  def setAllSettings(values: Map[String, String]) = {
    values.foreach(U => {
      setSetting(U._1, U._2)
    })
  }

  def setJars(paths:String) = {
    jars = resolveURIs(paths)
  }

  def toArgs: String = {
    val str = new StringBuilder()

    var ping: Boolean = true

    for (s <- toArray) {
      if (ping) {
        str ++= "--"
      }
      str ++= s
      str ++= " "

      ping = !ping
    }

    str.toString()
  }


  def toArgArray: Array[String] = {
    val args = new ArrayBuffer[String]()
    var ping: Boolean = true

    for (s <- toArray) {
      if (ping) {
        args += ("--" + s)
      }
      else if (s != null) {
        args += s
      }

      ping = !ping
    }

    args.toArray
  }

  def toArray: Array[String] = {
    val args = new ArrayBuffer[String]()
    if (name != null) {
      args += Name
      args += name
    }

    if (cluster != null) {
      args += Cluster
      args += cluster
    }

    if (driverClass != null) {
      args += Driver
      args += driverClass
    }

    if (jars != null) {
      args += Jars
      args += jars.mkString(",")
    }

    if (driverJar != null) {
      args += DriverJar
      args += driverJar
    }

    if (verbose) {
      args += Verbose
    }

    if (cores > 0) {
      args += Cores
      args += cores.toString
    }

    if (executors > 0) {
      args += Executors
      args += executors.toString
    }

    if (memoryKb > 0) {
      args += Memory
      args += memoryKb.toString
    }

    for (param <- params) {
      args += param._1
      args += param._2
    }

    args.toArray
  }

  @SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
  private def parse(opts: Seq[String]): Unit = opts match {
  case ("--name") :: value :: tail =>
    name = value
    parse(tail)

  case ("--cluster") :: value :: tail =>
    value match  {
    case "yarn" => useYarn()
    case "debug" => useDebug()
    case "local" => useLocal()
    case _ => cluster = value
    }
    parse(tail)

  case ("--driver") :: value :: tail =>
    driverClass = value
    parse(tail)

  case ("--jars") :: value :: tail =>
    jars = resolveURIs(value)
    parse(tail)

  case ("--driverjar") :: value :: tail =>
    driverJar = FileUtils.resolveURI(value)
    parse(tail)

  case ("--verbose" | "-v") :: tail =>
    verbose = true
    parse(tail)

  case ("--cores" | "--executor-cores") :: value :: tail =>
    cores = value.toInt
    parse(tail)

  case ("--executors" | "--num-executors") :: value :: tail =>
    executors = value.toInt
    parse(tail)

  case ("--memory | --executor-memory") :: value :: tail =>
    memoryKb = value.toInt
    parse(tail)

  case key :: value :: tail =>
    if (key.startsWith("--")) {
      if (value.startsWith("--")) {
        // The key is an on/off switch because the value is not really
        // a value, so continue parsing with the value
        setSetting(key.substring(2), null)
        parse(value :: tail)
      }
      else {
        setSetting(key.substring(2), value)
        parse(tail)
      }
    }
    else {
      throw new Exception("Invalid switch " + key + ": must start with --")
    }

  case key :: Nil =>
    if (key.startsWith("--")) {
      setSetting(key.substring(2), null)
    }
    else {
      throw new Exception("Invalid switch " + key + ": must start with --")
    }

  case Nil =>
  }

  def useYarn(): Unit = {
    cluster = YARN
    logInfo("Setting cluster to: " + cluster)

  }

  def useDebug(): Unit = {
    cluster = LOCAL + "[1]"
    logInfo("Setting cluster to: " + cluster)

    setupMemory()
  }

  def useLocal(): Unit = {
    val cores: Int = Runtime.getRuntime.availableProcessors()
    if (cores <= 2) {
      cluster = LOCAL
    }
    else {
      cluster = LOCAL+"[" + (cores.toDouble * 0.75).round + "]"
    }

    logInfo("Setting cluster to: " + cluster)

    setupMemory()
  }

  def useSpark(master: String): Unit = {
    if (master.startsWith(SPARK)) {
      cluster = master
    }
    else {
      cluster = SPARK + master
    }

    logInfo("Setting cluster to: " + cluster)
  }


  def isYarn: Boolean = {
    cluster.startsWith(YARN)
  }

  def isLocal: Boolean = {
    cluster.startsWith(LOCAL)
  }

  def isDebug: Boolean = {
    cluster.startsWith(LOCAL + "[1]")
  }

  def isSpark: Boolean = {
    cluster.startsWith(SPARK)
  }



  // taken from Spark:Utils (private methods)


  private def setupMemory(): Unit = {
    val maxMem = Runtime.getRuntime.maxMemory()
    if (maxMem != Long.MaxValue) {
      val mem = (maxMem * 0.95).round
      memoryKb = mem / 1024 // memory is in bytes, convert to kb
      logInfo("Setting max memory to: " + Memory.format(mem))
    }
  }

  /** Resolve a comma-separated list of paths. */
  private def resolveURIs(paths: String, testWindows: Boolean = false): Array[String] = {
    if (paths == null || paths.trim.isEmpty) {
      null
    }
    else {
      paths.split(",").map { p => FileUtils.resolveURI(p)}
    }
  }

  def loadYarnSettings() = {

    val res = calculateYarnResources()
    val sparkConf = SparkUtils.getConfiguration

    val minmemory = sparkConf.getLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB)
    val maxmemory = sparkConf.getLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB)

    val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", 384)
    val mem = res._3
    val actualoverhead = ((if ((mem * YarnSparkHadoopUtil.MEMORY_OVERHEAD_FACTOR) > executorMemoryOverhead) mem * YarnSparkHadoopUtil.MEMORY_OVERHEAD_FACTOR
    else executorMemoryOverhead) * 0.95).toLong

    cores = res._1
    executors = res._2 // reserve 1 executor for the driver
    executorMemKb = (mem - actualoverhead) * 1024 // memory per worker
    memoryKb = res._4 * 1024  // total memory (includes driver memory)

    logInfo("Configuring job (" + name + ") with " + (executors + 1) + " workers (1 driver, " + executors + " executors)  with " +
        cores + " threads each and " + SparkUtils.kbtohuman(memoryKb, "m") +
        " total memory, " + SparkUtils.kbtohuman(executorMemKb + (actualoverhead * 1024), "m") +
        " per worker (" + SparkUtils.kbtohuman(executorMemKb, "m") + " + " +
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

  private def calculateYarnResources():(Int, Int, Long, Long) = {

    val cl = getClass.getClassLoader

    val client:Class[_] =
      try {
        cl.loadClass("org.apache.hadoop.yarn.client.api.YarnClient")
      }
      catch {
        // YarnClient was here in older versions of YARN
        case cnfe: ClassNotFoundException =>
          cl.loadClass("org.apache.hadoop.yarn.client.YarnClient")
        case t:Throwable => throw t
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

    var cores:Int = Int.MaxValue
    var memory:Long = Long.MaxValue
    var totalmemory:Long = 0

    nr.foreach(rep => {
      val res = rep.getCapability

      memory = Math.min(memory, res.getMemory)
      cores = Math.min(cores, res.getVirtualCores)
      totalmemory += res.getMemory

    })

    val mincores = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES)
    val maxcores = conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES)

    cores  = Math.max(Math.min(cores, maxcores), mincores)

    val nodes:Int = nr.length

    stop.invoke(yc)


    // returns: (cores per nodes, nodes, memory (mb) per node)

    //  we need a minimum of 2 nodes (one for the worker, 1 for the driver)
    if (nodes == 1) {
      (cores - 1, nodes, memory / 2, totalmemory)
    }
    else {
      (cores, nodes, memory, totalmemory)
    }
  }


}
