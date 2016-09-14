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
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.mrgeo.core.MrGeoProperties
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.utils.logging.LoggingUtils
import org.mrgeo.utils.{FileUtils, HadoopUtils, Logging, SparkUtils}

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


  def loadYarnSettings():Unit = {

    val res = calculateYarnResources()

    val sparkConf = SparkUtils.getConfiguration


    val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", 384)

    //    cores = res._1
    //    executors = res._2 // reserve 1 executor for the driver
    //    executorMemKb = (mem - actualoverhead) * 1024 // memory per worker
    //    memoryKb = res._4 * 1024  // total memory (includes driver memory)

    val (newCores, newExecutors, newExMem) = adjustYarnParameters(res._1, res._2, res._3, res._4)
    // val (newCores, newExecutors, newExMem) = adjustYarnParameters(80, 10, 241664, 2416640)

    val actualoverhead = ((if ((newExMem * YarnSparkHadoopUtil.MEMORY_OVERHEAD_FACTOR) > executorMemoryOverhead) newExMem * YarnSparkHadoopUtil.MEMORY_OVERHEAD_FACTOR
    else executorMemoryOverhead) * 0.95).toLong

    cores = newCores
    executors = newExecutors
    executorMemKb = (newExMem - actualoverhead) * 1024
    memoryKb = res._4 * 1024

    if (sparkConf.getBoolean("spark.dynamicAllocation.enabled", defaultValue = false)) {
      logInfo("Configuring job (" + name + ") using Spark dynamic allocation, each executor (starting with "
          + executors + "), using " + cores + " cores, and " + SparkUtils.kbtohuman(memoryKb, "m") +
          " total memory, " + SparkUtils.kbtohuman(executorMemKb + (actualoverhead * 1024), "m") +
          " per worker (" + SparkUtils.kbtohuman(executorMemKb, "m") + " + " +
          SparkUtils.kbtohuman(actualoverhead * 1024, "m") + " overhead per task)")
      println("Configuring job (" + name + ") using Spark dynamic allocation, each executor (starting with "
          + executors + "), using " + cores + " cores, and " + SparkUtils.kbtohuman(memoryKb, "m") +
          " total memory, " + SparkUtils.kbtohuman(executorMemKb + (actualoverhead * 1024), "m") +
          " per worker (" + SparkUtils.kbtohuman(executorMemKb, "m") + " + " +
          SparkUtils.kbtohuman(actualoverhead * 1024, "m") + " overhead per task)")
    }
    else {
      logInfo("Configuring job (" + name + ") with " + (executors + 1) + " workers (1 driver, " + executors +
          " executors)  with " +
          cores + " cores each and " + SparkUtils.kbtohuman(memoryKb, "m") +
          " total memory, " + SparkUtils.kbtohuman(executorMemKb + (actualoverhead * 1024), "m") +
          " per worker (" + SparkUtils.kbtohuman(executorMemKb, "m") + " + " +
          SparkUtils.kbtohuman(actualoverhead * 1024, "m") + " overhead per task)")
    }
  }

  private def adjustYarnParameters(origCores: Int, origExecutors: Int, origMemory: Long, totalMemory: Long): (Int, Int, Int) = {
    val sparkConf = SparkUtils.getConfiguration
    val hadoopConf = HadoopUtils.createConfiguration()

    val minmemory = hadoopConf.getLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB)
    // val maxmemory = 241664
    val maxmemory = hadoopConf.getLong(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB)

    val mincores = hadoopConf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES)
    // val maxcores = 16 // 32
    val maxcores = hadoopConf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES)

    println("Adjusting job parameters - original values: " + origExecutors + " executors (nodes) " +
        origCores + " cores/node " + origMemory +  " memory/node " +  totalMemory + " total memory")
    logInfo("Adjusting job parameters - original values: " + origExecutors + " executors (nodes) " +
        origCores + " cores/node " + origMemory +  " memory/node " +  totalMemory + " total memory")

    var mem = Math.min(Math.max(origMemory, minmemory), maxmemory)
    var cores = Math.min(Math.max(origCores, mincores), maxcores)
    var executors = origExecutors

    val memratio = Math.max(origMemory / mem.toDouble, 1.0)
    val coreratio = Math.max(origCores / cores.toDouble, 1.0)

    println("Adjusting job parameters - min/max values: cores: " + mincores + "/" + maxcores +
        " memory: " + minmemory + "/" + maxmemory)

    println("Adjusting job parameters - adjusted to min/max values: " + executors + " executors (nodes) " +
        cores + " cores/node " + mem +  " memory/node " +
        " mem ratio: " + memratio + " core ratio: " + coreratio)

    if (sparkConf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      if (memratio > coreratio) {
        cores = Math.max((origCores / memratio).toInt, 1)
        executors = Math.max((origExecutors * memratio).toInt, 1)
      }
      else if (coreratio > memratio) {
        executors = Math.max((origExecutors * Math.floor(coreratio)).toInt, 1)
        mem = Math.max((origMemory / coreratio).toInt, 1)
      }
      else {
        executors = Math.max((origExecutors * coreratio).toInt, 1)
      }
    }

    // if we only have 1 executor, we may need to lower the cores by 1 to reserve space for the driver
    if ((origExecutors == 1) && (cores > 1) && (cores * executors >= origCores)) {
      cores = cores - 1
    }

    (cores, executors, mem.toInt)

  }


  //  private def configureYarnMemory(cores:Int, nodes:Int, memory:Long, unitMemory:Long, minMemory:Long, maxMemory:Long) = {
  //    val rawMemoryPerNode = memory / nodes
  //    val rawExecutorsPerNode = cores / nodes
  //    val rawMemPerExecutor = rawMemoryPerNode / rawExecutorsPerNode
  //
  //    val rawUnits = Math.floor(rawMemPerExecutor.toDouble / unitMemory)
  //
  //    val units = {
  //      val r = rawUnits * unitMemory
  //      if (r > maxMemory)
  //      // Make this is a multiple of unitMemory
  //        Math.floor(maxMemory.toDouble / unitMemory.toDouble).toInt
  //      else if (r < minMemory)
  //      // Make this is a multiple of unitMemory
  //        Math.ceil(minMemory.toDouble / unitMemory.toDouble).toInt
  //      else
  //        rawUnits
  //    }
  //
  //    val executorMemory = units * unitMemory
  //
  //    val executorsPerNode = (rawMemoryPerNode.toDouble / executorMemory).toInt
  //    val executors = executorsPerNode * nodes
  //
  //    (executors.toInt, executorMemory.toLong)
  //  }

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

    val nodes:Int = nr.length

    stop.invoke(yc)

    // returns: (cores per node, nodes, memory(mb) per node, total memory(mb))
    (cores, nodes, memory, totalmemory)
  }

}

object JobArguments{
  def main(args: Array[String]): Unit = {
    LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO)
    val ja = new JobArguments()
    ja.loadYarnSettings()
  }
}
