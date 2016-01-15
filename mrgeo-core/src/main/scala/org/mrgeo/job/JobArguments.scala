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

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.Logging
import org.mrgeo.core.MrGeoProperties
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.utils.{FileUtils, Memory}

import scala.collection.JavaConversions.{asScalaSet, _}
import scala.collection.mutable.ArrayBuffer

class JobArguments() extends Logging {

  final private val NAME:String =  "name"
  final private val CLUSTER:String =  "cluster"
  final private val DRIVER:String = "driver"
  final private val JARS:String =  "jars"
  final private val DRIVERJAR:String = "driverjar"
  final private val VERBOSE:String =  "verbose"
  final private val CORES:String =  "cores"
  final private val EXECUTORS:String =  "executors"
  final private val MEMORY:String =  "memory"
  final private val MRGEO_CONF_PREFIX = "MrGeoConf:"

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
      val key = MRGEO_CONF_PREFIX + U
      params += key -> props.getProperty(U)
    })
  }

  def setSetting(key:String, value:String) = {
    if (key.startsWith(MRGEO_CONF_PREFIX))
    {
      val mrGeoKey = key.substring(MRGEO_CONF_PREFIX.length)
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
      args += NAME
      args += name
    }

    if (cluster != null) {
      args += CLUSTER
      args += cluster
    }

    if (driverClass != null) {
      args += DRIVER
      args += driverClass
    }

    if (jars != null) {
      args += JARS
      args += jars.mkString(",")
    }

    if (driverJar != null) {
      args += DRIVERJAR
      args += driverJar
    }

    if (verbose) {
      args += VERBOSE
    }

    if (cores > 0) {
      args += CORES
      args += cores.toString
    }

    if (executors > 0) {
      args += EXECUTORS
      args += executors.toString
    }

    if (memoryKb > 0) {
      args += MEMORY
      args += memoryKb.toString
    }

    for (param <- params) {
      args += param._1
      args += param._2
    }

    args.toArray
  }

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

}
