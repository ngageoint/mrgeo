package org.mrgeo.spark.job

import org.apache.commons.lang3.SystemUtils
import org.mrgeo.utils.{FileUtils, Memory}

import scala.collection.mutable.ArrayBuffer

class JobArguments() {
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
  var name: String = null
  var cluster: String = "local[1]"
  var driverClass: String = null
  var driverJar: String = null

  var memory:String = null
  //var driverMem:String = null
  var executors:Int = -1
  var cores:Int = -1
  val params = collection.mutable.Map[String, String]()

  var jars: Array[String] = null
  var verbose: Boolean = false

  def this(args: Seq[String]) {
    this()
    parse(args.toList)
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

  def setSetting(key:String, value:String) = {
    params += key -> value
  }

  def setAllSettings(values: Map[String, String]) = {
    params ++= values
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
      args += "name"
      args += name
    }

    if (cluster != null) {
      args += "cluster"
      args += cluster
    }

    if (driverClass != null) {
      args += "driver"
      args += driverClass
    }

    if (jars != null) {
      args += "jars"
      args += jars.mkString(",")
    }

    if (driverJar != null) {
      args += "driverjar"
      args += driverJar
    }

    if (verbose) {
      args += "verbose"
    }

    if (cores > 0) {
      args += "cores"
      args += cores.toString
    }

    if (executors > 0) {
      args += "executors"
      args += executors.toString
    }

    if (memory != null) {
      args += "memory"
      args += memory
    }

//    if (driverMem != null) {
//      args += "driverMemory"
//      args += driverMem
//    }

    for (param <- params) {
      args += param._1
      args += param._2
    }

    args.toArray
  }

  def parse(opts: Seq[String]): Unit = opts match {
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

  case ("--cores") :: value :: tail =>
    cores = value.toInt
    parse(tail)

  case ("--executors") :: value :: tail =>
    executors = value.toInt
    parse(tail)

  case ("--memory") :: value :: tail =>
    memory = value
    parse(tail)

//  case ("--driverMemory") :: value :: tail =>
//    driverMem = value
//    parse(tail)

  case key :: value :: tail =>
    if (key.startsWith("--")) {
      if (value.startsWith("--")) {
        // The key is an on/off switch because the value is not really
        // a value, so continue parsing with the value
        params(key.substring(2)) = null
        parse(value :: tail)
      }
      else {
        params(key.substring(2)) = value
        parse(tail)
      }
    }
    else {
      throw new Exception("Invalid switch " + key + ": must start with --")
    }
  //      if (inSparkOpts) {
  //        value match {
  //          // convert --foo=bar to --foo bar
  //        case v if v.startsWith("--") && v.contains("=") && v.split("=").size == 2 =>
  //          val parts = v.split("=")
  //          parse(Seq(parts(0), parts(1)) ++ tail)
  //        case v if v.startsWith("-") =>
  //          val errMessage = s"Unrecognized option '$value'."
  //          printErrorAndExit(errMessage)
  //        case v =>
  //          primaryResource =
  //              if (!SparkSubmit.isShell(v)) {
  //                Utils.resolveURI(v).toString
  //              } else {
  //                v
  //              }
  //          inSparkOpts = false
  //          isPython = SparkSubmit.isPython(v)
  //          parse(tail)
  //        }
  //      } else {
  //        if (!value.isEmpty) {
  //          childArgs += value
  //        }
  //        parse(tail)
  //      }

  case key :: Nil =>
    if (key.startsWith("--")) {
      params(key.substring(2)) = null
    }
    else {
      throw new Exception("Invalid switch " + key + ": must start with --")
    }

  case Nil =>
  }

  def useYarn(): Unit = {
    cluster = YARN
    println("Setting cluster to: " + cluster)

  }

  def useDebug(): Unit = {
    cluster = LOCAL + "[1]"
    println("Setting cluster to: " + cluster)

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

    println("Setting cluster to: " + cluster)

    setupMemory()
  }

  def isYarn: Boolean = {
    cluster.startsWith(YARN)
  }

  def isLocal: Boolean = {
    cluster.startsWith(LOCAL)
  }

  def isSpark: Boolean = {
    cluster.startsWith(SPARK)
  }

  // taken from Spark:Utils (private methods)

  def useSpark(master: String): Unit = {
    if (master.startsWith(SPARK)) {
      cluster = master
    }
    else {
      cluster = SPARK + master
    }

    println("Setting cluster to: " + cluster)
  }

  private def setupMemory(): Unit = {
    val maxMem = Runtime.getRuntime.maxMemory()
    if (maxMem != Long.MaxValue) {
      val mem = (maxMem * 0.95).round
      memory = mem.toString
      println("Setting max memory to: " + Memory.format(mem))
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
