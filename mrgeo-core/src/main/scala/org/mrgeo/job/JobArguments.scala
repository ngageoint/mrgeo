package org.mrgeo.job

import java.io.File
import java.net.URI

import org.apache.commons.lang3.SystemUtils
import org.mrgeo.util.Memory
import scala.collection.mutable.ArrayBuffer

class JobArguments(args: Seq[String]) {
  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDrive = "([a-zA-Z])".r
  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS
  var name: String = "Unnamed MrGeo Job"
  var cluster: String = "local[1]"
  var driverClass: String = null
  var driverJar: String = null

  var executorMem:String = null
  var driverMem:String = null
  var executors:Int = -1
  var cores:Int = -1
  val params = collection.mutable.Map[String, String]()

  parse(args.toList)
  var jars: String = null
  var verbose: Boolean = false

  def this() {
    this(List[String]())
  }

  def hasSetting(name: String): Boolean = {
    return params.contains(name)
  }

  def getSetting(name: String): String = {
    return params(name)
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
      args += "jars "
      args += jars
    }

    if (driverJar != null) {
      args += "driverjar "
      args += driverJar
    }

    if (verbose) {
      args += "verbose"
    }

    args += "cores"
    args += cores.toString

    args += "executors"
    args += executors.toString

    args += "executorMemory"
    args += executorMem

    args += "driverMemory"
    args += driverMem

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
      driverJar = resolveURIs(value)
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case ("--verbose" | "-v") :: tail =>
      verbose = true
      parse(tail)

    case ("--cores") :: value :: tail =>
      cores = value.toInt
      parse(tail)

    case ("--executors") :: value :: tail =>
      executors = value.toInt
      parse(tail)

    case ("--executorMemory") :: value :: tail =>
      executorMem = value
      parse(tail)

    case ("--driverMemory") :: value :: tail =>
      driverMem = value
      parse(tail)

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
    cluster = "yarn"
    println("Setting cluster to: " + cluster)

  }

  def useDebug(): Unit = {
    cluster = "local[1]"
    println("Setting cluster to: " + cluster)

    setupMemory()
  }

  def useLocal(): Unit = {
    val cores: Int = Runtime.getRuntime.availableProcessors()
    if (cores <= 2) {
      cluster = "local"
    }
    else {
      cluster = "local[" + (cores.toDouble * 0.75).round + "]"
    }

    println("Setting cluster to: " + cluster)

    setupMemory()
  }

  // taken from Spark:Utils (private methods)

  def useSpark(master: String): Unit = {
    if (master.startsWith("spark://")) {
      cluster = master
    }
    else {
      cluster = "spark://" + master
    }

    println("Setting cluster to: " + cluster)
  }

  private def setupMemory(): Unit = {
    val maxMem = Runtime.getRuntime.maxMemory()
    if (maxMem != Long.MaxValue) {
      val mem = (maxMem * 0.95).round
      executorMem = mem.toString
      println("Setting max memory to: " + Memory.format(mem))
    }
  }

  /**
   * Format a Windows path such that it can be safely passed to a URI.
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    //    val outStream = SparkSubmit.printStream
    //    if (unknownParam != null) {
    //      outStream.println("Unknown/unsupported param " + unknownParam)
    //    }
    //    outStream.println(
    //      """Usage: spark-submit [options] <app jar | python file> [app options]
    //        |Options:
    //      """
    //    )
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  private def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash, but this is inconsistent with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path

    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }
    uri.getScheme match {
    case windowsDrive(d) if windows =>
      new URI("file:/" + uri.toString.stripPrefix("/"))
    case null =>
      // Preserve fragments for HDFS file name substitution (denoted by "#")
      // For instance, in "abc.py#xyz.py", "xyz.py" is the name observed by the application
      val fragment = uri.getFragment
      val part = new File(uri.getPath).toURI
      new URI(part.getScheme, part.getPath, fragment)
    case _ =>
      uri
    }
  }

  /** Resolve a comma-separated list of paths. */
  private def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    }
    else {
      paths.split(",").map { p => resolveURI(p, testWindows)}.mkString(",")
    }
  }

}
