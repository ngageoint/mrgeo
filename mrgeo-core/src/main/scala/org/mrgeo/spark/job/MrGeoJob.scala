package org.mrgeo.spark.job

import java.io._
import java.util.Properties

import org.apache.spark._
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


abstract class MrGeoJob {
  def registerClasses(): Array[Class[_]]

  def setup(job: JobArguments): Boolean

  def execute(context: SparkContext): Boolean

  def teardown(job: JobArguments): Boolean

  private[job] def run(args: Array[String] = new Array[String](0)) = {
    val job = new JobArguments(args)


    println("Configuring application")
    setup(job)

    val context = prepareJob(job)
    try {
      println("Running application")
      execute(context)
    }
    finally {
      println("Stopping spark context")
      context.stop()
    }

    teardown(job)
  }

  // These 3 methods are taken almost verbatim from Spark's Utils class, but they are all
  // private, so we needed to copy them here
  /** Load properties present in the given file. */
  private def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    }
    catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    }
    finally {
      inReader.close()
    }
  }

  private def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("SPARK_CONF_DIR")
        .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf"})
        .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
        .filter(_.isFile)
        .map(_.getAbsolutePath)
        .orNull
  }

  private def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("spark.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  final private def prepareJob(job: JobArguments): SparkContext = {

    val conf: SparkConf = new SparkConf()

    loadDefaultSparkProperties(conf)

    conf.setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
        //.registerKryoClasses(registerClasses())
        //    .set("spark.driver.extraClassPath", "")
        //    .set("spark.driver.extraJavaOptions", "")
        //    .set("spark.driver.extraLibraryPath", "")

        .set("spark.storage.memoryFraction", "0.25")

    // setup the kryo serializer
    //.set("spark.serializer","org.mrgeo.job.MySerializer")
    //.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //.set("spark.kryo.registrator","org.mrgeo.job.KryoRegistrar")
    //.set("spark.kryoserializer.buffer.mb","128")


    // driver memory and cores
    //.set("spark.driver.memory", if (job.driverMem != null) job.driverMem else "128m")
    //.set("spark.driver.cores", if (job.cores > 0) job.cores.toString else "1")


    if (conf.contains("spark.executor.extraClassPath")) {
      conf.set("spark.executor.extraClassPath",
        conf.get("spark.executor.extraClassPath") +
            ":/home/tim.tisler/projects/mrgeo/mrgeo-opensource/mrgeo-core/target/mrgeo-core.jar")
    }
    else {
      conf.set("spark.executor.extraClassPath",
        "/home/tim.tisler/projects/mrgeo/mrgeo-opensource/mrgeo-core/target/mrgeo-core.jar")
    }

    // we need to check the serializer property, there is a bug in the registerKryoClasses in Spark < 1.3.0 that
    // causes a ClassNotFoundException.  So we need to add a config property to use/ignore kryo
    if (MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_USE_KRYO, "false").equals("true")) {
      // Check and invoke for registerKryoClasses() with reflection, because isn't in pre Spark 1.2.0
      try {
        val method = conf.getClass.getMethod("registerKryoClasses", classOf[Array[Class[_]]])
        method.invoke(conf, registerClasses())
      }
      catch {
        case nsme: NoSuchMethodException => conf.set("spark.serializer", "org.mrgeo.spark.JavaSerializer")
        case e: Exception => e.printStackTrace()
      }
    }
    else {
      conf.set("spark.serializer", "org.mrgeo.spark.JavaSerializer")
    }

    if (job.isYarn) {
      //conf.set("spark.yarn.jar","")
      conf.set("spark.yarn.am.cores", if (job.cores > 0) { job.cores.toString } else { "1" })
          .set("spark.executor.memory", if (job.memory != null) { job.memory } else { "128m" })
          .set("spark.executor.cores", if (job.cores > 0) { job.cores.toString } else { "1" })
          .set("spark.cores.max", if (job.cores > 0) { job.cores.toString } else { "1" })
          .set("spark.yarn.preserve.staging.files", "true")
          // running in "cluster" mode, the driver runs within a YARN process
          .setMaster(job.YARN + "-cluster")

      val appargs = new ArrayBuffer[String]()

      appargs += "--jar"
      appargs += job.driverJar

      appargs += "--class"
      appargs += job.driverClass


      System.setProperty("SPARK_YARN_MODE", "true")

      //      val sb = new StringBuilder
      //
      //      job.jars.foreach(jar => {
      //        sb ++= jar
      //        sb += ','
      //      })


      //conf.set("spark.yarn.dist.files", sb.substring(0, sb.length - 1))

      // need to initialize the ApplicationMaster
      //      val appargs = new ArrayBuffer[String]()
      //
      //      appargs += "--jar"
      //      appargs += job.driverJar
      //
      //      appargs += "--class"
      //      appargs += job.driverClass
      //
      //      appargs +=
      //      ApplicationMaster.main(job.toArgArray)
    }
    else if (job.isSpark) {
      conf.set("spark.driver.memory", if (job.memory != null) {
        job.memory
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
    val context = new SparkContext(conf)

    context
  }

  //  final def main(args:Array[String]): Unit = {
  //    val job:JobArguments = new JobArguments(args)
  //
  //    println("Starting application")
  //
  //    val conf:SparkConf = new SparkConf()
  //        .setAppName(System.getProperty("spark.app.name", "generic-spark-app"))
  //        .setMaster(System.getProperty("spark.master", "local[1]"))
  //    //        .setJars(System.getProperty("spark.jars", "").split(","))
  //L::
  //    val context:SparkContext = new SparkContext(conf)
  //    try
  //    {
  //      execute(context)
  //    }
  //    finally
  //    {
  //      println("Stopping spark context")
  //      context.stop()
  //    }
  //  }

}
