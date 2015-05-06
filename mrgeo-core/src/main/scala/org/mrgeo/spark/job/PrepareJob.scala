package org.mrgeo.spark.job

import java.io.{IOException, FileInputStream, InputStreamReader, File}
import java.util.Properties

import org.apache.spark.{Logging, SparkException, SparkConf, SparkContext}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

object PrepareJob extends Logging {

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

  private[job] def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {
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


  final def prepareJob(job: JobArguments): SparkConf = {

    val conf: SparkConf = new SparkConf()

    loadDefaultSparkProperties(conf)


    logInfo("spark.app.name: " + conf.get("spark.app.name", "<not set>") + "  job.name: " + job.name)
    conf.setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
        //.registerKryoClasses(registerClasses())
        //    .set("spark.driver.extraClassPath", "")
        //    .set("spark.driver.extraJavaOptions", "")
        //    .set("spark.driver.extraLibraryPath", "")

        .set("spark.storage.memoryFraction", "0.25")

    if (job.isYarn) {
      conf.set("spark.yarn.am.cores", if (job.cores > 0) { job.cores.toString } else { "1" })
          .set("spark.executor.memory", if (job.memory != null) { job.memory } else { "128m" })
          .set("spark.executor.cores", if (job.cores > 0) { job.cores.toString } else { "1" })
          .set("spark.cores.max", if (job.cores > 0) { job.cores.toString } else { "1" })
          .set("spark.yarn.preserve.staging.files", "true")
          // running in "cluster" mode, the driver runs within a YARN process
          .setMaster(job.YARN + "-cluster")
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

    conf
  }

  def setupSerializer(mrgeoJob: MrGeoJob, job:JobArguments, conf:SparkConf) = {
    // we need to check the serializer property, there is a bug in the registerKryoClasses in Spark < 1.3.0 that
    // causes a ClassNotFoundException.  So we need to add a config property to use/ignore kryo
    if (MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_USE_KRYO, "false").equals("true")) {
      // Check and invoke for registerKryoClasses() with reflection, because isn't in pre Spark 1.2.0
      try {
        val method = conf.getClass.getMethod("registerKryoClasses", classOf[Array[Class[_]]])
        method.invoke(conf, mrgeoJob.registerClasses())
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
