package org.mrgeo.spark.job

import java.io.{IOException, FileInputStream, InputStreamReader, File}
import java.util.Properties

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Logging, SparkException, SparkConf, SparkContext}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.spark.job.yarn.MrGeoYarnJob
import org.mrgeo.utils.SparkUtils

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

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

      val dvmem = 1024 * 1024
      conf.set("spark.driver.memory", SparkUtils.kbtohuman(dvmem, "m"))
      conf.set("spark.driver.cores", "1")

      val exmem = SparkUtils.kbtohuman(job.memoryKb - dvmem, "m") //  / (job.cores * job.executors)) - dvmem)
      conf.set("spark.executor.memory", exmem)
      conf.set("spark.executor.instances", job.executors.toString)
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

//    // only 25% of storage is for caching datasets, 75% is available for processing
//    if (job.isMemoryIntensive) {
//      //conf.set("spark.storage.memoryFraction", "0.25")
//      //conf.set("spark.shuffle.memoryFraction", "0.33")
//    }


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
