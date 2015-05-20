package org.mrgeo.spark.job.yarn

import java.io.File
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.apache.spark.SparkConf
import org.mrgeo.core.{MrGeoProperties, MrGeoConstants}
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.{GDALUtils, DependencyLoader, SparkUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object MrGeoYarnDriver {
  final val DRIVER:String = "mrgeo.driver.class"
}

class MrGeoYarnDriver {

  def run (job:JobArguments, cl:ClassLoader, conf:SparkConf) = {
    // need to get initialize spark.deploy.yarn... by reflection, because it is package private
    // to org.apache.spark

    // tell Spark we are running in yarn mode
    System.setProperty("SPARK_YARN_MODE", "true")

    val clientargsclazz = cl.loadClass("org.apache.spark.deploy.yarn.ClientArguments")

    if (clientargsclazz != null) {
      val caconst = clientargsclazz.getConstructor(classOf[Array[String]], classOf[SparkConf])
      val args = caconst.newInstance(toYarnArgs(job, cl, conf), conf)

      val clientclazz = cl.loadClass("org.apache.spark.deploy.yarn.Client")

      if (clientclazz != null) {
        val const = clientclazz.getConstructor(clientargsclazz, classOf[SparkConf])
        val client = const.newInstance(args.asInstanceOf[Object], conf)
        val run = clientclazz.getMethod("run")

        try {
          run.invoke(client)
        }
        catch {
          case e:Exception => {
            e.printStackTrace()
            throw e
          }
        }
      }
    }

  }

  def toYarnArgs(job:JobArguments, cl:ClassLoader, conf:SparkConf) :Array[String] = {
    val args = new ArrayBuffer[String]()

    //        "  --jar JAR_PATH             Path to your application's JAR file (required in yarn-cluster mode)\n" +
    //        "  --class CLASS_NAME         Name of your application's main class (required)\n" +
    //        "  --arg ARG                  Argument to be passed to your application's main class.\n" +
    //        "                             Multiple invocations are possible, each will be passed in order.\n" +
    //        "  --num-executors NUM        Number of executors to start (Default: 2)\n" +
    //        "  --executor-cores NUM       Number of cores for the executors (Default: 1).\n" +
    //        "  --driver-memory MEM        Memory for driver (e.g. 1000M, 2G) (Default: 512 Mb)\n" +
    //        "  --executor-memory MEM      Memory per executor (e.g. 1000M, 2G) (Default: 1G)\n" +
    //        "  --name NAME                The name of your application (Default: Spark)\n" +
    //        "  --queue QUEUE              The hadoop queue to use for allocation requests (Default: 'default')\n" +
    //        "  --addJars jars             Comma separated list of local jars that want SparkContext.addJar to work with.\n" +
    //        "  --files files              Comma separated list of files to be distributed with the job.\n" +
    //        "  --archives archives        Comma separated list of archives to be distributed with the job."

    println("Looking for 'javax.servlet'")
    val jars = SparkUtils.jarsForPackage("javax.servlet", cl)
    jars.foreach(jar => { "  " + println(jar)})

    val sparkClass = ApplicationMaster.getClass.getName.replaceAll("\\$", "")
    val sparkJar = SparkUtils.jarForClass(sparkClass, cl)
    conf.set("spark.yarn.jar", sparkJar)

    val deps = DependencyLoader.getDependencies(MrGeoYarnJob.getClass)
    val cpsb:StringBuilder = new StringBuilder

    for (jar <- deps) {
      //if (jar.contains("spark")) {
        if (cpsb.length > 0) {
          cpsb ++= ":"
        }
        cpsb ++= jar
      //}
    }
    conf.set("spark.driver.extraClassPath", cpsb.toString())
    conf.set("spark.executor.extraClassPath", cpsb.toString())

    conf.set("spark.driver.extraLibraryPath",
      MrGeoProperties.getInstance.getProperty(MrGeoConstants.GDAL_PATH, ""))
    conf.set("spark.executor.extraLibraryPath",
      MrGeoProperties.getInstance.getProperty(MrGeoConstants.GDAL_PATH, ""))

    conf.set("spark.eventLog.overwrite", "true") // overwrite event logs

    val driverClass = MrGeoYarnJob.getClass.getName.replaceAll("\\$","")
    val driverJar =  SparkUtils.jarForClass(driverClass, cl)


    args += "--class"
    args += driverClass

    args += "--jar"
    args += driverJar

    args += "--num-executors"
    args += "4" // job.executors.toString
    conf.set("spark.executor.instances", "4")// job.executors.toString)

    args += "--executor-cores"
    args += "2" //  job.cores.toString
    conf.set("spark.executor.cores", "2") // job.cores.toString)

    val exmemory:Int = SparkUtils.humantokb(job.memory)
    val dvmem = SparkUtils.kbtohuman(exmemory / (job.cores * job.executors))
    //val dvmem = SparkUtils.kbtohuman((Runtime.getRuntime.maxMemory() / 1024).toInt)

    //args += "--driver-memory"
    //args += dvmem
    conf.set("spark.driver.memory", dvmem)

    //args += "--executor-memory"
    //args += job.memory
    conf.set("spark.executor.memory", job.memory)


    args += "--name"
    if (job.name != null && job.name.length > 0) {
      args += job.name
    }
    else {
      args += "Unnamed MrGeo Job"
    }

    // need to make sure the driver jar isn't included.  Yuck!
    val driver = new File(driverJar).getName

    var clean = ""
    job.jars.foreach(jar => {
      if (!jar.contains(driver)) {
        if (clean.length > 0) {
          clean += ","
        }
        clean += jar
      }
    })


    args += "--addJars"
    args += clean

    args += "--arg"
    args += "--" + MrGeoYarnDriver.DRIVER

    args += "--arg"
    args += job.driverClass

    // map the user params
    job.params.foreach(p => {
      args += "--arg"
      args += "--" + p._1
      args += "--arg"
      args += p._2
    })

    args.toArray
  }


}
