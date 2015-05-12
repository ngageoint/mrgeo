package org.mrgeo.spark.job

import java.io.File
import java.net.URL
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ClassUtil
import org.apache.hadoop.yarn.api.records.{NodeReport, NodeState}
import org.apache.spark.{Logging, SparkConf}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.spark.job.yarn.MrGeoYarnDriver
import org.mrgeo.utils.{FileUtils, HadoopUtils, SparkUtils, DependencyLoader}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

abstract class MrGeoDriver extends Logging {

  def run(name:String, driver:String = this.getClass.getName, args:Map[String, String] = Map[String, String](), hadoopConf:Configuration) = {
    val job = new JobArguments()

    job.driverClass = driver

    job.name = name
    job.setAllSettings(args)


    logInfo("Configuring application")

    // setup dependencies, but save the local deps to use in the classloader
    val local = setupDependencies(job)

    var parentLoader:ClassLoader = Thread.currentThread().getContextClassLoader
    if (parentLoader == null) {
      parentLoader = getClass.getClassLoader
    }

    val urls = local.map(jar => {
      new URL(FileUtils.resolveURL(jar))
    })

    val conf = PrepareJob.prepareJob(job)
    val cl = new URLClassLoader(urls.toSeq, parentLoader)

    setupDriver(job, cl)


    if (HadoopUtils.isLocal(hadoopConf))
    {
      job.useLocal()
    }
    else {
      val cluster = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_CLUSTER, "local")

      cluster.toLowerCase match {
      case "yarn" => job.useYarn()
      case "spark" => {
        val master = conf.get("spark.master", "spark://localhost:7077")
        job.useSpark(master)
      }
      case _ => job.useLocal()
      }
    }

    // yarn needs to be run in its own client code, so we'll set up it up separately
    if (job.isYarn) {
      loadYarnSettings(job, cl)
      addYarnClasses(cl)

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
    // need to get the YARN classes by reflection, since we support non YARN setups
    val clazz = getClass.getClassLoader.loadClass("org.apache.hadoop.yarn.conf.YarnConfiguration")

    if (clazz != null) {
      val conf = clazz.newInstance()
      val getInt = clazz.getMethod("getInt", classOf[String], classOf[Int])

      val defcores:Integer = 1
      val cores = getInt.invoke(conf, "yarn.nodemanager.resource.cpu-vcores", defcores).asInstanceOf[Int]
      if (job.cores <= 0 || job.cores < cores) {
        job.cores = cores
      }

      val res = calculateYarnResources()

      // TODO:  This calculation is just voodoo.  We need to figure out a better one
      job.cores = 2
      job.executors = res._1 / job.cores

      // This is what I _think_ these values should be, but job regularly fail with "out of memory"
      // errors when I set them this way...
      //job.executors = res._2 + 1 // total CPUs + 1 (for the driver)
      //job.cores = res._1 / job.executors

      val sparkConf:SparkConf = new SparkConf()

      // Additional memory to allocate to containers
      // For now, use driver's memory overhead as our AM container's memory overhead
      val amMemoryOverhead = sparkConf.getInt("spark.yarn.driver.memoryOverhead", 384)
      val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", 364)

      //      val defmem:Integer = 2048
      //      val mem = (((getInt.invoke(conf, "yarn.nodemanager.resource.memory-mb", defmem).asInstanceOf[Int] * 1024)
      //          - executorMemoryOverhead - amMemoryOverhead) * 0.95).toInt

      val mem = ((res._3 - executorMemoryOverhead - amMemoryOverhead) * 0.98).toInt
      job.memory = SparkUtils.kbtohuman(mem * 1024) // mem is in mb, convert to kb

      //      if (job.memory != null) {
      //        val exmem = SparkUtils.humantokb(job.memory)
      //
      //        if (mem < exmem) {
      //          job.memory = SparkUtils.kbtohuman(mem)
      //        }
      //      }
      //      else {
      //        job.memory = SparkUtils.kbtohuman(mem)
      //      }
    }
  }

  private def calculateYarnResources():(Int, Int, Int) = {

    val cl = getClass.getClassLoader

    var client:Class[_] = null
    try {
      client = cl.loadClass("org.apache.hadoop.yarn.client.api.YarnClient")
    }
    catch {
      // YarnClient was here in older versions of YARN
      case cnfe: ClassNotFoundException => {
        client = cl.loadClass("org.apache.hadoop.yarn.client.YarnClient")
      }
    }

    val create = client.getMethod("createYarnClient")
    val init = client.getMethod("init", classOf[Configuration])
    val start = client.getMethod("start")


    val getNodeReports = client.getMethod("getNodeReports", classOf[Array[NodeState]])
    val stop = client.getMethod("stop")


    val yc = create.invoke(null)
    init.invoke(yc, HadoopUtils.createConfiguration())
    start.invoke(yc)

    val na = new Array[NodeState](1)
    na(0) = NodeState.RUNNING

    val nr = getNodeReports.invoke(yc, na).asInstanceOf[util.ArrayList[NodeReport]]

    var totalcores:Int = 0
    var totalmem:Long = 0

    nr.foreach(rep => {
      val res = rep.getCapability

      totalmem = totalmem + res.getMemory
      totalcores = totalcores + res.getVirtualCores
    })

    val executors:Int = nr.length

    stop.invoke(yc)

    (totalcores, executors, totalmem.toInt)

    //
    //      val yc = YarnClient.createYarnClient()
    //      yc.init(HadoopUtils.createConfiguration())
    //      yc.start()
    //      val nr = yc.getNodeReports(NodeState.RUNNING)
    //
    //      var totalcores:Int = 0
    //      var totalmem:Long = 0
    //
    //      nr.foreach(rep => {
    //        val res = rep.getCapability
    //
    //        totalmem = totalmem + res.getMemory
    //
    //        totalcores = totalcores + res.getVirtualCores
    //
    //      })
    //      val executors:Int = nr.length
    //
    //      yc.stop()
    //
    //    }
    //    catch {
    //      case nsme:NoSuchMethodException => {
    //        val clazz = getClass.getClassLoader.loadClass("org.apache.hadoop.yarn.client.YarnClient")
    //        create = clazz.getMethod("createYarnConfig")
    //
    //      }
    //    }
    //
    //
    //
    //
    //   (totalcores, executors, (totalmem / 1024).toInt)
  }
  protected def setupDependencies(job:JobArguments): mutable.Set[String] = {

    val dependencies = DependencyLoader.getDependencies(getClass)
    val qualified = DependencyLoader.copyDependencies(dependencies)
    val jars:StringBuilder = new StringBuilder

    for (jar <- qualified) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      if (!jar.contains("spark-yarn")) {
        if (jars.length > 0) {
          jars ++= ","
        }
        jars ++= jar
      }
    }
    job.setJars(jars.toString())

    dependencies
  }

}
