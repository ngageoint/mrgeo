package org.mrgeo.spark.job

import java.io.File
import java.net.URL

import org.apache.hadoop.util.ClassUtil
import org.apache.spark.SparkConf
import org.mrgeo.utils.DependencyLoader

import scala.collection.JavaConversions._
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader
import scala.util.control.Breaks

abstract class MrGeoDriver {

  def run(args:Array[String] = new Array[String](0)) = {
    val job = new JobArguments(args)

    val classname = getClass.getName
    if (classname.endsWith("$")) {
      job.driverClass = classname.substring(0, classname.lastIndexOf("$"))
    }
    else {
      job.driverClass = classname
    }

    println("Configuring application")

    setupDependencies(job)


    var parentLoader:ClassLoader = Thread.currentThread().getContextClassLoader
    if (parentLoader == null) {
      parentLoader = getClass.getClassLoader
    }

    val urls = job.jars.map(jar => new URL(jar))
    //urls.foreach(jar => println(jar.toString))

    val cl = new URLClassLoader(urls.toSeq, parentLoader)

    if (job.isYarn) {
      getYarnSettings(job, cl)
      addYarnClasses(cl)
    }
    val jobclass = cl.loadClass(job.driverClass)

    val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoJob]
    jobinstance.run(job.toArgArray)
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

  private def humantokb(human:String):Int = {
    //val pre: Char = new String ("KMGTPE").charAt (exp - 1)

    val trimmed = human.trim.toLowerCase
    val units = trimmed.charAt(human.length - 1)
    val exp = units match {
    case 'k' => 0
    case 'm' => 1
    case 'g' => 2
    case 'p' => 3
    case 'e' => 4
    case _ => return trimmed.substring(0, trimmed.length - 2).toInt
    }

    val mult = Math.pow(1024, exp).toInt

    trimmed.substring(0, trimmed.length - 2).toInt * mult
  }

  private def kbtohuman(kb:Int):String = {

    val unit = 1024
    val kbunit = unit * unit
    val exp: Int = (Math.log(kb) / Math.log(kbunit)).toInt
    val pre: Char = new String("MGTPE").charAt(exp)

    "%d%s".format((kb / Math.pow(unit, exp + 1)).toInt, pre)
  }

  private def getYarnSettings(job:JobArguments, cl: URLClassLoader) = {
    // need to get the Yarn config by reflection, since we support non YARN setups

    val clazz = getClass.getClassLoader.loadClass("org.apache.hadoop.yarn.conf.YarnConfiguration")

    if (clazz != null) {
      val conf = clazz.newInstance()
      val getInt = clazz.getMethod("getInt", classOf[String], classOf[Int])

      val defcores:Integer = 1
      val cores = getInt.invoke(conf, "yarn.nodemanager.resource.cpu-vcores", defcores).asInstanceOf[Int]
      if (job.cores <= 0 || job.cores < cores) {
        job.cores = cores
      }

      val sparkConf:SparkConf = new SparkConf()

      // Additional memory to allocate to containers
      // For now, use driver's memory overhead as our AM container's memory overhead
      val amMemoryOverhead = sparkConf.getInt("spark.yarn.driver.memoryOverhead", 384)
      val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead", 364)

      val defmem:Integer = 2048
      val mem = (((getInt.invoke(conf, "yarn.nodemanager.resource.memory-mb", defmem).asInstanceOf[Int] * 1024)
      - executorMemoryOverhead - amMemoryOverhead) * 0.95).toInt

      if (job.memory != null) {
        val exmem = humantokb(job.memory)

        if (mem < exmem) {
          job.memory = kbtohuman(mem)
        }

      }
      else {
        job.memory = kbtohuman(mem)
      }
    }
  }

  protected def setupDependencies(job:JobArguments): Unit = {

    val dependencies = DependencyLoader.getDependencies(getClass)
    val jars:StringBuilder = new StringBuilder

    for (jar <- dependencies) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      //if (!jar.contains("spark-yarn")) {
      if (jars.length > 0) {
        jars ++= ","
      }
      jars ++= jar
      //}
    }
    job.setJars(jars.toString())

    job.driverJar = getClass.getProtectionDomain.getCodeSource.getLocation.getPath

    if (!job.driverJar.endsWith(".jar")) {

      val jar: String = ClassUtil.findContainingJar(getClass)

      if (jar != null) {
        job.driverJar = jar
      }
      else {
        // now the hard part, need to look in the dependencies...

        //val urls:Array[URL] = Array.ofDim(dependencies.size())
        val urls = dependencies.map(jar => new File(jar).toURI.toURL)

        val cl = new URLClassLoader(urls.toSeq, Thread.currentThread().getContextClassLoader)

        val classFile: String = getClass.getName.replaceAll("\\.", "/") + ".class"
        val iter =  cl.getResources(classFile)

        val break = new Breaks

        break.breakable(
          while (iter.hasMoreElements) {
            val url: URL = iter.nextElement
            if (url.getProtocol == "jar") {
              val path: String = url.getPath
              if (path.startsWith("file:")) {
                // strip off the "file:" and "!<classname>"
                job.driverJar = path.substring("file:".length).replaceAll("!.*$", "")

                break.break()
              }
            }
          })
      }

    }



  }

}
