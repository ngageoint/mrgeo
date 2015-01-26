package org.mrgeo.job

import java.io.File
import java.net.URL

import org.apache.hadoop.util.ClassUtil
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.utils.DependencyLoader
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
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


    val urls = job.jars.map(jar => new File(jar).toURI.toURL)
    val cl = new URLClassLoader(urls.toSeq, Thread.currentThread().getContextClassLoader)

    val jobclass = cl.loadClass(job.driverClass)

    val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoJob]
    jobinstance.run(job.toArgArray)
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

abstract class MrGeoJob  {
  def registerClasses(): Array[Class[_]]
  def setup(job:JobArguments): Boolean
  def execute(context:SparkContext):Boolean
  def teardown(job:JobArguments): Boolean

  private[job] def run(args:Array[String] = new Array[String](0)) = {
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


  final private def prepareJob(job:JobArguments): SparkContext = {
    val conf:SparkConf = new SparkConf()
        .setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
        .registerKryoClasses(registerClasses())
        //    .set("spark.driver.extraClassPath", "")
        //    .set("spark.driver.extraJavaOptions", "")
        //    .set("spark.driver.extraLibraryPath", "")

        .set("spark.storage.memoryFraction","0.25")

        // setup the kryo serializer
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        //.set("spark.kryo.registrator","org.mrgeo.job.KryoRegistrar"
        .set("spark.kryoserializer.buffer.mb","128")

        // driver memory and cores
        .set("spark.driver.memory", if (job.driverMem != null) job.driverMem else "128m")
        .set("spark.driver.cores", if (job.cores > 0) job.cores.toString else "1")

    if (job.isYarn) {
      //conf.set("spark.yarn.jar","")
      conf.set("spark.executor.memory", if (job.executorMem != null) job.executorMem else "128m")
          .set("spark.cores.max", if (job.executors > 0) job.executors.toString else "1")
      .setMaster(job.YARN + "-client")

      System.setProperty("SPARK_YARN_MODE", "true")
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
      //ApplicationMaster.main(appargs.toArray)
//      ApplicationMaster.main(job.toArgArray)
    }
    else if (job.isSpark) {
      conf.set("spark.driver.memory", if (job.driverMem != null) job.driverMem else "128m")
          .set("spark.driver.cores", if (job.cores > 0) job.cores.toString else "1")
    }

    for (j <- job.jars) { println(j)}
    new SparkContext(conf)
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
  //
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
