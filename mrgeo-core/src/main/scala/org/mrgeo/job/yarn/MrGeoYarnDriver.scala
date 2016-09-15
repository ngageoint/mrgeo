/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.job.yarn

import java.io.{File, PrintWriter}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.SparkConf
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.job.JobArguments
import org.mrgeo.utils.SparkUtils

import scala.collection.mutable.ArrayBuffer

object MrGeoYarnDriver {
  final val DRIVER:String = "mrgeo.driver.class"

  final val ARGFILE:String = "mrgeo-argfile"
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
          case e:Exception =>
            e.printStackTrace()
            throw e
        }
      }
    }

  }

  @SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "Using File to strip path from a file")
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
    //        "  --files files              Comma separated list of files to be distributed with the job." +
    //        "  --archives archives        Comma separated list of archives to be distributed with the job."

    //    println("Looking for 'javax.servlet'")
    //    val jars = SparkUtils.jarsForPackage("javax.servlet", cl)
    //    jars.foreach(jar => { "  " + println(jar)})

    //    val sparkClass = ApplicationMaster.getClass.getName.replaceAll("\\$", "")
    //    val sparkJar = SparkUtils.jarForClass(sparkClass, cl)
    //    conf.set("spark.yarn.jar", sparkJar)
    //    conf.set("spark.yarn.jar", "/home/hadoop/spark/lib/spark-assembly-1.3.1-hadoop2.4.0.jar")

    val driverClass = MrGeoYarnJob.getClass.getName.replaceAll("\\$","")
    val driverJar =  SparkUtils.jarForClass(driverClass, cl)

    args += "--class"
    args += driverClass

    args += "--jar"
    args += driverJar

    // if dynamic allocation is _not_ enabled, we need to set the num-executors
    if (!conf.getBoolean("spark.dynamicAllocation.enabled", defaultValue = false)) {
      //val executors = conf.get("spark.executor.instances", "2").toInt
      // For Spark 1.6.0, passing --num-executors no longer works. Instead, you
      // have to set the spark.executor.instances configuration setting.
      conf.set("spark.executor.instances", job.executors.toString)
      args += "--num-executors"
      //args += executors.toString
      args += job.executors.toString
    }
    args += "--executor-cores"
    args += job.cores.toString

    // spark.executor.memory is the total memory available to spark,
    // --executor-memory is the memory per executor.  Go figure...
    conf.set("spark.executor.memory", SparkUtils.kbtohuman(job.memoryKb, "m"))
    args += "--executor-memory"
    args += SparkUtils.kbtohuman(job.executorMemKb, "m")

    args += "--driver-cores"
    args += "1"

    args += "--driver-memory"
    // don't need that much memory on the driver...
    args += "1024m" // SparkUtils.kbtohuman(job.executorMemKb, "m")

    args += "--name"
    if (job.name != null && job.name.length > 0) {
      args += job.name
    }
    else {
      args += "Unnamed MrGeo Job"
    }

    // need to make sure the driver jar isn't included.  Yuck!
    val driver = new File(driverJar).getName

    val clean = Set.newBuilder[String]
    job.jars.foreach(jar => {
      if (!jar.contains(driver)) {
        clean += jar
      }
    })

    args += "--addJars"
    args += clean.result().mkString(",")

    args += "--arg"
    args += "--" + MrGeoYarnDriver.DRIVER

    args += "--arg"
    args += job.driverClass

    // this is silly, but arguments are passed simply as parameters to the java process that is spun
    // off as the spark driver.  We need to make sure the command line isn't too long.  If it is, we'll
    // put the params into a file that we'll send along to the driver...
    var length:Int = 0
    job.params.foreach(p => {
      length += 12 +       // 12 for the other text needed in the args
          p._1.length +
          { if (p._2 != null) { p._2.length } else 4 } // length of "null"
    })

    // 100K is kinda arbitrary, but it's a nice pretty number
    if (length < 100000) {
      // map the user params
      job.params.foreach(p => {
        args += "--arg"
        args += "--" + p._1
        args += "--arg"
        args += p._2
      })
    }
    else {
      val temp = HadoopFileUtils.createUniqueTmpPath()
      val fs = HadoopFileUtils.getFileSystem(temp)

      val stream = fs.create(temp, true)
      val out = new PrintWriter(stream)
      job.params.foreach(p => {
        out.println("--" + p._1)
        out.println(p._2)
      })
      out.close()

      args += "--arg"
      args += "--" + MrGeoYarnDriver.ARGFILE
      args += "--arg"
      args += temp.toUri.toString
    }

    args.toArray
  }


}
