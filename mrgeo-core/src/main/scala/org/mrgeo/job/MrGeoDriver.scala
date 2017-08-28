/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.job

import java.io.{File, IOException}
import java.net.URL
import java.security.{AccessController, PrivilegedAction, PrivilegedActionException}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ClassUtil
import org.apache.spark.SparkConf
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.job.yarn.MrGeoYarnDriver
import org.mrgeo.utils._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

object MrGeoDriver extends Logging {
  final def prepareJob(job:JobArguments):SparkConf = {

    val conf = SparkUtils.getConfiguration

    logInfo("spark.app.name: " + conf.get("spark.app.name", "<not set>") + "  job.name: " + job.name)
    conf.setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
    //.registerKryoClasses(registerClasses())

    if (job.isYarn) {
      // running in "cluster" mode, the driver runs within a YARN process
      conf.setMaster(job.YARN + "-cluster")

      conf.set("spark.submit.deployMode", "cluster")

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
    else {
      conf.set("spark.ui.enabled", "false")
    }

    //    val fracs = calculateMemoryFractions(job)
    //    conf.set("spark.storage.memoryFraction", fracs._1.toString)
    //    conf.set("spark.shuffle.memoryFraction", fracs._2.toString)

    conf
  }

}

@SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"),
  justification = "addYarnClasses() - Using File() as a shortcut to create a URI")
abstract class MrGeoDriver extends Logging {

  def setup(job:JobArguments):Boolean

  def run(name:String, driver:String = this.getClass.getName, args:Map[String, String] = Map[String, String](),
          hadoopConf:Configuration, additionalClasses:Option[scala.collection.immutable.Set[Class[_]]] = None) = {
    val job = new JobArguments()

    job.driverClass = driver

    job.name = name
    job.setAllSettings(args)
    job.addMrGeoProperties()
    val dpfProperties = DataProviderFactory.getConfigurationFromProviders
    job.setAllSettings(dpfProperties.toMap)


    logInfo("Configuring application")

    // setup dependencies, but save the local deps to use in the classloader
    val local = setupDependencies(job, hadoopConf, additionalClasses)

    var parentLoader:ClassLoader = Thread.currentThread().getContextClassLoader
    if (parentLoader == null) {
      parentLoader = getClass.getClassLoader
    }

    val urls = local.map(jar => {
      new URL(FileUtils.resolveURL(jar))
    })

    try {
      AccessController.doPrivileged(new PrivilegedAction[Any] {
        @SuppressFBWarnings(
          value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"),
          justification = "Scala generated code")
        override def run():Boolean = {
          val cl = new URLClassLoader(urls.toSeq, parentLoader)

          setupDriver(job, cl)

          setup(job)

          if (HadoopUtils.isLocal(hadoopConf)) {
            if (!job.isDebug) {
              job.useLocal()
            }
            else {
              job.useDebug()
            }
          }
          else {
            val cluster = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_CLUSTER, "local")

            cluster.toLowerCase match {
              case "yarn" =>
                job.useYarn()
                job.loadYarnSettings()
                addYarnClasses(cl)

              case "spark" =>
                val conf = MrGeoDriver.prepareJob(job)
                val master = conf.get("spark.master", "spark://localhost:7077")
                job.useSpark(master)
              case _ => job.useLocal()
            }
          }

          val conf = MrGeoDriver.prepareJob(job)

          // yarn needs to be run in its own client code, so we'll set up it up separately
          if (job.isYarn) {

            val jobclass = cl.loadClass(classOf[MrGeoYarnDriver].getCanonicalName)
            val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoYarnDriver]

            jobinstance.run(job, cl, conf)
          }
          else {
            val jobclass = cl.loadClass(job.driverClass)
            val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoJob]

            jobinstance.run(job, conf)
          }

          true
        }

      })
    }
    catch {
      // just unwrap the exception
      case pae:PrivilegedActionException => throw pae.getException
    }
  }

  def calculateMemoryFractions(job:JobArguments) = {
    val exmem = if (job.executorMemKb > 0) {
      job.executorMemKb
    }
    else {
      job.memoryKb
    }

    val pmem = SparkUtils
        .humantokb(MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_MAX_PROCESSING_MEM, "1G"))
    val shufflefrac = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_SHUFFLE_FRACTION, "0.5").toDouble

    if (shufflefrac < 0 || shufflefrac > 1) {
      throw new IOException(MrGeoConstants.MRGEO_SHUFFLE_FRACTION + " must be between 0 and 1 (inclusive)")
    }
    val cachefrac = 1.0 - shufflefrac

    val smemfrac = if (exmem > pmem * 2) {
      (exmem - pmem).toDouble / exmem.toDouble
    }
    else {
      0.5 // if less than 2x, 1/2 memory is for shuffle/cache
    }

    if (log.isInfoEnabled) {
      val cfrac = smemfrac * cachefrac
      val sfrac = smemfrac * shufflefrac
      val pfrac = 1 - (cfrac + sfrac)
      val p = (exmem * pfrac).toLong
      val c = (exmem * cfrac).toLong
      val s = (exmem * sfrac).toLong

      logInfo("total memory:            " + SparkUtils.kbtohuman(exmem, "m"))
      logInfo("mrgeo processing memory: " + SparkUtils.kbtohuman(p, "m") + " (" + pfrac + ")")
      logInfo("shuffle/cache memory:    " + SparkUtils.kbtohuman(exmem - p, "m") + " (" + smemfrac + ")")
      logInfo("    cache memory:   " + SparkUtils.kbtohuman(c, "m") + " (" + cfrac + ")")
      logInfo("    shuffle memory: " + SparkUtils.kbtohuman(s, "m") + " (" + sfrac + ")")
    }

    // storage, shuffle fractions
    (smemfrac * cachefrac, smemfrac * shufflefrac)

  }

  protected def setupDependencies(job:JobArguments, hadoopConf:Configuration,
                                  additionalClasses:Option[scala.collection.immutable.Set[Class[_]]] = None):mutable.Set[String] = {

    val oldprint = DependencyLoader.getPrintMissingDependencies

    DependencyLoader.setPrintMissingDependencies(false)
    DependencyLoader.resetMissingDependencyList()

    val dependencies = DependencyLoader.getDependencies(getClass)
    val qualified = DependencyLoader.copyDependencies(dependencies, hadoopConf)
    val jars:StringBuilder = new StringBuilder

    for (jar <- qualified) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      //if (!jar.contains("spark-yarn")) {
      if (jars.nonEmpty) {
        jars ++= ","
      }
      jars ++= jar
      //}
    }

    val dpfDependencies = DataProviderFactory.getDependencies
    val dpfQualified = DependencyLoader.copyDependencies(dpfDependencies, hadoopConf)
    for (jar <- dpfQualified) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      //if (!jar.contains("spark-yarn")) {
      if (jars.nonEmpty) {
        jars ++= ","
      }
      jars ++= jar
      //}
    }

    if (additionalClasses.isDefined) {
      var additionalDependencies = Set.newBuilder[String]
      additionalClasses.get.foreach(additionalClass => {
        additionalDependencies ++= DependencyLoader.getDependencies(additionalClass)
      })
      val additionalQualified = DependencyLoader.copyDependencies(additionalDependencies.result(), hadoopConf)
      additionalQualified.foreach(jar => {
        if (jars.nonEmpty) {
          jars ++= ","
        }
        jars ++= jar
      })
    }
    job.setJars(jars.toString())


    DependencyLoader.printMissingDependencies()
    DependencyLoader.setPrintMissingDependencies(oldprint)
    DependencyLoader.resetMissingDependencyList()
    dependencies
  }

  private def setupDriver(job:JobArguments, cl:URLClassLoader) = {
    if (job.driverClass == null) {
      job.driverClass = getClass.getName.replaceAll("\\$", "").replaceAll("\\$", "")
    }

    if (job.driverJar == null || !job.driverJar.endsWith(".jar")) {
      val clazz = Class.forName(job.driverClass)
      val jar:String = ClassUtil.findContainingJar(clazz)

      if (jar != null) {
        job.driverJar = jar
      }
      else {
        job.driverJar = SparkUtils.jarForClass(job.driverClass, cl)
      }
    }
  }

  private def addYarnClasses(cl:URLClassLoader) = {
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

}
