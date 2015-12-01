/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.job

import java.io.File
import java.net.URL
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ClassUtil
import org.apache.hadoop.yarn.api.records.{NodeReport, NodeState}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.Logging
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.job.yarn.MrGeoYarnDriver
import org.mrgeo.utils._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

abstract class MrGeoDriver extends Logging {

  def setup(job: JobArguments): Boolean

  def run(name:String, driver:String = this.getClass.getName, args:Map[String, String] = Map[String, String](),
      hadoopConf:Configuration, additionalClasses: Option[Seq[Class[_]]] = None) = {
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
        addYarnClasses(cl)
      case "spark" =>
        val conf = PrepareJob.prepareJob(job)
        val master = conf.get("spark.master", "spark://localhost:7077")
        job.useSpark(master)
      case _ => job.useLocal()
      }
    }

    val conf = PrepareJob.prepareJob(job)

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




  protected def setupDependencies(job:JobArguments, hadoopConf:Configuration,
      additionalClasses: Option[Seq[Class[_]]] = None): mutable.Set[String] = {

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

    dependencies
  }

}
