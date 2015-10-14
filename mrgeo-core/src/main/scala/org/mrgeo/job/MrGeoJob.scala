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

import org.apache.spark._
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils

import scala.collection.JavaConversions._

abstract class MrGeoJob extends Logging {
  def registerClasses(): Array[Class[_]]

  def setup(job: JobArguments, conf:SparkConf): Boolean

  def execute(context: SparkContext): Boolean

  def teardown(job: JobArguments, conf:SparkConf): Boolean

  private[job] def run(job:JobArguments, conf:SparkConf) = {
    // need to do this here, so we can call registerClasses() on the job.
    PrepareJob.setupSerializer(this, job, conf)

    logInfo("Setting up job")
    setup(job, conf)

    val context = new SparkContext(conf)
    val checkpointDir = HadoopFileUtils.createJobTmp(context.hadoopConfiguration).toString

    try {
      logInfo("Running job")
      execute(context)
    }
    finally {
      logInfo("Stopping spark context")
      context.stop()

      HadoopFileUtils.delete(context.hadoopConfiguration, checkpointDir)
    }

    teardown(job, conf)
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
