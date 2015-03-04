package org.mrgeo.spark.job

import org.apache.spark._
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}

import scala.collection.JavaConversions._

abstract class MrGeoJob extends Logging {
  def registerClasses(): Array[Class[_]]

  def setup(job: JobArguments): Boolean

  def execute(context: SparkContext): Boolean

  def teardown(job: JobArguments): Boolean

  private[job] def run(job:JobArguments, conf:SparkConf) = {
    // need to do this here, so we can call registerClasses() on the job.
    PrepareJob.setupSerializer(this, job, conf)

    println("Setting up job")
    setup(job)

    val context = new SparkContext(conf)
    try {
      println("Running job")
      execute(context)
    }
    finally {
      println("Stopping spark context")
      context.stop()
    }

    teardown(job)
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
