package org.mrgeo.job

import org.apache.spark.{SparkConf, SparkContext}

abstract class MrGeoJob
{
  def setup(job:JobArguments): Boolean
  def execute(context:SparkContext):Boolean
  def teardown(job:JobArguments): Boolean

  final def run(args:Array[String] = new Array[String](0)) = {
    val job = new JobArguments(args)


    //job.useDebug()
    //job.useLocal()
    //job.useYarn()

    val classname = getClass.getName
    if (classname.endsWith("$"))
    {
      job.driverClass = classname.substring(0, classname.lastIndexOf("$"))
    }
    else
    {
      job.driverClass = classname
    }

    // set the driver jar

    setup(job)
    SubmitJob.run(job)
    teardown(job)
  }

  final def main(args:Array[String]): Unit = {
    val job:JobArguments = new JobArguments(args)

    println("Starting application")

    val conf:SparkConf = new SparkConf()
        .setAppName(System.getProperty("spark.app.name", "generic-spark-app"))
        .setMaster(System.getProperty("spark.master", "local[1]"))
//        .setJars(System.getProperty("spark.jars", "").split(","))

    val context:SparkContext = new SparkContext(conf)
    try
    {
      execute(context)
    }
    finally
    {
      println("Stopping spark context")
      context.stop()
    }
  }
}
