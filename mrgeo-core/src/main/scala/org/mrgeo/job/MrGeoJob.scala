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

package org.mrgeo.job

import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.ImageStats
import org.mrgeo.utils.Bounds

import scala.collection.mutable
import scala.reflect.ClassTag

object MrGeoJob extends Logging {
    def setupSerializer(mrgeoJob: MrGeoJob, conf: SparkConf) = {
      val classes = Array.newBuilder[Class[_]]

      // automatically include common classes
      classes += classOf[TileIdWritable]
      classes += classOf[RasterWritable]

      classes += classOf[Array[(TileIdWritable, RasterWritable)]]
      classes += classOf[Array[TileIdWritable]]

      classes += classOf[Bounds]

      classes += classOf[ImageStats]
      classes += classOf[Array[ImageStats]]

      // include the old TileIdWritable & RasterWritable
      classes += classOf[org.mrgeo.core.mapreduce.formats.TileIdWritable]
      classes += classOf[org.mrgeo.core.mapreduce.formats.RasterWritable]

      // context.parallelize() calls create a WrappedArray.ofRef()
      classes += classOf[mutable.WrappedArray.ofRef[_]]

      // rdd.sortByKey() uses all these, yuck!
      classes += classOf[Array[(_, _, _)]]
      classes += classOf[ClassTag[_]]
      classes += Class.forName("scala.reflect.ClassTag$$anon$1") // this is silly!!!
      classes += classOf[Class[_]]


      // TODO:  Need to call DataProviders to register classes
      classes += classOf[FileSplitInfo]


      classes ++= mrgeoJob.registerClasses()

      registerClasses(classes.result(), conf)
    }

    def registerClasses(classes: Array[Class[_]], conf: SparkConf) = {
      if (MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_USE_KRYO, "false").equals("true")) {
        try {
          val all = mutable.HashSet.empty[String]
          all ++= conf.get("spark.kryo.classesToRegister", "").split(",").filter(!_.isEmpty)

          all ++= classes.filter(!_.getName.isEmpty).map(_.getName)

          conf.set("spark.kryo.classesToRegister", all.mkString(","))
          conf.set("spark.serializer", classOf[KryoSerializer].getName)
        }
        catch {
          case nsme: NoSuchMethodException => conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
          case e: Exception => e.printStackTrace()
        }
      }
      else {
        conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      }
    }

  }

abstract class MrGeoJob extends Logging {
  def registerClasses(): Array[Class[_]]

  def setup(job: JobArguments, conf:SparkConf): Boolean

  def execute(context: SparkContext): Boolean

  def teardown(job: JobArguments, conf:SparkConf): Boolean

  private[job] def run(job:JobArguments, conf:SparkConf) = {
    // need to do this here, so we can call registerClasses() on the job.
    MrGeoJob.setupSerializer(this, conf)

    logInfo("Setting up job")
    setup(job, conf)

    val context = new SparkContext(conf)

    //context.addSparkListener(new MrGeoListener(context))

    val checkpointDir = HadoopFileUtils.createJobTmp(context.hadoopConfiguration).toString

    try {
      logInfo("Running job")
      context.setCheckpointDir(checkpointDir)
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
