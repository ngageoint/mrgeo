package org.mrgeo.utils

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.{VectorInputFormat, VectorInputFormatContext, VectorDataProvider}
import org.mrgeo.geometry.Geometry
import scala.collection.JavaConversions._

object SparkVectorUtils
{
  def loadVectorRDD(input: String, providerProperties: ProviderProperties,
                    context: SparkContext): VectorRDD = {
    val dp: VectorDataProvider = DataProviderFactory.getVectorDataProvider(input,
      DataProviderFactory.AccessMode.READ, providerProperties)

    loadVectorRDD(dp, context)
  }

  def loadVectorRDD(provider:VectorDataProvider, context: SparkContext): VectorRDD = {
//    val conf1 = provider.setupSparkJob(context.hadoopConfiguration)
    val inputs = Set(provider.getResourceName)
    val vifc = new VectorInputFormatContext(inputs, provider.getProviderProperties)
    val vfp = provider.getVectorInputFormatProvider(vifc)
    val job = Job.getInstance(context.hadoopConfiguration)
    val conf2 = vfp.setupJob(job, provider.getProviderProperties)
    VectorRDD(context.newAPIHadoopRDD(job.getConfiguration,
      classOf[VectorInputFormat],
      classOf[LongWritable],
      classOf[Geometry]))
  }

  def calculateBounds(rdd: VectorRDD): Bounds = {

    val bounds = rdd.aggregate(new Bounds())((bounds, geom) => {
      bounds.expand(geom._2.getBounds)
      bounds
    }
      ,
      (b1, b2) => {
        b1.expand(b2)

        b1
      })

    bounds
  }
}
