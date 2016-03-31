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

package org.mrgeo.utils

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector._
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
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
//    val conf1 = provider.setupOutput(context.hadoopConfiguration)
    val inputs = Set(provider.getPrefixedResourceName)
    val vifc = new VectorInputFormatContext(inputs, provider.getProviderProperties)
    val vfp = provider.getVectorInputFormatProvider(vifc)
    val job = Job.getInstance(context.hadoopConfiguration)
    val conf2 = vfp.setupJob(job, provider.getProviderProperties)
    VectorRDD(context.newAPIHadoopRDD(job.getConfiguration,
      classOf[VectorInputFormat],
      classOf[FeatureIdWritable],
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

  def save(features: VectorRDD, outputProvider: VectorDataProvider, context: SparkContext,
           providerproperties:ProviderProperties): Unit = {

    features.persist(StorageLevel.MEMORY_AND_DISK_SER)

//    val output = outputProvider.getResourceName
//    val tofc = new VectorOutputFormatContext(output)
//    val tofp = outputProvider.getVectorOutputFormatProvider(tofc)
//    val job = Job.getInstance(context.hadoopConfiguration)
//    tofp.setupJob(job)

//    println("Num features: " + features.count())
//    features.saveAsNewAPIHadoopDataset(job.getConfiguration)
    val writer = outputProvider.getVectorWriter
    try {
      // TODO: The following call to collect introduces a limitation on how much
      // vector data can be saved because all the data will have to be brought
      // back to this node, and thus it must fit into memory. However, for current
      // requirements regarding vector data output, this is acceptable.
      features.collect.foreach(U => {
        writer.append(U._1, U._2)
      })
    } finally {
      writer.close()
    }
    features.unpersist()
  }
}
