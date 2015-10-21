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

package org.mrgeo.mapalgebra.vector

import java.io.IOException

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.VectorDataProvider
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.utils.SparkVectorUtils

object VectorDataMapOp {
  def apply(dataprovider: VectorDataProvider) = {
    new VectorDataMapOp(dataprovider)
  }

  def apply(mapop:MapOp):Option[VectorDataMapOp] = {
    mapop match {
      case rmo:VectorDataMapOp => Some(rmo)
      case _ => None
    }
  }
}

class VectorDataMapOp(dataprovider: VectorDataProvider) extends VectorMapOp {
  private var vectorRDD: Option[VectorRDD] = None

  def rdd(zoom:Int):Option[VectorRDD]  = {
    load(zoom)
    vectorRDD
  }

  def rdd():Option[VectorRDD] = {
    load()
    vectorRDD
  }

  private def load(zoom:Int = -1)  = {

    if (vectorRDD.isEmpty) {
      if (context == null) {
        throw new IOException("Error creating VectorRDD, can not create an RDD without a SparkContext")
      }

//      metadata(dataprovider.getMetadataReader.read())
      vectorRDD = Some(SparkVectorUtils.loadVectorRDD(dataprovider, context()))
    }

  }

//  override def metadata():Option[VectorMetadata] =  {
//    load()
//    super.metadata()
//  }

  // nothing to do here...
  override def setup(job: JobArguments, conf: SparkConf): Boolean = true
  override def execute(context: SparkContext): Boolean = true
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

}
