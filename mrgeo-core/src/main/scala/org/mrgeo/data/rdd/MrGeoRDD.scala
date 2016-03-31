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

package org.mrgeo.data.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class MrGeoRDD[K:ClassTag, V:ClassTag](parent: RDD[(K, V)])  extends RDD[(K, V)](parent) {

  AutoPersister.incrementRef(this)
  walkTree(this)

  if (log.isDebugEnabled()) {
    logDebug("***************==")
    //    Thread.currentThread().getStackTrace.foreach(st => {
    //      logDebug(st.toString)
    //    })
    printDependencies(this)
    ///logDebug(this.toDebugString)
    logDebug("***************--")
  }

  private def walkTree(rdd: RDD[_], increment:Boolean = true): Boolean = {
    var lclinc = increment
    rdd.dependencies.foreach(dep => {
      dep.rdd match {
      case mrgeo:MrGeoRDD[_,_] =>
        if (increment) {
          AutoPersister.incrementRef(mrgeo)
          lclinc = false
        }
        else {
          AutoPersister.decrementRef(mrgeo)
        }
      case _ =>
      }})

      rdd.dependencies.foreach(dep => {
        walkTree(dep.rdd, lclinc)
      })

    lclinc
  }

//  override def finalize(): Unit = {
//    AutoPersister.decrementRef(this)
//  }

  private def printDependencies(rdd:RDD[_], level:Int = 0) {

    val sb = StringBuilder.newBuilder
    for (sp <- 0 until level) {
      sb ++= " "
    }
    sb ++= rdd.id + " (" + rdd.getClass.getSimpleName + ")"

    sb ++= "  " + (if (rdd.getStorageLevel != StorageLevel.NONE) rdd.getStorageLevel.description else "")
    rdd match {
    case mrgeo:MrGeoRDD[_,_] =>
      sb ++= " --- MrGeoRDD ref: " + AutoPersister.getRef(mrgeo)
    case _ =>
    }

    logDebug(sb.toString())

    rdd.dependencies.foreach(dep => {
      printDependencies(dep.rdd, level + 1)
    })
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[(K, V)].iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[(K, V)].partitions
  }


}
