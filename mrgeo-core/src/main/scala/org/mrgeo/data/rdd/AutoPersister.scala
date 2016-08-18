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

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.mrgeo.utils.Logging

import scala.collection.mutable

object AutoPersister extends Logging {

  val references = mutable.Map.empty[Int, Int]

  val defaultStorageLevel = StorageLevel.MEMORY_AND_DISK_SER

  def getRef(rdd:RDD[_]): Int = {
    references.getOrElse(rdd.id, 0)
  }

  // force a persist
  def persist(rdd:RDD[_], storageLevel: StorageLevel = defaultStorageLevel) = {
    rdd.persist(storageLevel)
    incrementRef(rdd)
  }

  // force an unpersist
  def unpersist(rdd:RDD[_]) = {
    rdd.unpersist()
    references.remove(rdd.id)
  }

  // decrement the ref count, unpersist if needed
  def decrementRef(rdd:RDD[_]):Int = {
    val cnt = references.getOrElse(rdd.id, return 0) - 1
    logDebug("decrement ref: " + rdd.id + " from: " + (cnt + 1) + " to: " + cnt + (if (cnt <= 0) " unpersisting" else ""))

    references.put(rdd.id, cnt)
    if (cnt <= 0 && rdd.getStorageLevel != StorageLevel.NONE) {
      rdd.unpersist()
      references.remove(rdd.id)
    }

    cnt
  }

  // increment the ref count, persist when the count hits 2 (no need to persist over that)
  def incrementRef(rdd:RDD[_], storageLevel: StorageLevel = defaultStorageLevel):Int = {

    val cnt = references.getOrElseUpdate(rdd.id, 0) + 1

    logDebug("increment ref: " + rdd.id + " from: " + (cnt - 1) + " to: " + cnt + (if (cnt == 2) " persisting" else ""))
    references.put(rdd.id, cnt)

    if (cnt == 2 && rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    }
    cnt
  }

}
