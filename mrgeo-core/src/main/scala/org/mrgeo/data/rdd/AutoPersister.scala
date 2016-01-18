package org.mrgeo.data.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object AutoPersister {
  val defaultStorageLevel = StorageLevel.MEMORY_AND_DISK_SER

  def persist(rdd:RDD[_], storageLevel: StorageLevel = defaultStorageLevel) = {
    //rdd.id

    rdd.persist(defaultStorageLevel)
  }

  def unpersist(rdd:RDD[_]) = {
    rdd.unpersist()
  }
}
