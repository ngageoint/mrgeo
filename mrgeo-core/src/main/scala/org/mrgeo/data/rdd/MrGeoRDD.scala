package org.mrgeo.data.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class MrGeoRDD[K:ClassTag, V:ClassTag](parent: RDD[(K, V)])  extends RDD[(K, V)](parent) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[(K, V)].iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[(K, V)].partitions
  }
}
