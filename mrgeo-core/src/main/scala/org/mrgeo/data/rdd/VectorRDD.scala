package org.mrgeo.data.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.mrgeo.mapreduce.GeometryWritable

object VectorRDD {
  def apply(parent: VectorRDD): VectorRDD = {
    new VectorRDD(parent)
  }
  def apply(parent: RDD[(Long, GeometryWritable)]): VectorRDD = {
    new VectorRDD(parent)
  }
}

class VectorRDD(parent: RDD[(Long, GeometryWritable)]) extends RDD[(Long, GeometryWritable)](parent) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(Long, GeometryWritable)] = {
    firstParent[(Long, GeometryWritable)].iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[(Long, GeometryWritable)].partitions
  }
}
