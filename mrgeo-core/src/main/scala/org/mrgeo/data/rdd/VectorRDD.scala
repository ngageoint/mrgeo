package org.mrgeo.data.rdd

import org.apache.hadoop.io.LongWritable
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.mrgeo.geometry.Geometry

object VectorRDD {
  def apply(parent: VectorRDD): VectorRDD = {
    new VectorRDD(parent)
  }
  def apply(parent: RDD[(LongWritable, Geometry)]): VectorRDD = {
    new VectorRDD(parent)
  }
}

class VectorRDD(parent: RDD[(LongWritable, Geometry)]) extends RDD[(LongWritable, Geometry)](parent) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(LongWritable, Geometry)] = {
    firstParent[(LongWritable, Geometry)].iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[(LongWritable, Geometry)].partitions
  }
}
