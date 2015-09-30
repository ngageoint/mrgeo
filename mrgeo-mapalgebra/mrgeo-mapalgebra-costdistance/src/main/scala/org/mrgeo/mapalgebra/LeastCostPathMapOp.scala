package org.mrgeo.mapalgebra

import java.io.{ObjectInput, ObjectOutput, Externalizable}

import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.job.JobArguments

object LeastCostPathMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("leastCostPath", "lcp")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new LeastCostPathMapOp(node, variables)
}

// TODO:  Complete this when VectorMapOp is complete
class LeastCostPathMapOp extends MapOp with Externalizable {

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true
  override def execute(context: SparkContext): Boolean = true
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true
  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}
}