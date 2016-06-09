package org.mrgeo.mapalgebra.vector

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.ProviderProperties
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.{MapAlgebra, MapOp}

class SaveVectorMapOp extends VectorMapOp with Externalizable {
  private var vectorRDD: Option[VectorRDD] = None
  private var input: Option[VectorMapOp] = None
  private var output:String = null

  private[mapalgebra] def this(inputMapOp:Option[VectorMapOp], name:String) = {
    this()

    input = inputMapOp
    output = name
  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 2) {
      throw new ParserException(node.getName + " takes 2 arguments")
    }

    input = VectorMapOp.decodeToVector(node.getChild(0), variables)
    output = MapOp.decodeString(node.getChild(1)) match {
      case Some(s) => s
      case _ => throw new ParserException("Error decoding String")
    }

  }

  override def rdd(): Option[VectorRDD] = vectorRDD

  override def execute(context: SparkContext): Boolean = {
    input match {
      case Some(v) =>
        v.save(output, providerProperties, context)
      case None => throw new IOException("Error saving vector")
    }

    true
  }

  var providerProperties:ProviderProperties = null

  override def setup(job: JobArguments, conf:SparkConf): Boolean = {
    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))
    true
  }

  override def teardown(job: JobArguments, conf:SparkConf): Boolean = true
  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

}
