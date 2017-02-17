/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.mapalgebra.vector

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.ProviderProperties
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.{MapAlgebra, MapOp}

@SuppressFBWarnings(value = Array("UPM_UNCALLED_PRIVATE_METHOD"), justification = "Scala constant")
class SaveVectorMapOp extends VectorMapOp with Externalizable {
  var providerProperties:ProviderProperties = null
  private var vectorRDD:Option[VectorRDD] = None
  private var input:Option[VectorMapOp] = None
  private var output:String = null

  override def rdd():Option[VectorRDD] = vectorRDD

  override def execute(context:SparkContext):Boolean = {
    input match {
      case Some(v) =>
        v.save(output, providerProperties, context)
      case None => throw new IOException("Error saving vector")
    }

    true
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}

  private[mapalgebra] def this(inputMapOp:Option[VectorMapOp], name:String) = {
    this()

    input = inputMapOp
    output = name
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
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

}
