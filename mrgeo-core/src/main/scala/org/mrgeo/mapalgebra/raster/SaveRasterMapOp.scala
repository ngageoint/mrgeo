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

package org.mrgeo.mapalgebra.raster

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.ProviderProperties
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.{MapAlgebra, MapOp}
import org.mrgeo.publisher.MrGeoPublisherFactory
import scala.collection.JavaConverters._


class SaveRasterMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None
  private var input: Option[RasterMapOp] = None
  private var output:String = null
  private var publishImage: Boolean = false

  private[mapalgebra] def this(inputMapOp:Option[RasterMapOp], name:String, publishImage: Boolean = false) = {
    this()

    input = inputMapOp
    output = name
    this.publishImage = publishImage

  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 2) {
      throw new ParserException(node.getName + " takes 2 arguments")
    }

    input = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    output = MapOp.decodeString(node.getChild(1)) match {
    case Some(s) => s
    case _ => throw new ParserException("Error decoding String")
    }
    if (node.getNumChildren == 3) {
      publishImage = MapOp.decodeBoolean(node.getChild(2)).asInstanceOf[Boolean]
    }

  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean = {
    input match {
    case Some(pyramid) =>

      rasterRDD = pyramid.rdd()
      val meta = new MrsPyramidMetadata(pyramid.metadata() getOrElse (throw new IOException("Can't load metadata! Ouch! " + pyramid.getClass.getName)))

      // set the pyramid name to the output
      meta.setPyramid(output)
      metadata(meta)

      pyramid.save(output, providerProperties, context)
      if (publishImage) {
        MrGeoPublisherFactory.getAllPublishers.asScala.foreach(_.publishImage(output, meta))
      }

    case None => throw new IOException("Error saving raster")
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
