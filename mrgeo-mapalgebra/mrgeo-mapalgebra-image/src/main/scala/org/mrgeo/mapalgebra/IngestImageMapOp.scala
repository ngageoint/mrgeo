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

package org.mrgeo.mapalgebra

import java.io._

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.ingest.{IngestImage, IngestInputProcessor}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.tms.TMSUtils

object IngestImageMapOp extends MapOpRegistrar {

  override def register: Array[String] = {
    Array[String]("ingest")
  }

  // Do not define any "create" methods here for the python interface because this map
  // algebra function is not meant to be exposed to Python. Python users should instead
  // call MrGeo.ingest_image(). Define createMapOp methods instead so that MrGeo.ingest_image
  // can call those methods.

  def createMapOp(input:String, zoom:Int, skip_preprocessing: Boolean, nodataOverride: Array[Double],
                  categorical:Boolean, skip_category_load: Boolean, protectionLevel: String):MapOp = {
    if (nodataOverride == null) {
      new IngestImageMapOp(input, Some(zoom), None, Some(skip_preprocessing), Some(categorical),
        Some(skip_category_load), protectionLevel)
    }
    else {
      val ndo = Array.ofDim[Double](nodataOverride.length)
      nodataOverride.zipWithIndex.foreach(U => {
        ndo(U._2) = U._1
      })
      new IngestImageMapOp(input, Some(zoom), Some(ndo), Some(skip_preprocessing), Some(categorical),
        Some(skip_category_load), protectionLevel)
    }
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new IngestImageMapOp(node, variables)
}

@SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "File() used for existance.  Actual file must be a geospatial file")
class IngestImageMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None

  private var inputs:Option[Array[String]] = None
  private var nodataOverride: Option[Array[Double]] = None
  private var categorical: Boolean = false
  private var skipPreprocessing: Boolean = false
  private var skipCategoryLoad = false
  private var zoom = -1
  private var protectionLevel: String = ""

  private[mapalgebra] def this(input:String, zoom:Option[Int], nodataOverride: Option[Array[Double]],
                               skipPreprocessing: Option[Boolean], categorical:Option[Boolean],
                               skipCategoryLoad:Option[Boolean], protectionLevel: String) = {
    this()
    val inputs = Array.ofDim[String](1)
    inputs(0) = input
    this.inputs = Some(inputs)
    this.categorical = categorical.getOrElse(false)
    this.skipCategoryLoad = skipCategoryLoad.getOrElse(false)
    this.skipPreprocessing = skipPreprocessing.getOrElse(false)
    this.zoom = zoom.getOrElse(-1)
    this.nodataOverride = nodataOverride
    this.protectionLevel = protectionLevel
  }

  private[mapalgebra] def this(inputs:Array[String], zoom:Option[Int], nodataOverride: Option[Array[Double]],
                               skipPreprocessing: Option[Boolean], categorical:Option[Boolean],
                               skipCategoryLoad:Option[Boolean], protectionLevel: String) = {
    this()
    this.inputs = Some(inputs)
    this.categorical = categorical.getOrElse(false)
    this.skipCategoryLoad = skipCategoryLoad.getOrElse(false)
    this.skipPreprocessing = skipPreprocessing.getOrElse(false)
    this.zoom = zoom.getOrElse(-1)
    this.nodataOverride = nodataOverride
    this.protectionLevel = protectionLevel
  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1 || node.getNumChildren > 5) {
      throw new ParserException("Usage: ingest(input(s), [zoom], [skippreprocessing], [nodata values], [categorical], [skipCategoryLoad], [protectionLevel]")
    }

    val in = Array.ofDim[String](1)
    in(0) = MapOp.decodeString(node.getChild(0), variables).getOrElse(throw new ParserException("Missing required input"))
    this.inputs = Some(in)

    if (node.getNumChildren >= 2) {
      zoom = MapOp.decodeInt(node.getChild(1), variables).getOrElse(
        throw new ParserException(f"Expected integer value for zoom instead of ${node.getChild(1).toString}"))
      if (zoom < 1 || zoom > TMSUtils.MAXZOOMLEVEL) {
        throw new ParserException(f"Invalid zoom ${node.getChild(1).toString}")
      }
    }

    if (node.getNumChildren >= 3) {
      skipPreprocessing = MapOp.decodeBoolean(node.getChild(2), variables).getOrElse(
        throw new ParserException(f"Expected boolean value for skippreprocessing instead of ${node.getChild(2).toString}"))
    }

    if (node.getNumChildren >= 4) {
      val str = MapOp.decodeString(node.getChild(3), variables).getOrElse(throw new ParserException("Missing nodata values"))
      if (str.trim.length > 0) {
        val strElements = str.split(",")
        val nodataOverrideValues = Array.ofDim[Double](strElements.length)
        for (i <- 0 until nodataOverrideValues.length) {
          try {
            nodataOverrideValues(i) = parseNoData(strElements(i));
          }
          catch {
            case nfe: NumberFormatException => {
              throw new ParserException("Invalid nodata value " + strElements(i))
            }
          }
        }
        nodataOverride = Some(nodataOverrideValues)
      }
    }

    if (node.getNumChildren >= 5) {
      categorical = MapOp.decodeBoolean(node.getChild(4), variables).getOrElse(
        throw new ParserException(f"Expected boolean value for categorical instead of ${node.getChild(4).toString}"))
    }

    if (node.getNumChildren >= 6) {
      skipCategoryLoad = MapOp.decodeBoolean(node.getChild(5), variables).getOrElse(
        throw new ParserException(f"Expected boolean value for skipCategoryLoad instead of ${node.getChild(5).toString}"))
    }

    if (node.getNumChildren >= 7) {
      protectionLevel = MapOp.decodeString(node.getChild(6), variables).getOrElse(
        throw new ParserException(f"Expected string value for protectionLevel instead of ${node.getChild(6).toString}"))
    }
  }

  private def parseNoData(fromArg: String): Double =
  {
    val arg = fromArg.trim();
    if (arg.compareToIgnoreCase("nan") != 0)
    {
      return arg.toDouble
    }
    else
    {
      return Double.NaN;
    }
  }

  override def registerClasses(): Array[Class[_]] = {
    // IngestImage ultimately creates a WrappedArray of Array[String], WrappedArray is already
    // registered, so we need the Array[String]
    Array[Class[_]](classOf[Array[String]])
  }


  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {

    true
  }

  override def execute(context: SparkContext): Boolean = {

    val inputfiles = inputs.getOrElse(throw new IOException("Inputs not set"))
    val iip = new IngestInputProcessor(context.hadoopConfiguration,
      nodataOverride match {
        case None => null
        case Some(ndo) => ndo
      }, zoom, skipPreprocessing)
    inputfiles.foreach(input => {
      iip.processInput(input, true)
    })
    val result = IngestImage.ingest(context, iip.getInputs.toArray, iip.getZoomlevel, skipPreprocessing, iip.tilesize,
      categorical, skipCategoryLoad, iip.getNodata, protectionLevel)
    rasterRDD = result._1 match {
    case rrdd:RasterRDD =>
      rrdd.checkpoint()
      Some(rrdd)
    case _ => None
    }

    metadata(result._2 match {
    case md:MrsPyramidMetadata => md
    case _ => null
    })

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

}
