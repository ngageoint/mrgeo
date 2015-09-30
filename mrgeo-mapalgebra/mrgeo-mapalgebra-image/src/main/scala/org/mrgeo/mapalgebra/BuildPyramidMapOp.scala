package org.mrgeo.mapalgebra

import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.aggregators.{MeanAggregator, AggregatorRegistry}
import org.mrgeo.buildpyramid.BuildPyramid
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.{DataProviderNotFound, DataProviderFactory, ProviderProperties}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.image.MrsImagePyramidMetadata.Classification
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments

object BuildPyramidMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("buildpyramid", "bp")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new BuildPyramidMapOp(node, true, variables)
}

class BuildPyramidMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  private var aggregator: Option[String] = None

  var providerProperties:ProviderProperties = null


  private[mapalgebra] def this(node: ParserNode, isSlope: Boolean, variables: String => Option[ParserNode]) = {
    this()

    if ((node.getNumChildren < 1) || (node.getNumChildren > 2)) {
      throw new ParserException(
        "buildpyramid usage: buildpyramid(source raster, [aggregation type])")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    if (node.getNumChildren == 3) {
      aggregator = Some(MapOp.decodeString(node.getChild(1)) match {
      case Some(s) =>
        val clazz = AggregatorRegistry.aggregatorRegistry.get(s.toUpperCase)
        if (clazz != null) {
          s.toUpperCase
        }
        else {
          throw new ParserException("Invalid aggregator " + s)
        }
      case _ => throw new ParserException("Can't decode string")
      })
    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean = {
    val input: RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    rasterRDD = input.rdd()
    val meta = new MrsImagePyramidMetadata(input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName)))

    // Need to see if this is a saved pyramid.  If it is, we can buildpyramids, otherwise it is
    // a temporary RDD and we need to error out
    if (meta.getPyramid == null || meta.getPyramid.length == 0) {
      throw new DataProviderNotFound("Pyramid must exist (saved) before buildPyramid)! (unnamed layer)")
    }
    try {
      val dp = DataProviderFactory.getMrsImageDataProvider(meta.getPyramid, AccessMode.READ, providerProperties)

      if (aggregator.isDefined) {
        meta.setResamplingMethod(aggregator.get)
      }

      metadata(meta)

      val agg = {
        val clazz = AggregatorRegistry.aggregatorRegistry.get(meta.getResamplingMethod)

        if (clazz != null) {
          clazz.newInstance()
        }
        else {
          // if there is no aggregator and we are continuous, use the mean aggregator
          meta.getClassification match {
          case Classification.Continuous => new MeanAggregator
          case _ =>
            throw new IOException("Invalid aggregator " + meta.getResamplingMethod)
          }
        }
      }

      BuildPyramid.build(meta.getPyramid, agg, context, providerProperties)
      true
    }
    catch {
      case dpnf:DataProviderNotFound => throw new DataProviderNotFound("Pyramid must exist (saved) before buildPyramid)! (" + meta.getPyramid + ")", dpnf)
    }
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {

    providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))
    true
  }
  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

}
