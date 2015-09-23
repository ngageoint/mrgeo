package org.mrgeo.mapalgebra

import java.io.{IOException, ObjectOutput, ObjectInput, Externalizable}

import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.aggregators.{Aggregator, AggregatorRegistry}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsImagePyramidMetadata.Classification
import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.TMSUtils

object ChangeClassificationMapOp extends MapOpRegistrar {

  override def register: Array[String] = {
    Array[String]("changeClassification")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new ChangeClassificationMapOp(node, variables)
}

class ChangeClassificationMapOp extends RasterMapOp with Externalizable {

  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  private var classification: Option[Classification] = None
  private var aggregator: Option[String] = None

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if ((node.getNumChildren < 2) || (node.getNumChildren > 3)) {
      throw new ParserException(
        "ChangeClassificationMapOp usage: changeClassification(source raster, type, [aggregation type])");
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    classification = Some(MapOp.decodeString(node.getChild(1), variables) match {
    case Some(s) => s match {
    case "categorical" => Classification.Categorical
    case _ => Classification.Continuous
    }
    case _ => throw new ParserException("Can't decode string")
    })


    if (node.getNumChildren == 3) {
      aggregator = Some(MapOp.decodeString(node.getChild(2)) match {
      case Some(s) =>
        val clazz = AggregatorRegistry.aggregatorRegistry.get(s.toUpperCase)
        if (clazz != null) {
          s
        }
        else {
          throw new ParserException("Invalid aggregator " + s)
        }
      case _ => throw new ParserException("Can't decode string")
      })
    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {
    val input: RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse
        (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    rasterRDD = Some(input.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName)))

    meta.setClassification(classification.getOrElse(throw new IOException("Can't get classification! Ouch! " + input.getClass.getName)))

    if (aggregator.isDefined) {
      meta.setResamplingMethod(aggregator.get)
    }

    metadata(meta)
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}

}
