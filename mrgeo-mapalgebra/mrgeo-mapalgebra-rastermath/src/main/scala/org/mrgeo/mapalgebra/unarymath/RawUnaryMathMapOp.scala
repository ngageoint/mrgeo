package org.mrgeo.mapalgebra.unarymath

import java.awt.image.WritableRaster
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.SparkUtils

abstract class RawUnaryMathMapOp extends RasterMapOp with Externalizable {
  var input:Option[RasterMapOp] = None
  var rasterRDD:Option[RasterRDD] = None

  private[unarymath] def initialize(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires one arguments")
    }
    else if (node.getNumChildren > 1) {
      throw new ParserException(node.getName + " requires only two arguments")
    }

    val childA = node.getChild(0)

    childA match {
    case const:ParserConstantNode => None
    case func:ParserFunctionNode => input = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("\"" + childA + "\" is not a raster input")
    }
    case variable:ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const:ParserConstantNode => None
      case func:ParserFunctionNode => input = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("\"" + childA + "\" is not a raster input")
      }
      }
    }
    if (input.isEmpty) {
      throw new ParserException("\"" + node.getName + "\" must have at least 1 raster input")
    }

    if (input.isDefined && !input.get.isInstanceOf[RasterMapOp]) {
      throw new ParserException("\"" + childA + "\" is not a raster input")
    }

    this.protectionLevel(protectionLevel)
  }
  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {

    // our metadata is the same as the raster
    val meta = input.get.metadata().get

    val rdd = input.get.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + input.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = meta.getDefaultValue(0)

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toRaster(tile._2).asInstanceOf[WritableRaster]

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata)) {
              raster.setSample(x, y, b, function(v))
            }
          }
        }
      }
      (tile._1, RasterWritable.toWritable(raster))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, nodata))

    true
  }

  private[unarymath] def function(a:Double):Double

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

}
