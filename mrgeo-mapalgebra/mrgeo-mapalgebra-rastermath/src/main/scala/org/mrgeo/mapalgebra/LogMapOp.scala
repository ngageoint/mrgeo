package org.mrgeo.mapalgebra

import java.awt.image.WritableRaster
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.SparkUtils

object LogMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("log")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null): MapOp =
    new LogMapOp(node, variables, protectionLevel)
}

class LogMapOp extends RasterMapOp with Externalizable {

  private var inputMapOp:Option[RasterMapOp] = None
  private var base:Option[Double] = None
  private var rasterRDD:Option[RasterRDD] = None

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {
    this()

    if (node.getNumChildren < 1) {
      throw new ParserException(node.getName + " requires at least one argument")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " requires only one or two arguments")
    }

    val childA = node.getChild(0)
    childA match {
    case func:ParserFunctionNode => inputMapOp = func.getMapOp match {
    case raster:RasterMapOp => Some(raster)
    case _ =>  throw new ParserException("First term \"" + childA + "\" is not a raster input")
    }
    case variable:ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case func:ParserFunctionNode => inputMapOp = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("First term \"" + childA + "\" is not a raster input")
      }
      }
    }

    if (node.getNumChildren == 2) {
      base = MapOp.decodeDouble(node.getChild(1))
    }

  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def execute(context: SparkContext): Boolean = {
    // our metadata is the same as the raster
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = meta.getDefaultValue(0)

    // precompute the denominator for the calculation
    val baseVal =
    if (base.isDefined) {
       Math.log(base.get)
    }
    else {
      1
    }

    rasterRDD = Some(RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toRaster(tile._2).asInstanceOf[WritableRaster]

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata)) {
              raster.setSample(x, y, b, Math.log(v) / baseVal)
            }
          }
        }
      }

      (tile._1, RasterWritable.toWritable(raster))
    })))

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, nodata))

    true
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
    base = in.readObject().asInstanceOf[Option[Double]]
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(base)

  }

}
