package org.mrgeo.mapalgebra.binarymath

import java.awt.image.WritableRaster
import java.io.{IOException, Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.mapalgebra.MapOp
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.{SparkUtils, TMSUtils, GDALUtils}

//object RawBinaryMathMapOpRegistrar extends MapOpRegistrar {
//  override def register: Array[String] = {
//    Array[String]("+", "-", "*", "/")
//  }
//  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
//    new RawBinaryMathMapOp(node, variables)
//
//  override def toString: String = "RawBinaryMathMapOp (object)"
//
//}

abstract class RawBinaryMathMapOp extends RasterMapOp with Externalizable {
  var constA:Option[Double] = None
  var constB:Option[Double] = None

  var varA:Option[RasterMapOp] = None
  var varB:Option[RasterMapOp] = None

  var rasterRDD:Option[RasterRDD] = None

  private[binarymath] def initialize(node:ParserNode, variables: String => Option[ParserNode], protectionLevel:String = null) = {

    if (node.getNumChildren < 2) {
      throw new ParserException(node.getName + " requires two arguments")
    }
    else if (node.getNumChildren > 2) {
      throw new ParserException(node.getName + " requires only two arguments")
    }

    val childA = node.getChild(0)
    val childB = node.getChild(1)

    childA match {
    case const:ParserConstantNode => constA = MapOp.decodeDouble(const)
    case func:ParserFunctionNode => varA = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("First term \"" + childA + "\" is not a raster input")
    }
    case variable:ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const:ParserConstantNode => constA = MapOp.decodeDouble(const)
      case func:ParserFunctionNode => varA = func.getMapOp match {
        case raster:RasterMapOp => Some(raster)
        case _ =>  throw new ParserException("First term \"" + childA + "\" is not a raster input")
      }
      }
    }

    childB match {
    case const:ParserConstantNode => constB = MapOp.decodeDouble(const)
    case func:ParserFunctionNode => varB = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("Second term \"" + childB + "\" is not a raster input")
    }
    case variable:ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const:ParserConstantNode => constB = MapOp.decodeDouble(const)
      case func:ParserFunctionNode => varB = func.getMapOp match {
      case raster:RasterMapOp => Some(raster)
      case _ =>  throw new ParserException("Second term \"" + childB + "\" is not a raster input")
      }
      }
    }

    if (constA.isEmpty && varA.isEmpty) {
      throw new ParserException("First term \"" + childA + "\" is invalid")
    }
    if (constB.isEmpty && varB.isEmpty) {
      throw new ParserException("Second term \"" + childA + "\" is invalid")
    }

    if (varA.isEmpty && varB.isEmpty) {
      throw new ParserException("\"" + node.getName + "\" must have at least 1 raster input")
    }

    this.protectionLevel(protectionLevel)
  }
  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {
    rasterRDD =
        if (constA.isDefined) {
          computeWithConstantA(varB.get, constA.get)
        }
        else if (constB.isDefined) {
          computeWithConstantB(varA.get, constB.get)
        }
        else {
          compute(varA.get, varB.get)
        }
    true
  }

  private[binarymath] def computeWithConstantA(raster: RasterMapOp, const: Double): Option[RasterRDD] = {
    val rdd = raster.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + raster.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = raster.metadata().getOrElse(throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName)).getDefaultValue(0)

    val answer = RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toRaster(tile._2).asInstanceOf[WritableRaster]

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata)) {
              raster.setSample(x, y, b, function(const, v))
            }
          }
        }
      }
      (tile._1, RasterWritable.toWritable(raster))
    }))

    metadata(SparkUtils.calculateMetadata(answer, raster.metadata().get.getMaxZoomLevel, nodata))

    Some(answer)

  }

  private[binarymath] def computeWithConstantB(raster: RasterMapOp, const: Double): Option[RasterRDD] = {

    val rdd = raster.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + raster.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata = raster.metadata().getOrElse(throw new IOException("Can't load metadata! Ouch! " + raster.getClass.getName)).getDefaultValue(0)

    val answer = RasterRDD(rdd.map(tile => {
      val raster = RasterWritable.toRaster(tile._2).asInstanceOf[WritableRaster]

      for (y <- 0 until raster.getHeight) {
        for (x <- 0 until raster.getWidth) {
          for (b <- 0 until raster.getNumBands) {
            val v = raster.getSampleDouble(x, y, b)
            if (RasterMapOp.isNotNodata(v, nodata)) {
              raster.setSample(x, y, b, function(v, const))
            }
          }
        }
      }
      (tile._1, RasterWritable.toWritable(raster))
    }))

    metadata(SparkUtils.calculateMetadata(answer, raster.metadata().get.getMaxZoomLevel, nodata))

    Some(answer)

  }

  private[binarymath] def compute(raster1: RasterMapOp, raster2: RasterMapOp): Option[RasterRDD] = {
    val rdd1 = raster1.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + raster1.getClass.getName))
    val rdd2 = raster2.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " + raster2.getClass.getName))

    // copy this here to avoid serializing the whole mapop
    val nodata1 = raster1.metadata() match {
    case Some(metadata) => metadata.getDefaultValue(0)
    case _ => Double.NaN
    }
    val nodata2 = raster2.metadata() match {
    case Some(metadata) => metadata.getDefaultValue(0)
    case _ => Double.NaN
    }

    // group the RDDs
    val group = new PairRDDFunctions(rdd1).cogroup(rdd2)

    val answer = RasterRDD(group.map(tile => {
      val iter1 = tile._2._1
      val iter2 = tile._2._2

      // raster 1 is missing, non-overlapping tile, use raster 2
      if (iter1.isEmpty) {
        (tile._1, iter2.head)
      }
      else if (iter2.isEmpty) {
        // raster 2 is missing, non-overlapping tile, use raster 1
        (tile._1, iter1.head)
      }
      else {
        // we know there are only 1 item in each group's iterator, so we can use head()
        val raster1 = RasterWritable.toRaster(iter1.head).asInstanceOf[WritableRaster]
        val raster2 = RasterWritable.toRaster(iter2.head).asInstanceOf[WritableRaster]

        for (y <- 0 until raster1.getHeight) {
          for (x <- 0 until raster1.getWidth) {
            for (b <- 0 until raster1.getNumBands) {
              val v1 = raster1.getSampleDouble(x, y, b)
              if (RasterMapOp.isNotNodata(v1, nodata1)) {
                val v2 = raster2.getSampleDouble(x, y, b)
                if (RasterMapOp.isNotNodata(v2, nodata2)) {
                  raster1.setSample(x, y, b, function(v1, v2))
                }
                else {
                  // if raster2 is nodata, we need to set raster1's pixel to nodata as well
                  raster1.setSample(x, y, b, nodata1)
                }
              }
            }
          }
        }

        (tile._1, RasterWritable.toWritable(raster1))
      }
    }))

    metadata(SparkUtils.calculateMetadata(answer, raster1.metadata().get.getMaxZoomLevel, nodata1))

    Some(answer)
  }


  private[binarymath] def function(a:Double, b:Double):Double

  override def readExternal(in: ObjectInput): Unit = {
    constA = in.readObject().asInstanceOf[Option[Double]]
    constB = in.readObject().asInstanceOf[Option[Double]]
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(constA)
    out.writeObject(constB)
  }

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

}
