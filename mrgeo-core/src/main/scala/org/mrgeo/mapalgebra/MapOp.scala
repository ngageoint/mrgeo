package org.mrgeo.mapalgebra

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.mrgeo.mapalgebra.parser.{ParserConstantNode, ParserNode, ParserVariableNode}
import org.mrgeo.spark.job.JobArguments

object MapOp {
  def decodeDouble(node: ParserNode): Option[Double] = {
    node match {
    case c: ParserConstantNode =>
      c.getValue match {
      case d: java.lang.Double => Some(d)
      case f: java.lang.Float => Some(f.toDouble)
      case l: java.lang.Long => Some(l.toDouble)
      case i: java.lang.Integer => Some(i.toDouble)
      case s: java.lang.Short => Some(s.toDouble)
      case b: java.lang.Byte => Some(b.toDouble)
      case s: String =>
        try
          Some(s.toDouble)
        catch {
          case e: Exception => None
        }
      case _ => None
      }
    case _ => None
    }
  }

  def decodeFloat(node: ParserNode): Option[Float] = {
    val value = decodeDouble(node)
    if (value.isDefined) {
      Some(value.get.toFloat)
    }
    None
  }

  def decodeLong(node: ParserNode): Option[Long] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toLong)
    case _ => None
    }
  }

  def decodeInt(node: ParserNode): Option[Int] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toInt)
    case _ => None
    }
  }

  def decodeShort(node: ParserNode): Option[Short] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toShort)
    case _ => None
    }
  }

  def decodeByte(node: ParserNode): Option[Byte] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toByte)
    case _ => None
    }
  }

  def decodeString(node: ParserNode): Option[String] = {
    node match {
    case c: ParserConstantNode => Some(c.getValue.toString)
    case _ => None
    }
  }

  def decodeVariable(node: ParserVariableNode, variables: String => Option[ParserNode]): Option[ParserNode] = {
    variables(node.getName) match {
    case Some(value) =>
      value match {
      case v1: ParserVariableNode => decodeVariable(v1, variables)
      case p: ParserNode => Some(p)
      case _ => None
      }
    case _ => None
    }
  }
}

abstract class MapOp extends Logging {
  private var protectionlevel: String = null

  private var sparkContext: SparkContext = null

  def context(cont: SparkContext) = sparkContext = cont
  def context(): SparkContext = sparkContext

  def setup(job: JobArguments, conf: SparkConf): Boolean
  def execute(context: SparkContext): Boolean
  def teardown(job: JobArguments, conf: SparkConf): Boolean

  def protectionLevel():String = { protectionlevel }
  def protectionLevel(level:String) = { protectionlevel = level }
}

