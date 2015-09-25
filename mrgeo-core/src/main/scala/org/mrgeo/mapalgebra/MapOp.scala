package org.mrgeo.mapalgebra

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.mrgeo.mapalgebra.parser._
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
  def decodeDouble(node:ParserNode, variables: String => Option[ParserNode]): Option[Double] = {
    node match {
    case const: ParserConstantNode => decodeDouble(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeDouble(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a double")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a double")
    }
  }

  def decodeFloat(node: ParserNode): Option[Float] = {
    val value = decodeDouble(node)
    if (value.isDefined) {
      Some(value.get.toFloat)
    }
    None
  }

  def decodeFloat(node:ParserNode, variables: String => Option[ParserNode]): Option[Float] = {
    node match {
    case const: ParserConstantNode => decodeFloat(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeFloat(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a float")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a float")
    }
  }


  def decodeLong(node: ParserNode): Option[Long] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toLong)
    case _ => None
    }
  }

  def decodeLong(node:ParserNode, variables: String => Option[ParserNode]): Option[Long] = {
    node match {
    case const: ParserConstantNode => decodeLong(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeLong(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a long")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a long")
    }
  }

  def decodeInt(node: ParserNode): Option[Int] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toInt)
    case _ => None
    }
  }

  def decodeInt(node:ParserNode, variables: String => Option[ParserNode]): Option[Int] = {
    node match {
    case const: ParserConstantNode => decodeInt(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeInt(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a integer")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a integer")
    }
  }


  def decodeShort(node: ParserNode): Option[Short] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toShort)
    case _ => None
    }
  }

  def decodeShort(node:ParserNode, variables: String => Option[ParserNode]): Option[Short] = {
    node match {
    case const: ParserConstantNode => decodeShort(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeShort(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a short")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a short")
    }
  }

  def decodeByte(node: ParserNode): Option[Byte] = {
    decodeDouble(node) match {
    case Some(value) => Some(value.toByte)
    case _ => None
    }
  }

  def decodeByte(node:ParserNode, variables: String => Option[ParserNode]): Option[Byte] = {
    node match {
    case const: ParserConstantNode => decodeByte(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeByte(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a byte")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a byte")
    }
  }


  def decodeString(node: ParserNode): Option[String] = {
    node match {
    case c: ParserConstantNode => Some(c.getValue.toString)
    case _ => None
    }
  }

  def decodeString(node:ParserNode, variables: String => Option[ParserNode]): Option[String] = {
    node match {
    case const: ParserConstantNode => decodeString(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeString(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a string")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a string")
    }
  }

  def decodeBoolean(node: ParserNode): Option[Boolean] = {
    decodeString(node) match {
    case Some(value) => value.toLowerCase match {
      case "true" | "1" | "yes" => Some(true)
      case "false" | "0" | "no" => Some(false)
      case _ => None
    }
    case _ => None
    }
  }

  def decodeBoolean(node:ParserNode, variables: String => Option[ParserNode]): Option[Boolean] = {
    node match {
    case const: ParserConstantNode => decodeBoolean(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeBoolean(node)
      case _ => throw new ParserException("Term \"" + node + "\" is not a boolean")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a boolean")
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
  private var sparkContext: SparkContext = null

  def context(cont: SparkContext) = sparkContext = cont
  def context(): SparkContext = sparkContext

  def setup(job: JobArguments, conf: SparkConf): Boolean
  def execute(context: SparkContext): Boolean
  def teardown(job: JobArguments, conf: SparkConf): Boolean
}

