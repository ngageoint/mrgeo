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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.utils.Logging

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
      case const: ParserConstantNode => decodeDouble(const)
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
    else {
      None
    }
  }

  def decodeFloat(node:ParserNode, variables: String => Option[ParserNode]): Option[Float] = {
    node match {
    case const: ParserConstantNode => decodeFloat(node)
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, variables).get match {
      case const: ParserConstantNode => decodeFloat(const)
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
      case const: ParserConstantNode => decodeLong(const)
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
      case const: ParserConstantNode => decodeInt(const)
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
      case const: ParserConstantNode => decodeShort(const)
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
      case const: ParserConstantNode => decodeByte(const)
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
      case const: ParserConstantNode => decodeString(const)
      case _ => throw new ParserException("Term \"" + node + "\" is not a string")
      }
    case _ => throw new ParserException("Term \"" + node + "\" is not a string")
    }
  }

  @SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
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
      case const: ParserConstantNode => decodeBoolean(const)
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

  def registerClasses(): Array[Class[_]] = {Array.empty[Class[_]] }

  def setup(job: JobArguments, conf: SparkConf): Boolean
  def execute(context: SparkContext): Boolean
  def teardown(job: JobArguments, conf: SparkConf): Boolean
}

