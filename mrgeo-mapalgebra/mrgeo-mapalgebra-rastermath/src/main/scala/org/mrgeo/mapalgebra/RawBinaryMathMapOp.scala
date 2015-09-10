package org.mrgeo.mapalgebra

import java.io.{ObjectInput, ObjectOutput, Externalizable}

import com.vividsolutions.jts.geom.util.GeometryMapper.MapOp
import org.apache.spark.SparkContext
import org.mrgeo.mapalgebra.parser._

object RawBinaryMathMapOpRegistrar extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("+", "-", "*", "/")
  }
  override def apply(function:String, node:ParserNode, variables: String => Option[MapOp]): MapOp =
    new RawBinaryMathMapOp(function, node, variables)

  override def toString: String = "RawBinaryMathMapOp (object)"

}

class RawBinaryMathMapOp extends MapOp with Externalizable {
  var function:String = null
  var constA:Double = Double.PositiveInfinity
  var constB:Double = Double.PositiveInfinity

  private[mapalgebra] def this(function:String, node:ParserNode, variables: String => Option[MapOp]) = {
    this()
    this.function = function

    if (node.getNumChildren < 2) {
      throw new ParserException(function + " requires two arguments")
    }
    else if (node.getNumChildren < 2) {
      throw new ParserException(function + " requires only two arguments")
    }

    val childA = node.getChild(0)
    childA match {
    case c:ParserConstantNode => constA
    case v:ParserVariableNode =>
    case f:ParserFunctionNode =>
    case _ => throw new ParserException("Unknown argument type")
    }

  }

  override def execute(context: SparkContext): Boolean = {false}

  override def readExternal(in: ObjectInput): Unit = {
    function = in.readUTF()
    constA = in.readDouble()
    constB = in.readDouble()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(function)
    out.writeDouble(constA)
    out.writeDouble(constB)
  }
}
