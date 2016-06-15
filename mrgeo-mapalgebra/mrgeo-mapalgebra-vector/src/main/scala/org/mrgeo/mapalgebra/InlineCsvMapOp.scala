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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.Geometry
import org.mrgeo.hdfs.vector.Column.FactorType
import org.mrgeo.hdfs.vector.{Column, ColumnDefinitionFile, DelimitedParser}
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.vector.VectorMapOp

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object InlineCsvMapOp extends MapOpRegistrar
{
  override def register: Array[String] = {
    Array[String]("InlineCsv", "csv")
  }

  def create(columns:String, values:String):MapOp = {
    new InlineCsvMapOp(columns, values)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp = {
    new InlineCsvMapOp(node, variables)
  }

  def parseColumns(columns: String, delim: Char): ColumnDefinitionFile =
  {
    val cdf = new ColumnDefinitionFile()
    var columnList = new ListBuffer[Column]()
    val columnArray = columns.split(Character.toString(delim))
    columnArray.foreach(cs => {
      val c = new Column(cs, FactorType.Nominal)
      columnList += c
    })

    cdf.setColumns(columnList.toList)

    cdf
  }
}


@SuppressFBWarnings(value = Array("NP_LOAD_OF_KNOWN_NULL_VALUE"), justification = "Scala generated code")
class InlineCsvMapOp extends VectorMapOp with Externalizable
{
  private val recordSeparator = ';'
  private val encapsulator = '\''
  private val fieldSeparator = ','
  private var records: Array[String] = null
  private var delimitedParser: DelimitedParser = null
  private var vectorrdd: Option[VectorRDD] = None

  def this(columns:String, values:String) = {
    this()

    val cdf = if (columns != null && columns.length > 0) {
      InlineCsvMapOp.parseColumns(columns, fieldSeparator)
    }
    else {
      throw new ParserException("Missing the column definitions for inline csv")
    }

    records = if (values != null && values.length > 0) {
      values.split(Character.toString(recordSeparator))
    }
    else {
      throw new ParserException("Missing values for inline csv")
    }


    val attributes = new ListBuffer[String]()

    var xCol: Int = -1
    var yCol: Int = -1
    var geometryCol: Int = -1
    for ((col, i) <- cdf.getColumns.view.zipWithIndex) {
      val c = col.getName

      if (col.getType == Column.FactorType.Numeric)
      {
        if (c.equals("x"))
        {
          xCol = i
        }
        else if (c.equals("y"))
        {
          yCol = i
        }
      }
      else
      {
        if (c.toLowerCase().equals("geometry"))
        {
          geometryCol = i
        }
      }
      attributes.add(c)
    }

    delimitedParser = new DelimitedParser(attributes, xCol, yCol, geometryCol,
      recordSeparator, encapsulator, cdf.isFirstLineHeader)
  }

  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def rdd(): Option[VectorRDD] = {
    vectorrdd
  }

  override def execute(context: SparkContext): Boolean = {
    var recordData = new ListBuffer[(FeatureIdWritable, Geometry)]()
    if (records != null) {
      for ((record, i) <- records.view.zipWithIndex) {
        val geom = delimitedParser.parse(record)
        recordData.+=((new FeatureIdWritable(i), geom))
      }
    }
    vectorrdd = Some(VectorRDD(context.parallelize(recordData)))
    true
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  @SuppressFBWarnings(value = Array("NP_LOAD_OF_KNOWN_NULL_VALUE"), justification = "Scala generated code")
  def initialize(node:ParserNode, variables: String => Option[ParserNode]): Unit = {
    if (node.getNumChildren != 2)
    {
      throw new ParserException("Inline CSV takes two arguments. (columns and values)")
    }

    val columns = MapOp.decodeString(node.getChild(0))
    val cdf = columns match {
      case Some(c) =>
        InlineCsvMapOp.parseColumns(c, fieldSeparator)
      case None =>
        throw new ParserException("Missing the column definitions for inline csv")
    }

    val values = MapOp.decodeString(node.getChild(1))
    records = values match {
      case Some(v) =>
        values.get.split(Character.toString(recordSeparator))
      case None =>
        throw new ParserException("Missing values for inline csv")
    }

    val attributes = new ListBuffer[String]()

    var xCol: Int = -1
    var yCol: Int = -1
    var geometryCol: Int = -1
    for ((col, i) <- cdf.getColumns.view.zipWithIndex) {
      val c = col.getName

      if (col.getType == Column.FactorType.Numeric)
      {
        if (c.equals("x"))
        {
          xCol = i
        }
        else if (c.equals("y"))
        {
          yCol = i
        }
      }
      else
      {
        if (c.toLowerCase().equals("geometry"))
        {
          geometryCol = i
        }
      }
      attributes.add(c)
    }

    delimitedParser = new DelimitedParser(attributes, xCol, yCol, geometryCol,
      recordSeparator, encapsulator, cdf.isFirstLineHeader)
  }

  override def readExternal(in: ObjectInput): Unit = {
    delimitedParser = new DelimitedParser()
    delimitedParser.readExternal(in)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    delimitedParser.writeExternal(out)
  }
}
