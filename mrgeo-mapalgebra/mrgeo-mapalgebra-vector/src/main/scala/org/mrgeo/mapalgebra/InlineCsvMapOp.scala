package org.mrgeo.mapalgebra

import java.io.{ObjectInput, ObjectOutput, Externalizable}

import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.geometry.Geometry
import org.mrgeo.hdfs.vector.Column.FactorType
import org.mrgeo.hdfs.vector.{DelimitedParser, Column, ColumnDefinitionFile}
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.spark.job.JobArguments
import collection.JavaConversions._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object InlineCsvMapOp extends MapOpRegistrar
{
  override def register: Array[String] = {
    Array[String]("InlineCsv", "csv")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new InlineCsvMapOp(node, variables)

  def parseColumns(columns: String, delim: Char): ColumnDefinitionFile =
  {
    var cdf: ColumnDefinitionFile = new ColumnDefinitionFile()
    var columnList = new ListBuffer[Column]()
    val columnArray = columns.split(Character.toString(delim))
    columnArray.foreach(cs => {
      val c = new Column(cs, FactorType.Nominal)
      columnList.+=(c)
    })

    cdf.setColumns(columnList.toList)

    return cdf
  }
}


class InlineCsvMapOp extends VectorMapOp with Externalizable
{
  private val recordSeparator = ';'
  private val encapsulator = '\''
  private val fieldSeparator = ','
  private var records: Array[String] = null
  private var delimitedParser: DelimitedParser = null
  private var vectorrdd: Option[VectorRDD] = None

  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def rdd(): Option[VectorRDD] = {
    vectorrdd
  }

  override def execute(context: SparkContext): Boolean = {
    var recordData = new ListBuffer[(LongWritable, Geometry)]()
    for ((record, i) <- records.view.zipWithIndex) {
      val geom = delimitedParser.parse(record)
      recordData .+= ((new LongWritable(i), geom))
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

  def initialize(node:ParserNode, variables: String => Option[ParserNode]): Unit = {
    if (node.getNumChildren != 2)
    {
      throw new ParserException("Inline CSV takes two arguments. (columns and values)")
    }

    val columns = MapOp.decodeString(node.getChild(0))
    val cdf = columns match {
      case Some(c) => {
        InlineCsvMapOp.parseColumns(c, fieldSeparator)
      }
      case None => {
        throw new ParserException("Missing the column definitions for inline csv")
      }
    }

    val values = MapOp.decodeString(node.getChild(1))
    records = values match {
      case Some(v) => {
        values.get.split(Character.toString(recordSeparator))
      }
      case None => {
        throw new ParserException("Missing values for inline csv")
      }
    }

    var attributes = new ListBuffer[String]()

    var xCol: Int = -1
    var yCol: Int = -1
    var geometryCol: Int = -1
    for ((col, i) <- cdf.getColumns.view.zipWithIndex) {
      val c = col.getName()

      if (col.getType() == Column.FactorType.Numeric)
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
      recordSeparator, encapsulator, cdf.isFirstLineHeader())
  }

  override def readExternal(in: ObjectInput): Unit = {
  }

  override def writeExternal(out: ObjectOutput): Unit = {
  }
}
