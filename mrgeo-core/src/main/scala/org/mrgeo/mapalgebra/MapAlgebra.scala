package org.mrgeo.mapalgebra

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.rdd.MrGeoRDD
import org.mrgeo.mapalgebra.parser.{ParserNode, ParserException, ParserAdapterFactory}
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils.StringUtils

import scala.collection.mutable

object MapAlgebra extends MrGeoDriver {

  def run(expression:String, output:String, conf:Configuration) = {
  }

  def validate(expression:String):Boolean = new MapAlgebra().validate(expression)

  override def setup(job: JobArguments): Boolean = true
}

class MapAlgebra extends MrGeoJob with Externalizable {
  private val filePattern = Pattern.compile("\\s*\\[([^\\]]+)\\]\\s*")
  private val parser = ParserAdapterFactory.createParserAdapter
  private val variables = mutable.Map.empty[String, Option[MrGeoRDD[_,_]]]
  parser.initialize()


  def validate(expression:String):Boolean = {
    val cleaned = cleanExpression(expression)

    try {
      val mapops = parse(cleaned)
    }
    catch {
      // any exception means an error
      case p:ParserException => return false
      case e:Exception => return false
    }

    true
  }

  private def parse(expression: String): Array[MapOp] = {
    val mapops = Array.newBuilder[MapOp]

    val lines = expression.split(";")

    lines.foreach(line => {
      val rootNode: ParserNode = parser.parse(line)
    })


    mapops.result()
  }

  private def cleanExpression(expression: String): String = {
    logInfo("Raw expression: " + expression)

    val lines = expression.split("\n")

    val cleanlines = lines.map(raw => {
      val line = raw.trim
      if (line.isEmpty || line.startsWith("#")) {
        ""
      }
      else if (line.indexOf("#") > 0) {
        line.substring(0, line.indexOf("#")).trim
      }
      else {
        line
      }
    })

    val cleanexp = StringUtils.join(cleanlines, ";")
    logInfo("Cleaned expression: " + cleanexp)
    cleanexp
  }

//  private def mapFiles(expression: String) = {
//    val m = filePattern.matcher(expression)
//    val filesFound = mutable.HashSet.empty[String]
//
//    // TODO:  NaN is a special case?
//    files.put("NaN", None)
//
//    var i: Int = 0
//    while (m.find) {
//      val file: String = m.group(1)
//      if (!files.contains(file)) {
//        files.put(file, None)
//      }
//    }
//  }

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes.result()
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = true

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}


object TestMapAlgebra extends App {
  MapAlgebra.validate("[/mrgeo/images/small-elevation] + [/mrgeo/images/small-elevation]")
}
