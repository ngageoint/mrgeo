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

import java.io._
import java.util.regex.Pattern
import javax.script.ScriptEngineManager

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.{DataProviderFactory, DataProviderNotFound, ProviderProperties}
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.{MrsPyramidMapOp, RasterMapOp}
import org.mrgeo.mapalgebra.vector.{VectorDataMapOp, VectorMapOp}
import org.mrgeo.utils.StringUtils

import scala.collection.JavaConversions._
import scala.collection.mutable

object MapAlgebra extends MrGeoDriver {

  final private val MapAlgebra = "mapalgebra"
  final private val Output = "output"
  // these two parameters may need to be accessed by mapops, they just can get it from the job
  final val ProtectionLevel = "protection.level"
  final val ProviderProperties = "provider.properties"

  def mapalgebra(expression: String, output: String,
      conf: Configuration, providerProperties: ProviderProperties, protectionLevel: String = null): Boolean = {
    val args = mutable.Map[String, String]()

    val name = "MapAlgebra"

    args += MapAlgebra -> expression
    args += Output -> output

    args += ProtectionLevel -> {
      if (protectionLevel == null) {
        ""
      }
      else {
        protectionLevel
      }
    }
    args += ProviderProperties -> {
      if (providerProperties == null) {
        ""
      }
      else {
        data.ProviderProperties.toDelimitedString(providerProperties)
      }
    }

    run(name, classOf[MapAlgebra].getName, args.toMap, conf, Some(MapOpFactory.getMapOpClasses.toSet))

      true
  }

  def validate(expression: String, providerProperties: ProviderProperties): Boolean = {
    try {
      new MapAlgebra().isValid(expression, providerProperties)
    }
    catch {
      // any exception means an error
      case p: ParserException =>
        logError("Parser error!  " + p.getMessage)
        return false
      case e: Exception =>
        e.printStackTrace()
        return false
    }

    true
  }

  def validateWithExceptions(expression: String, providerProperties: ProviderProperties) = {
    new MapAlgebra().isValid(expression, providerProperties)
  }

  override def setup(job: JobArguments): Boolean = true
}

class MapAlgebra() extends MrGeoJob with Externalizable {
  private val filePattern = Pattern.compile("\\s*\\[([^\\]]+)\\]\\s*")
  private val parser = ParserAdapterFactory.createParserAdapter
  private val variables = mutable.Map.empty[ParserVariableNode, Option[ParserNode]]

  {
    val cn: ParserConstantNode = new ParserConstantNode
    cn.setValue(Double.NaN)
    cn.setName("NaN")

    val vn = new ParserVariableNode
    vn.setNativeNode(null)
    vn.setName("NaN")

    variables.put(vn, Some(cn))
  }

  parser.initialize()

  def isValid(expression: String,
      providerproperties: ProviderProperties = ProviderProperties.fromDelimitedString("")) = {
    parse(expression)
  }

  private def parse(expression: String, protectionLevel: String = null): Array[ParserNode] = {
    val nodes = Array.newBuilder[ParserNode]

    val cleaned = cleanExpression(expression)

    val mapped = mapFiles(cleaned)

    val lines = Array.newBuilder[String]

    val line = mutable.StringBuilder.newBuilder
    var insingle = false
    var indouble = false

    mapped.foreach {
      case ch@'"' =>
        if (!insingle) {
          indouble = !indouble
        }
        line += ch
      case ch@'\'' =>
        if (!indouble) {
          insingle = !insingle
        }
        line += ch
      case ch@';' =>
        if (!insingle && !indouble) {
          lines += line.result()
          line.clear()
        }
        else {
          line += ch
        }
      case ch => line += ch
    }
    //    mapped.foreach(ch =>
    //        line += ch
    //      ch match {
    //      case @'"' =>
    //        if (!insingle)
    //          indouble = !indouble
    //      case @'\'' =>
    //        if (!indouble)
    //          insingle = !insingle
    //      case @';' =>
    //        if (!insingle && !indouble) {
    //        lines += line.result()
    //        line.clear()
    //      })
    //
    if (indouble || insingle) {
      if (indouble) {
        throw new ParserException("Unclosed double quote: " + cleaned)
      }
      else {
        throw new ParserException("Unclosed single quote: " + cleaned)
      }
    }

    if (line.nonEmpty) {
      lines += line.result()
    }

    lines.result().foreach(line => {
      val node = parser.parse(line)
      buildMapOps(node)
      nodes += node
    })

    nodes.result()
  }

  private def cleanExpression(expression: String): String = {
    logInfo("Raw expression: " + expression)

    val lines = expression.split("\r?\n|\r|;")

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

    val cleaner = cleanlines.filter(!_.isEmpty)

    val cleanexp = StringUtils.join(cleaner, ";").replaceAll("\\s+", " ")
    logInfo("Cleaned expression: " + cleanexp)
    cleanexp
  }

  // we have to map files differently because the parser doesn't know about the "[<file>]" syntax
  private def mapFiles(expression: String) = {

    var exp = expression
    var i: Int = 0

    var matcher = filePattern.matcher(exp)
    while (matcher.find) {
      val file: String = matcher.group(1)

      //if (!variables.contains(file)) {
      val variable = "__file_" + i + "__"

      val vn = new ParserVariableNode
      vn.setNativeNode(null)
      vn.setName(variable)

      val fn = new ParserFunctionNode
      fn.setName(file)
      fn.setMapOp(loadResource(file).orNull)

      variables.put(vn, Some(fn))
      exp = exp.replace("[" + file + "]", variable)
      i += 1
      //}
      matcher = filePattern.matcher(exp)
    }
    logInfo("Expression with files mapped: " + exp)

    exp
  }

  def findVariable(name: String): Option[ParserNode] = {
    variables.find(variable => variable._1.getName == name) match {
    case Some(v) => v._2
    case None => None
    }
  }

  private def buildMapOps(node: ParserNode): Unit = {
    // special case for "="
    node match {
    case function: ParserFunctionNode =>
      val name = function.getName
      if (name == "=") {
        if (function.getNumChildren != 2) {
          throw new ParserException("Variable \"" + name +
              "\" must be in the form " + name + " =  <expression>")
        }

        val variable = function.getChild(0)

        variable match {
        case v: ParserVariableNode =>
          if (MapOpFactory.exists(variable.getName)) {
            throw new ParserException("Cannot use variable name \"" + variable.getName +
                "\" because there is a function of the same name")
          }
          val value = function.getChild(1)

          buildMapOps(value)
          variables.put(v, Some(value))

        case _ => throw new ParserException("Left side of \"=\" must be a valid variable name")
        }

        return
      }

    case _ =>
    }

    node.getChildren.foreach(child => {
      buildMapOps(child)
    })

    node match {
    case const: ParserConstantNode =>

    case variable: ParserVariableNode =>
      val name = variable.getName
      if (findVariable(name).isEmpty) {
        throw new ParserException("Variable \"" + name +
            "\" must be defined before used")
      }
    case function: ParserFunctionNode =>
      val name = function.getName
      // Remember,  "=" was handled above
      if (!MapOpFactory.exists(name)) {
        throw new ParserException("Function \"" + name + "\" does not exist")
      }

      // NOTE:  mapop constructor should throw ParserExceptions on error
      MapOpFactory(function, findVariable) match {
      case Some(op) => function.setMapOp(op)
      case _ =>
      }
    }
  }

  private def loadResource(name: String): Option[MapOp] = {
    try {
      val imdp = DataProviderFactory.getMrsImageDataProvider(name, AccessMode.READ, providerproperties)
      return Some(MrsPyramidMapOp(imdp))
    }
    catch {
      case e: DataProviderNotFound =>
    }
    try {
      val vdp = DataProviderFactory.getVectorDataProvider(name, AccessMode.READ, providerproperties)
      return Some(VectorDataMapOp(vdp))
    }
    catch {
      case e: DataProviderNotFound =>
    }
    None
  }

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes.result()
  }

  var expression: String = null
  var output: String = null
  var providerproperties: ProviderProperties = null
  var protectionLevel: String = null

  var nodes: Array[ParserNode] = null

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    expression = job.getSetting(MapAlgebra.MapAlgebra)
    output = job.getSetting(MapAlgebra.Output)

    providerproperties = ProviderProperties.fromDelimitedString(
      job.getSetting(MapAlgebra.ProviderProperties))

    protectionLevel = job.getSetting(MapAlgebra.ProtectionLevel)
    if (protectionLevel.length == 0) {
      protectionLevel = null
    }

    nodes = parse(expression)

    val classes = Array.newBuilder[Class[_]]

    nodes.foreach(node => {
      setup(node, job, conf)
      classes ++= register(node, job, conf)
    })

    MrGeoJob.registerClasses(classes.result(), conf)

    true
  }


  private def register(node: ParserNode, job: JobArguments, conf: SparkConf): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    // depth first run
    node.getChildren.foreach(child => {
      classes ++= register(child, job, conf)
    })

    node match {
    case function: ParserFunctionNode =>
      function.getName match {
      case "=" => // ignore assignments...
      case _ =>
        val mapop = function.getMapOp

        if (mapop != null) {
          classes ++= mapop.registerClasses()
        }
      }

    case _ => // no op, nothing to do if we're not a function (MapOp)
    }

    classes.result()
  }

  private def setup(node: ParserNode, job: JobArguments, conf: SparkConf): Unit = {
    // depth first run
    node.getChildren.foreach(child => {
      setup(child, job, conf)
    })

    node match {
    case function: ParserFunctionNode =>
      function.getName match {
      case "=" => // ignore assignments...
      case _ =>
        val mapop = function.getMapOp

        if (mapop != null) {
          mapop.setup(job, conf)
        }
      }

    case _ => // no op, nothing to do if we're not a function (MapOp)
    }
  }


  override def execute(context: SparkContext): Boolean = {

    // we need to run through each variable and make sure the context is set.  Input files are
    // known to _not_ have the context set
    variables.values.foreach {
      case Some(variable) =>
        variable match {
        case function: ParserFunctionNode => function.getMapOp.context(context)
        case _ =>
        }
      case _ =>
    }

    // execute the mapalgebra
    nodes.foreach(node => {
      execute(node, context)
    })

    // now take the last RDD created and save it
    nodes.reverseIterator.foreach { node =>
      if (save(node, output, providerproperties, context)) {
        return true
      }
    }

    false
  }


  private def save(node: ParserNode, output: String, providerproperties: ProviderProperties,
      context: SparkContext): Boolean = {

    node match {
    case function: ParserFunctionNode =>
      function.getMapOp match {
      case rmo: RasterMapOp =>
        rmo.save(output, providerproperties, context)
        return true
      case vmo: VectorMapOp =>
        vmo.save(output, providerproperties, context)
        return true
      case _ =>
        function.getChildren.foreach(child => {
          if (save(child, output, providerproperties, context)) {
            return true
          }
        })
      }
    case variable: ParserVariableNode =>
      MapOp.decodeVariable(variable, findVariable) match {
      case Some(pn) => pn match {
      case function: ParserFunctionNode => function.getMapOp match {
      case rmo: RasterMapOp =>
        rmo.save(output, providerproperties, context)
        return true
      case vmo: VectorMapOp =>
        vmo.save(output, providerproperties, context)
        return true
      case _ =>
      }

      }
      case _ => throw new IOException("Error finding a node to save")
      }

    case _ =>
    }
    false
  }


  private def execute(node: ParserNode, context: SparkContext): Unit = {
    // depth first run
    node.getChildren.foreach(child => {
      execute(child, context)
    })

    node match {
    case function: ParserFunctionNode =>
      function.getName match {
      case "=" => // ignore assignments...
      case _ =>
        val mapop = function.getMapOp

        if (mapop != null) {
          mapop.execute(context)
        }
      }
    case _ => // no op, nothing to do if we're not a function (MapOp)
    }
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}

@SuppressFBWarnings(value=Array("NM_CLASS_NAMING_CONVENTION"), justification = "Well, yes it does!")
object TestMapAlgebra extends App {

  val baseDir: File = new File(System.getProperty("java.io.tmpdir"))
  val username: String = "jython-" + System.getProperty("user.name")
  val cacheDir: File = new File(baseDir, username)

  //System.setProperty("python.path", "/usr/lib/python2.7")
  System.setProperty("python.verbose", "debug")
  System.setProperty("python.cachedir", cacheDir.getCanonicalPath)


  //System.setProperty("python.home", "/usr/lib/python2.7/")

  val manager = new ScriptEngineManager()
  manager.getEngineFactories.foreach(engine => {
    println(engine.getEngineName + " " + engine.getLanguageName + " " + engine.getNames)
  })

  println()
  val engine = manager.getEngineByName("python")

  if (engine != null) {
    println(engine.getFactory.getEngineName)
  }
  else {
    println("no python")
  }
  //  val conf = HadoopUtils.createConfiguration()
  //  val pp = ProviderProperties.fromDelimitedString("")
  //
  //  HadoopUtils.setupLocalRunner(conf)
  //
  //  val expression = "x = 100; " +
  //      "y = [/mrgeo/images/small-elevation]; " +
  //      "z = 10 / [/mrgeo/images/small-elevation] + [/mrgeo/images/small-elevation] * [/mrgeo/images/small-elevation] - x + y; " +
  //      "a = [/mrgeo/images/small-elevation] + [/mrgeo/images/small-elevation] * [/mrgeo/images/small-elevation] + [/mrgeo/images/small-elevation]; " +
  //      "[/mrgeo/images/small-elevation] + [/mrgeo/images/small-elevation] * [/mrgeo/images/small-elevation] + [/mrgeo/images/small-elevation]"
  //
  //
  //  val output = "test-mapalgebra"
  //  if (MapAlgebra.validate(expression, pp)) {
  //    //MapAlgebra.mapalgebra(expression, output, conf, pp)
  //  }

  System.exit(0)
}
