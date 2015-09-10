package org.mrgeo.mapalgebra

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.buildpyramid.BuildPyramidSpark
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.mapalgebra.parser._
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils.{HadoopUtils, StringUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable

object MapAlgebra extends MrGeoDriver {

  final private val MapAlgebra = "mapalgebra"
  final private val Output = "output"
  final private val ProviderProperties = "provider.properties"

  def run(expression:String, output:String,
      conf:Configuration, providerProperties: ProviderProperties):Boolean = {
    val args = mutable.Map[String, String]()


    val name = "MapAlgebra"

    args += MapAlgebra -> expression
    args += Output -> output

    if (providerProperties != null)
    {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
    }
    else
    {
      args += ProviderProperties -> ""
    }

    run(name, classOf[MapAlgebra].getName, args.toMap, conf)

    true
  }

  def validate(expression:String, conf:Configuration, providerProperties: ProviderProperties):Boolean = new MapAlgebra(conf).validate(expression)

  override def setup(job: JobArguments): Boolean = true
}

class MapAlgebra(val conf:Configuration) extends MrGeoJob with Externalizable {
  private val filePattern = Pattern.compile("\\s*\\[([^\\]]+)\\]\\s*")
  private val parser = ParserAdapterFactory.createParserAdapter
  private val variables = mutable.Map.empty[String, Option[MapOp]]
  parser.initialize()

  def this() {
    this(HadoopUtils.createConfiguration())
  }

  def validate(expression:String):Boolean = {
    try {
      val nodes = parse(expression)

      nodes.foreach(node => {
        walkNodesForValidation(node)
      })
    }
    catch {
      // any exception means an error
      case p:ParserException =>
        logError("Parser error!  " + p.getMessage)
        return false
      case e:Exception => return false
    }

    true
  }

  private def parse(expression: String): Array[ParserNode] = {
    val nodes = Array.newBuilder[ParserNode]

    val cleaned = cleanExpression(expression)

    val mapped = mapFiles(cleaned)

    val lines = mapped.split(";")

    lines.foreach(line => {
      nodes += parser.parse(line)
    })

    nodes.result()
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

  private def mapFiles(expression: String) = {
    val found = mutable.Set.empty[String]

    var exp = expression
    var i: Int = 0

    var matcher = filePattern.matcher(exp)
    while (matcher.find) {
      val file: String = matcher.group(1)
      if (!variables.contains(file)) {
        val variable = "__file_" + i + "__"
        variables.put(variable, loadResource(file))
        exp = exp.replace("[" + file + "]", variable)
        i += 1
      }
      matcher = filePattern.matcher(exp)
    }
    logInfo("Expression with files mapped: " + exp)

    exp
  }

  private def walkNodesForValidation(node: ParserNode):Unit = {
    node match {
    case const: ParserConstantNode =>
    case variable: ParserVariableNode =>
      val name = variable.getName
      if (!variables.containsKey(name)) {
        throw new ParserException("Variable \"" + name +
            "\" must be defined before used")
      }
    case function: ParserFunctionNode =>
      val name = function.getName
      if (name == "=") {
        if (function.getNumChildren != 2) {
          throw new ParserException("Variable \"" + name +
              "\" must be in the form " + name + " =  <expression>")
        }
        val varname = function.getChild(0).getName
        if (MapOpFactory.exists(varname)) {
          throw new ParserException("Cannot use variable name \"" + name +
              "\" because there is a a function of the same name")
        }
        variables.put(varname, None)
      }
      else {
        if (!MapOpFactory.exists(name)) {
          throw new ParserException("Function \"" + name + "\" does not exist")
        }
        // NOTE:  mapop constructor should throw ParserExceptions on error
        val op = MapOpFactory(name, function, variables)

      }
    }

    node.getChildren.foreach(child => {
      walkNodesForValidation(child)
    })
  }

  private def loadResource(name:String):Option[MapOp] = {
    val imdp = DataProviderFactory.getMrsImageDataProvider(name, AccessMode.READ, conf)

    if (imdp != null) {
      return Option(RasterMapOp(imdp))
    }

    //TODO:  Implement Vector
    //    val vdp = DataProviderFactory.getVectorDataProvider(name, AccessMode.READ, conf)
    //    if (vdp != null) {
    //      return new VectorRDD(vdp)
    //    }

    None

  }

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes.result()
  }

  var expression:String = null
  var output:String = null
  var providerproperties:ProviderProperties = null

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    expression = job.getSetting(MapAlgebra.MapAlgebra)
    output = job.getSetting(MapAlgebra.Output)

    providerproperties = ProviderProperties.fromDelimitedString(
      job.getSetting(MapAlgebra.ProviderProperties))

    true
  }

  override def execute(context: SparkContext): Boolean = {
    val nodes = parse(expression)

    nodes.foreach(node => {
    })

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}


object TestMapAlgebra extends App {

  val conf = HadoopUtils.createConfiguration()
  val pp = ProviderProperties.fromDelimitedString("")

  HadoopUtils.setupLocalRunner(conf)

  val expression = "x = 100; y = [/mrgeo/images/small-elevation]\n 10 / [/mrgeo/images/small-elevation] + " +
      "[/mrgeo/images/small-elevation] * [/mrgeo/images/small-elevation] - x + y"
  val output = "test-mapalgebra"
  if (MapAlgebra.validate(expression, conf, pp)) {
    MapAlgebra.run(expression, output, conf, pp)
  }
}
