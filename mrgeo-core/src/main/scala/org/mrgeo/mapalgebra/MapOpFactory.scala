package org.mrgeo.mapalgebra


import java.io.File
import java.lang.reflect.Modifier

import org.apache.spark.Logging
import org.mrgeo.core.MrGeoProperties
import org.mrgeo.mapalgebra.parser.ParserNode
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.runtime.universe._


object MapOpFactory extends Logging {
  val functions = mutable.HashMap.empty[String, MapOpRegistrar]

  val mirror = runtimeMirror(Thread.currentThread().getContextClassLoader) // obtain runtime mirror
  private def registerFunctions() = {
    val start = System.currentTimeMillis()

    val mapops = decendants(classOf[MapOpRegistrar]) getOrElse Set.empty

    logInfo("Registering MapOps:")

    mapops.foreach(symbol => {
      mirror.reflectModule(symbol.asModule).instance match {
      case mapop: MapOpRegistrar =>
        logInfo("  " + mapop)
        mapop.register.foreach(op => {
          functions.put(op.toLowerCase, mapop)
        })
      case _ =>
      }
    })

    val time = System.currentTimeMillis() - start

    logInfo("Registration took " + time + "ms")
  }

  def getMapOpClasses: scala.collection.immutable.Set[Class[_]] = {
    var result = Set.newBuilder[Class[_]]
    functions.values.foreach(c => {
      result += c.getClass
    })
    result.result()
  }

  // create a mapop from a function name, called by MapOpFactory("<name>")
  def apply(node: ParserNode, variables: String => Option[ParserNode]): Option[MapOp] = {
    if (functions.isEmpty) {
      registerFunctions()
    }

    functions.get(node.getName.toLowerCase) match {
    case Some(mapop) =>
      val op = mapop.apply(node, variables)
      Some(op)
    case None => None
    }
  }

  def exists(name: String): Boolean = {
    if (functions.isEmpty) {
      registerFunctions()
    }

    functions.contains(name)
  }

  private def decendants(clazz: Class[_]) = {

    // get all the URLs for this classpath, filter files by "mrgeo" in development mode, then strip .so files
    val urls = (ClasspathHelper.forClassLoader() ++ ClasspathHelper.forJavaClassPath()).filter(url => {
      val file = new File(url.getPath)
      file.isDirectory || (if (MrGeoProperties.isDevelopmentMode) file.getName.contains("mrgeo") else file.isFile)
    }
    ).filter(p => !p.getFile.endsWith(".so") && !p.getFile.contains(".so."))


    // register mapops
    val cfg = new ConfigurationBuilder()
        .setUrls(urls)
        .setScanners(new SubTypesScanner())
        .useParallelExecutor()

    val reflections: Reflections = new Reflections(cfg)
    //val reflections: Reflections = new Reflections("org.mrgeo")

    val classes = reflections.getSubTypesOf(clazz).filter(p => {
      !Modifier.isAbstract(p.getModifiers)
    })

    Some(classes.map(clazz => {
      mirror.staticModule(clazz.getCanonicalName).asModule
    }))
  }
}

