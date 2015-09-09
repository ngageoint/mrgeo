package org.mrgeo.mapalgebra


import java.lang.reflect.Modifier

import org.apache.spark.Logging
import org.clapper.classutil.ClassInfo
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

  private def registerFunctions() = {
    val start = System.currentTimeMillis()

    val mapops = decendants(classOf[MapOpRegistrar]) getOrElse Set.empty

    val time = System.currentTimeMillis() - start

    logInfo("Registering MapOps:")

    val mirror = runtimeMirror(Thread.currentThread().getContextClassLoader)  // obtain runtime mirror

    mapops.foreach(symbol => {
      mirror.reflectModule(symbol.asModule).instance match {
      case mapop:MapOpRegistrar =>
        logInfo("  " + mapop)
        mapop.register.foreach(op => {
          functions.put(op, mapop)
        })
      case _ =>
      }

    })

    logInfo("Registration took " + time + "ms")
  }

  // create a mapop from a function name, called by MapOpFactory("<name>")
  def apply(name:String, node:ParserNode): Option[MapOp] = {
    if (functions.isEmpty) {
      registerFunctions()
    }

    functions.get(name) match {
    case Some(mapop) => Option(mapop.apply(name, node))
    case None => None
    }
  }

  private def decendants(clazz: Class[_]) = {
    val urls = {
      // get all the URLs from the classloader, and remove and .so files
      if (!MrGeoProperties.isDevelopmentMode) {
        ClasspathHelper.forClassLoader().filter(!_.getFile.endsWith(".so"))
      }
      else {
        // this is a development shortcut.  We know all our mapops are in mrgeo... jars
        logWarning("Development mode, only looking in jars with \"mrgeo\" in the name for MapOps")
        ClasspathHelper.forClassLoader().filter(_.getFile.contains("mrgeo"))
      }
    }

    // register mapops
    val cfg = new ConfigurationBuilder()
        .setUrls(urls)
        .setScanners(new SubTypesScanner())
        .useParallelExecutor()

    val reflections: Reflections = new Reflections(cfg)

    val classes = reflections.getSubTypesOf(clazz).filter(p => !Modifier.isAbstract(p.getModifiers))

    val mirror = runtimeMirror(Thread.currentThread().getContextClassLoader)  // obtain runtime mirror

    Some(classes.map(clazz => {
      val sym = mirror.classSymbol(clazz)

      if (sym.companionSymbol != null) {
        sym.companionSymbol
      }
      else {
        sym
      }
    }))
  }

  private def subclasses(name:String, classes:Stream[ClassInfo], depth:Int): mutable.Set[ClassInfo] = {
    val subs = mutable.HashSet[ClassInfo]()

    val sc = classes.filter(_.superClassName == name)

    for (c <- sc) {
      for (i <- Range(0, depth * 2)) {
        print(" ")
      }
      println("found " + c)
      subs ++= subclasses(c.name, classes, depth + 1)
    }

    subs ++= sc
    for (c <- subs) {
      for (i <- Range(0, depth * 2)) {
        print(" ")
      }
      println("** in list " + c)

    }

    subs.filter(_.isConcrete)
  }
}

