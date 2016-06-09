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


import java.io.{File, FileFilter}
import java.lang.reflect.Modifier
import java.net.URL

import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.Logging
import org.mrgeo.core.MrGeoProperties
import org.mrgeo.mapalgebra.parser.ParserNode
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.existentials
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
          logInfo("    (" + op.toLowerCase + ")")
          functions.put(op.toLowerCase, mapop)
        })
      case _ =>
      }
    })

    val time = System.currentTimeMillis() - start

    logInfo("Registration took " + time + "ms")
  }

  def describe():Array[(String, String, String)] = {
    val mapops = Array.newBuilder[(String, String, String)]

    if (functions.isEmpty) {
      registerFunctions()
    }

    functions.foreach(name => {
      name._2 match {
      case mapop:MapOp =>
        mapops += ((name._1, "TODO:  call mapop.description" /* mapop.description */ ,
            "TODO: call mapop.usage" /* mapop.usage */ ))
      case _ =>
      }
    })

    mapops.result()
  }

  def getMapOpClasses: Array[Class[_]] = {
    if (functions.isEmpty) {
      registerFunctions()
    }

    val result = Set.newBuilder[Class[_]]
    functions.values.foreach(c => {
      result += c.getClass
    })
    result.result().toArray
  }

  def getMapOpClassNames: Array[String] = {

    val result = Array.newBuilder[String]
    getMapOpClasses.foreach(cl =>{
      result += cl.getCanonicalName
    })

    result.result()
  }

  def getMapOpNames: Array[String] = {
    if (functions.isEmpty) {
      registerFunctions()
    }

    val result = Set.newBuilder[String]
    functions.keysIterator.foreach(c => {
      result += c
    })
    result.result().toArray
  }

  def getSignatures(classname:String): Array[String] = {
    if (functions.isEmpty) {
      registerFunctions()
    }

    val mapop = functions.values.filter(_.getClass.getCanonicalName.startsWith(classname)).head

    val im = mirror reflect mapop
    val create = newTermName("create")
    val ts = im.symbol.typeSignature
    val method = ts.member(create)

    val rawsig = method match {
    case symbol: TermSymbol =>
      symbol.alternatives.map {
        case creaters: MethodSymbol =>
          creaters.paramss.head.map(_.asTerm).zipWithIndex.map {
            case (term, index) =>
              // If the term is a primitive, then use the lower case of the actual type
              // name because Double in scala equates to double in Java (not Double).
              // If the term is an array, then do the same for the type of the elements
              // it stores.
              term.name + ":" + (if (term.typeSignature.typeSymbol == definitions.ArrayClass) {
                val arrayElementType = term.typeSignature.asInstanceOf[TypeRefApi].args.head
                val elementTypeName = if (arrayElementType <:< typeOf[AnyVal]) {
                  arrayElementType.toString.toLowerCase()
                }
                else {
                  arrayElementType.toString
                }
                elementTypeName + "*"
              }
              else {
                if (term.typeSignature <:< typeOf[AnyVal]) {
                  term.typeSignature.toString.toLowerCase
                }
                else {
                  term.typeSignature.toString
                }
              }) + {
                if (term.isParamWithDefault) {
                  val getter = ts member newTermName("create$default$" + (index + 1))
                  if (getter != NoSymbol) {
                    "=" + ((im reflectMethod getter.asMethod)() match {
                    case s:String => "\"" + s + "\""
                    case x => x
                    })
                  }
                  else {
                    method.typeSignature match {
                    case t if t =:= typeOf[String] => null
                    case t if t =:= typeOf[Double] => 0.0
                    case t if t =:= typeOf[Float]  => 0.0F
                    case t if t =:= typeOf[Long]   => 0L
                    case t if t =:= typeOf[Int]    => 0
                    case x                         => throw new IllegalArgumentException(x.toString)
                    }
                  }
                }
                else {
                  ""
                }
              }
          }.mkString("|")
        case _ => ""
      }
    case _ => Seq.empty[String]
    }

    rawsig.toArray
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
    // in spark, the main jar is renamed "__app__.jar" (Client.APP_JAR), so we need to include that as well
    val urls = (ClasspathHelper.forClassLoader() ++ ClasspathHelper.forJavaClassPath()).filter(url => {
      val file = new File(url.getPath)
      file.isDirectory || (if (MrGeoProperties.isDevelopmentMode) file.getName.contains("mrgeo") || file.getName.equals("__app__.jar") else file.isFile)
    }).filter(p => !p.getFile.endsWith(".so") && !p.getFile.contains(".so."))
        .flatMap(url => {
          val us = url.getFile
          if (us.contains("*") || us.contains("?")) {
            val f = new File(us)
            val base = f.getParentFile

            val filter = new WildcardFileFilter(f.getName).asInstanceOf[FileFilter]
            val files = base.listFiles(filter)

            files.map(f => f.toURI.toURL)
          }
          else Array[URL](url)
        })

    if (log.isDebugEnabled) {
      logDebug("Classpath for finding MapOps:")
      urls.foreach(url => {
        val file = new File(url.getPath)
        logDebug("  " + url.toString + "  (" + {if (file.isDirectory) "dir" else "file"} + ")" )
      })
    }

    // register mapops
    val cfg = new ConfigurationBuilder()
        .setUrls(urls.toSeq)
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



