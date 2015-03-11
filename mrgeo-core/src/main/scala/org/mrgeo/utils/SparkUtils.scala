/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.utils

import java.net.URL
import java.util
import java.util.Properties

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.input.image.HdfsMrsImagePyramidSimpleInputFormat
import org.mrgeo.image.MrsImagePyramidMetadata

import scala.util.control.Breaks

object SparkUtils {

  def loadMrsPyramid(imageName: String, context: SparkContext):
  (RDD[(TileIdWritable, RasterWritable)], MrsImagePyramidMetadata) =
  {
    val keyclass = classOf[TileIdWritable]
    val valueclass = classOf[RasterWritable]
    
    //TODO:   Get this from the DataProvider
    val inputformatclass = classOf[HdfsMrsImagePyramidSimpleInputFormat] // MrsImagePyramidInputFormat]

    // build a phony job...
    val job = new Job()

    val providerProps: Properties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)

    val metadata: MrsImagePyramidMetadata = dp.getMetadataReader.read()

    FileInputFormat.addInputPaths(job, imageName + "/" + metadata.getMaxZoomLevel)

    //    MrsImageDataProvider.setupMrsPyramidInputFormat(job, imageName, metadata.getMaxZoomLevel,
    //      metadata.getTilesize, providerProps)

    // calculate the friction surface
    val image = context.newAPIHadoopRDD(job.getConfiguration,
      inputformatclass,
      keyclass,
      valueclass).persist(StorageLevel.MEMORY_AND_DISK_SER)

    (image, metadata)
  }

  def humantokb(human:String):Int = {
    //val pre: Char = new String ("KMGTPE").charAt (exp - 1)
    val trimmed = human.trim.toLowerCase
    val units = trimmed.charAt(trimmed.length - 1)
    val exp = units match {
    case 'k' => 0
    case 'm' => 1
    case 'g' => 2
    case 'p' => 3
    case 'e' => 4
    case _ => return trimmed.substring(0, trimmed.length - 2).toInt
    }

    val mult = Math.pow(1024, exp).toInt

    val v:Int = trimmed.substring(0, trimmed.length - 1).toInt
    v * mult
  }

  def kbtohuman(kb:Int):String = {
    val unit = 1024
    val kbunit = unit * unit
    val exp: Int = (Math.log(kb) / Math.log(kbunit)).toInt
    val pre: Char = new String("MGTPE").charAt(exp)

    "%d%s".format((kb / Math.pow(unit, exp + 1)).toInt, pre)
  }

  def jarForClass(clazz:String, cl:ClassLoader = null): String = {
    // now the hard part, need to look in the dependencies...
    val classFile: String = clazz.replaceAll("\\.", "/") + ".class"

    var iter: util.Enumeration[URL] = null

    if (cl != null) {
      iter = cl.getResources(classFile)
    }
    else
    {
      val cll = getClass.getClassLoader
      iter = cll.getResources(classFile)
    }

      while (iter.hasMoreElements) {
        val url: URL = iter.nextElement
        if (url.getProtocol == "jar") {
          val path: String = url.getPath
          if (path.startsWith("file:")) {
            // strip off the "file:" and "!<classname>"
            return path.substring("file:".length).replaceAll("!.*$", "")
          }
        }
      }

    null
  }

}
