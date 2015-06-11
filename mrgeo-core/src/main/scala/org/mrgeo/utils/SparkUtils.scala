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

import java.io.{IOException, FileInputStream, InputStreamReader, File}
import java.net.URL
import java.util
import java.util.{Enumeration, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputFormat, Job}
import org.apache.spark.{SparkException, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.{TiledOutputFormatContext, TileIdWritable}
import org.mrgeo.hdfs.partitioners.{ImageSplitGenerator, TileIdPartitioner}
import org.mrgeo.image.{MrsImagePyramid, ImageStats, MrsImagePyramidMetadata}
import org.mrgeo.spark.SparkTileIdPartitioner

import scala.collection.{Map, mutable}
import scala.collection.JavaConversions._

object SparkUtils {

  def getConfiguration:SparkConf = {

    val conf = new SparkConf()
    loadDefaultSparkProperties(conf)

    conf
  }

  // These 3 methods are taken almost verbatim from Spark's Utils class, but they are all
  // private, so we needed to copy them here
  /** Load properties present in the given file. */
  private def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile, s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    }
    catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    }
    finally {
      inReader.close()
    }
  }

  private def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("SPARK_CONF_DIR")
        .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf"})
        .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
        .filter(_.isFile)
        .map(_.getAbsolutePath)
        .orNull
  }

  private def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("spark.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }


  def loadMrsPyramidAndMetadata(imageName: String, context: SparkContext):
  (RDD[(TileIdWritable, RasterWritable)], MrsImagePyramidMetadata) = {
    val keyclass = classOf[TileIdWritable]
    val valueclass = classOf[RasterWritable]

    // build a phony job...
    val job = new Job()

    val providerProps: Properties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)
    val metadata: MrsImagePyramidMetadata = dp.getMetadataReader.read()

    MrsImageDataProvider.setupMrsPyramidSingleSimpleInputFormat(job, imageName, providerProps)

    val inputFormatClass: Class[InputFormat[TileIdWritable, RasterWritable]] = job.getInputFormatClass
        .asInstanceOf[Class[InputFormat[TileIdWritable, RasterWritable]]]
    val image = context.newAPIHadoopRDD(job.getConfiguration,
      inputFormatClass,
      keyclass,
      valueclass)

    (image, metadata)
  }

  def loadMrsPyramid(imageName: String, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val providerProps: Properties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)

    loadMrsPyramid(dp, context)
  }

  def loadMrsPyramid(imageName: String, zoom:Int, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val providerProps: Properties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)

    loadMrsPyramid(dp, zoom, context)
  }

  def loadMrsPyramid(provider:MrsImageDataProvider, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val keyclass = classOf[TileIdWritable]
    val valueclass = classOf[RasterWritable]

    // build a phony job...
    val job = new Job()

    val providerProps: Properties = null
    MrsImageDataProvider.setupMrsPyramidSingleSimpleInputFormat(job, provider.getResourceName, providerProps)

    val inputFormatClass: Class[InputFormat[TileIdWritable, RasterWritable]] = job.getInputFormatClass
        .asInstanceOf[Class[InputFormat[TileIdWritable, RasterWritable]]]

    context.newAPIHadoopRDD(job.getConfiguration,
      inputFormatClass,
      keyclass,
      valueclass)
  }

  def loadMrsPyramid(provider:MrsImageDataProvider, zoom:Int, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val keyclass = classOf[TileIdWritable]
    val valueclass = classOf[RasterWritable]

    val metadata: MrsImagePyramidMetadata = provider.getMetadataReader.read()

    // build a phony job...
    val job = new Job()

    val providerProps: Properties = null
    MrsImageDataProvider.setupMrsPyramidSingleSimpleInputFormat(job, provider.getResourceName, zoom,
      metadata.getTilesize, metadata.getBounds, providerProps)

    val inputFormatClass: Class[InputFormat[TileIdWritable, RasterWritable]] = job.getInputFormatClass
        .asInstanceOf[Class[InputFormat[TileIdWritable, RasterWritable]]]

    context.newAPIHadoopRDD(job.getConfiguration,
      inputFormatClass,
      keyclass,
      valueclass)
  }

  def saveMrsPyramid(tiles: RDD[(TileIdWritable, RasterWritable)], provider:MrsImageDataProvider,
      zoom:Int, conf:Configuration, providerproperties:Properties): Unit = {

    val metadata = provider.getMetadataReader.read()

    val bounds = metadata.getBounds
    val bands = metadata.getBands
    val tiletype = metadata.getTileType
    val tilesize = metadata.getTilesize
    val nodatas = metadata.getDefaultValues
    val output = provider.getResourceName
    val protectionlevel = metadata.getProtectionLevel

    saveMrsPyramid(tiles, provider, output, zoom, tilesize, nodatas, conf,
      tiletype, bounds, bands, protectionlevel, providerproperties)
  }

  def saveMrsPyramid(tiles: RDD[(TileIdWritable, RasterWritable)], provider:MrsImageDataProvider, output:String,
      zoom:Int, tilesize:Int, nodatas:Array[Double], conf:Configuration, tiletype:Int = -1,
      bounds:Bounds = new Bounds(), bands:Int = -1,
      protectionlevel:String = null, providerproperties:Properties = new Properties()): Unit = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    val tileIncrement = 1

    var localbounds = bounds
    var localbands = bands
    var localtiletype = tiletype
    val output = provider.getResourceName

    conf.setInt(TileIdPartitioner.INCREMENT_KEY, tileIncrement)

    if (!localbounds.isValid) {
      localbounds = SparkUtils.calculateBounds(tiles, zoom, tilesize)
    }

    if (localbands <= 0  || localtiletype <= 0) {
      val tile = RasterWritable.toRaster(tiles.first()._2)

      localbands = tile.getNumBands
      localtiletype = tile.getTransferType
    }

    // calculate stats
    val stats = SparkUtils.calculateStats(tiles, localbands, nodatas)


    // save the new pyramid
    //    val dp = MrsImageDataProvider.setupMrsPyramidOutputFormat(job, output, bounds, zoom,
    //      tilesize, tiletype, bands, protectionlevel, providerproperties)

    val tileBounds = TMSUtils.boundsToTile(localbounds.getTMSBounds, zoom, tilesize)

    //val splitGenerator = new ImageSplitGenerator(tileBounds.w, tileBounds.s,
    //  tileBounds.e, tileBounds.n, zoom, tileIncrement)

    val splitGenerator = new ImageSplitGenerator(tileBounds.w, tileBounds.s,
      tileBounds.e, tileBounds.n, zoom, tileIncrement)

    val sparkPartitioner = new SparkTileIdPartitioner(splitGenerator)

    //logInfo("tiles has " + tiles.count() + " tiles in " + tiles.partitions.length + " partitions")

    //val partitioned = tiles.partitionBy(sparkPartitioner)

    //logInfo("partitioned has " + partitioned.count() + " tiles in " + partitioned.partitions.length + " partitions")
    // free up the tile's cache, it's not needed any more...

    //val sorted = partitioned.sortByKey()
    //logInfo("sorted has " + sorted.count() + " tiles in " + sorted.partitions.length + " partitions")

    // this is missing in early spark APIs
    val sorted = tiles.repartitionAndSortWithinPartitions(sparkPartitioner)

    // save the image
    //sorted.saveAsNewAPIHadoopDataset(conf) // job.getConfiguration)

    //    path: String,
    //    keyClass: Class[_],
    //    valueClass: Class[_],
    //    outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
    //    conf: Configuration = self.context.hadoopConfiguration)

    val tofc = new TiledOutputFormatContext(output, localbounds, zoom, tilesize)
    val tofp = provider.getTiledOutputFormatProvider(tofc)

    val writer = provider.getMrsTileWriter(zoom)
    val name = new Path(writer.getName).getParent.toString

    //println("saving to: " + name)
    sorted.saveAsNewAPIHadoopFile(name, classOf[TileIdWritable], classOf[RasterWritable], tofp.getOutputFormat.getClass, conf)

    //    sorted.foreachPartition(iter => {
    //      var writer:MrsTileWriter[Raster] = null
    //
    //      try {
    //        while (iter.hasNext) {
    //          val item = iter.next()
    //
    //          val key = item._1
    //          val value = item._2
    //
    //          if (writer == null) {
    //            val partition = sparkPartitioner.getPartition(key)
    //            //println("getting writer for: " + partition)
    //
    //            val dp = DataProviderFactory.getMrsImageDataProviderNoCache(output, AccessMode.WRITE,
    //              null.asInstanceOf[Properties])
    //
    //            val context = new MrsImagePyramidWriterContext(zoom, partition)
    //            writer = dp.getMrsTileWriter(context)
    //          }
    //
    //          //println("writing: " + key.get + " to " + writer.getName )
    //          writer.append(key, RasterWritable.toRaster(value))
    //        }
    //        //println("done looping")
    //      } finally {
    //        if (writer != null) {
    //          //println("closing: " + writer.getName)
    //          writer.close()
    //        }
    //      }
    //    })

    sparkPartitioner.writeSplits(output, zoom, conf) // job.getConfiguration)

    //dp.teardown(job)

    // calculate and save metadata
    MrsImagePyramid.calculateMetadata(output, zoom, provider.getMetadataWriter, stats,
      nodatas, localbounds, conf,  protectionlevel, providerproperties)
  }


  def calculateStats(rdd: RDD[(TileIdWritable, RasterWritable)], bands: Int,
      nodata: Array[Double]): Array[ImageStats] = {

    val zero = Array.ofDim[ImageStats](bands)

    for (i <- 0 until zero.length) {
      zero(i) = new ImageStats(Double.MaxValue, Double.MinValue, 0, 0)
    }

    val stats = rdd.aggregate(zero)((stats, t) => {
      val tile = RasterWritable.toRaster(t._2)

      for (y <- 0 until tile.getHeight) {
        for (x <- 0 until tile.getWidth) {
          for (b <- 0 until tile.getNumBands) {
            val p = tile.getSampleDouble(x, y, b)
            if (nodata(b).isNaN && !p.isNaN || nodata(b) != p) {
              stats(b).count += 1
              stats(b).sum += p
              stats(b).max = Math.max(stats(b).max, p)
              stats(b).min = Math.min(stats(b).min, p)
            }
          }
        }
      }

      stats
    },
      (stat1, stat2) => {
        val aggstat = stat1.clone()

        for (b <- 0 until aggstat.length) {
          aggstat(b).count += stat2(b).count
          aggstat(b).sum += stat2(b).sum
          aggstat(b).max = Math.max(aggstat(b).max, stat2(b).max)
          aggstat(b).min = Math.min(aggstat(b).min, stat2(b).min)
        }

        aggstat
      })

    for (i <- 0 until stats.length) {
      if (stats(i).count > 0) {
        stats(i).mean = stats(i).sum / stats(i).count
      }
    }

    stats
  }

  def calculateBounds(rdd: RDD[(TileIdWritable, RasterWritable)], zoom:Int, tilesize:Int): Bounds = {

    val bounds = rdd.aggregate(new Bounds())((bounds, t) => {
      val tile = TMSUtils.tileid(t._1.get, zoom)

      val tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, tilesize).asBounds()
      tb.expand(bounds)

      tb
    },
      (tb1, tb2) => {
        tb1.expand(tb2)

        tb1
      })

    bounds
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

  def kbtohuman(kb:Long, maxUnit:String = null):String = {
    if (kb == 0) {
      "0"
    }
    else {
      val suffix = new String("kmgtpe")
      val unit = 1024
      var exp: Int = (Math.log(kb) / Math.log(unit)).toInt

      if (maxUnit != null) {
        val maxexp = suffix.indexOf(maxUnit.trim.toLowerCase)
        if (maxexp > 0 && exp > maxexp)
        {
          exp = maxexp
        }
      }

      val pre: Char = suffix.charAt(exp)

      "%d%s".format((kb / Math.pow(unit, exp)).toInt, pre)
    }
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

  def jarsForClass(clazz:String, cl:ClassLoader = null): Array[String] = {
    // now the hard part, need to look in the dependencies...
    val classFile: String = clazz.replaceAll("\\.", "/") + ".class"

    jarsForPackage(classFile, cl)
  }

  def jarsForPackage(pkg:String, cl:ClassLoader = null): Array[String] = {
    // now the hard part, need to look in the dependencies...
    var iter: Enumeration[URL] = null

    val pkgFile: String = pkg.replaceAll("\\.", "/")

    if (cl != null) {
      iter = cl.getResources(pkgFile)
    }
    else
    {
      val cll = getClass.getClassLoader
      iter = cll.getResources(pkgFile)
    }

    val ab:mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make()
    while (iter.hasMoreElements) {
      val url: URL = iter.nextElement
      if (url.getProtocol == "jar") {
        val path: String = url.getPath
        if (path.startsWith("file:")) {
          // strip off the "file:" and "!<classname>"
          ab += path.substring("file:".length).replaceAll("!.*$", "")
        }
      }
    }

    ab.result()
  }

}
