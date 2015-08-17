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

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.net.URL
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputFormat, Job}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.spark._
import org.apache.spark.rdd.{OrderedRDDFunctions, PairRDDFunctions, RDD}
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.{ProviderProperties, DataProviderFactory}
import org.mrgeo.data.image.{MrsImagePyramidSimpleInputFormat, MrsImageDataProvider}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.{TiledInputFormatContext, TileIdWritable, TiledOutputFormatContext}
import org.mrgeo.hdfs.input.MapFileFilter
import org.mrgeo.hdfs.partitioners.ImageSplitGenerator
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo
import org.mrgeo.image.{ImageStats, MrsImagePyramid, MrsImagePyramidMetadata}
import org.mrgeo.spark.SparkTileIdPartitioner

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

object SparkUtils extends Logging {
  def calculateSplitData(rdd: RDD[(TileIdWritable, RasterWritable)]) = {

    // calculate the min/max tile id for each partition
    val partitions = rdd.mapPartitionsWithIndex((partition, data) => {
      var startId = Long.MaxValue
      var endId = Long.MinValue

      // not sure if the name is always part-r-xxxxx, but we'll use it for now.
      val name = f"part-r-$partition%05d"

      data.foreach(tile => {
        startId = Math.min(startId, tile._1.get())
        endId = Math.max(endId, tile._1.get())
      })

      val split = new FileSplitInfo(startId, endId, name, partition)

      val result = ListBuffer[(Int, FileSplitInfo)]()

      result.append((partition, split))

      result.iterator
    }, preservesPartitioning = true)


    val splits = Array.ofDim[FileSplitInfo](rdd.partitions.length)

    // collect the results and set the up the array
    partitions.collect().foreach(part => {
      splits(part._1) = part._2
    })

    splits
  }


  def getConfiguration: SparkConf = {

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
        .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
        .map { t => new File(s"$t${File.separator}spark-defaults.conf") }
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

    val providerProps: ProviderProperties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)
    val metadata: MrsImagePyramidMetadata = dp.getMetadataReader.read()

    (loadMrsPyramid(dp, metadata.getMaxZoomLevel, context), metadata)
  }

  def loadMrsPyramid(imageName: String, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val providerProps: ProviderProperties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)

    val metadata: MrsImagePyramidMetadata = dp.getMetadataReader.read()

    loadMrsPyramid(dp, metadata.getMaxZoomLevel, context)
  }

  def loadMrsPyramid(imageName: String, zoom: Int, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val providerProps: ProviderProperties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)

    loadMrsPyramid(dp, zoom, context)
  }

  def loadMrsPyramid(provider: MrsImageDataProvider, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val metadata: MrsImagePyramidMetadata = provider.getMetadataReader.read()

    loadMrsPyramid(provider, metadata.getMaxZoomLevel, context)
  }

  def loadMrsPyramid(provider:MrsImageDataProvider, zoom:Int, context: SparkContext): RDD[(TileIdWritable, RasterWritable)] = {
    val metadata: MrsImagePyramidMetadata = provider.getMetadataReader.read()

    val conf1 = provider.setupSparkJob(context.hadoopConfiguration)
    val inputs = Set(provider.getResourceName)
    val tifc = new TiledInputFormatContext(zoom, metadata.getTilesize, inputs, provider.getProviderProperties)
    val ifp = provider.getTiledInputFormatProvider(tifc)
    val conf2 = ifp.setupSparkJob(conf1, provider)

//    MrsImageDataProvider.setupMrsPyramidSingleSimpleInputFormat(job, provider.getResourceName,
//      zoom, metadata.getTilesize, null, providerProps) // null for bounds means use all tiles (no cropping)

    // build a phony job...
    val job = Job.getInstance(conf2)
//    val inputFormatClass: Class[InputFormat[TileIdWritable, RasterWritable]] = job.getInputFormatClass
//        .asInstanceOf[Class[InputFormat[TileIdWritable, RasterWritable]]]

//    log.warn("Running loadPyramid with configuration " + job.getConfiguration + " with input format " +
//      inputFormatClass.getName)
    context.newAPIHadoopRDD(job.getConfiguration,
      classOf[MrsImagePyramidSimpleInputFormat],
      classOf[TileIdWritable],
      classOf[RasterWritable])

//        FileInputFormat.addInputPath(job, new Path(provider.getResourceName, zoom.toString))
//        FileInputFormat.setInputPathFilter(job, classOf[MapFileFilter])
//
//        context.newAPIHadoopRDD(job.getConfiguration,
//          classOf[SequenceFileInputFormat[TileIdWritable, RasterWritable]],
//          classOf[TileIdWritable],
//          classOf[RasterWritable])
  }

  def saveMrsPyramid(tiles: RDD[(TileIdWritable, RasterWritable)], provider: MrsImageDataProvider,
      zoom:Int, conf:Configuration, providerproperties:ProviderProperties): Unit = {

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

  def saveMrsPyramid(tiles: RDD[(TileIdWritable, RasterWritable)],
      outputProvider: MrsImageDataProvider, inputprovider: MrsImageDataProvider,
      zoom:Int, conf:Configuration, providerproperties:ProviderProperties): Unit = {

    val metadata = inputprovider.getMetadataReader.read()

    val bounds = metadata.getBounds
    val bands = metadata.getBands
    val tiletype = metadata.getTileType
    val tilesize = metadata.getTilesize
    val nodatas = metadata.getDefaultValues
    val protectionlevel = metadata.getProtectionLevel

    val output = outputProvider.getResourceName

    saveMrsPyramid(tiles, outputProvider, output, zoom, tilesize, nodatas, conf,
      tiletype, bounds, bands, protectionlevel, providerproperties)
  }

  def saveMrsPyramid(tiles: RDD[(TileIdWritable, RasterWritable)], provider: MrsImageDataProvider, output: String,
      zoom: Int, tilesize: Int, nodatas: Array[Double], conf: Configuration, tiletype: Int = -1,
      bounds: Bounds = new Bounds(), bands: Int = -1,
      protectionlevel:String = null, providerproperties:ProviderProperties = new ProviderProperties()): Unit = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    tiles.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val tileIncrement = 1

    var localbounds = bounds
    var localbands = bands
    var localtiletype = tiletype
    val output = provider.getResourceName

    if (!localbounds.isValid) {
      localbounds = SparkUtils.calculateBounds(tiles, zoom, tilesize)
    }

    if (localbands <= 0 || localtiletype <= 0) {
      val tile = RasterWritable.toRaster(tiles.first()._2)

      localbands = tile.getNumBands
      localtiletype = tile.getTransferType
    }

    // calculate stats.  Do this after the save to give S3 a chance to finalize the actual files before moving
    // on.  This can be a problem for fast calculating/small partitions
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

    val tofc = new TiledOutputFormatContext(output, localbounds, zoom, tilesize)
    val tofp = provider.getTiledOutputFormatProvider(tofc)
    val conf1 = tofp.setupSparkJob(conf)

    // The following commented out section was in place for older versions of Spark
    // that did not include the OrderRDDFunctions.repartitionAndSortWithinPartitions
    // method. Since we're standardizing on Spark 1.2.0 as a minimum, we leave the
    // following commented out.
    //    var repartitionMethod: java.lang.reflect.Method = null
    //    val orderedTiles = new OrderedRDDFunctions[TileIdWritable, RasterWritable, (TileIdWritable, RasterWritable)](tiles)
    //    val repartitionMethodName = "repartitionAndSortWithinPartitions"
    //    try {
    //      repartitionMethod = orderedTiles.getClass.getDeclaredMethod(repartitionMethodName,
    //        classOf[Partitioner])
    //    }
    //    catch {
    //      case nsm: NoSuchMethodException => {
    //        // Ignore. On older versions of Spark, this method does not exist, and
    //        // that is handled later in the code with repartitionMethod == null.
    //      }
    //    }
    //    if (repartitionMethod != null) {
    //      // The new method exists, so let's call it through reflection because it's
    //      // more efficient.
    //      log.info("Saving MrsPyramid using new repartition method")
    //      val sorted: RDD[(TileIdWritable, RasterWritable)] = repartitionMethod.invoke(orderedTiles, sparkPartitioner).asInstanceOf[RDD[(TileIdWritable, RasterWritable)]]
    //      val saveSorted = new PairRDDFunctions(sorted)
    //      val saveMethodName = "saveAsNewAPIHadoopFile"
    //      val saveMethod = saveSorted.getClass.getDeclaredMethod(saveMethodName,
    //        classOf[String] /* name */,
    //        classOf[Class[Any]]  /* keyClass */,
    //        classOf[Class[Any]] /*valueClass */,
    //        classOf[Class[OutputFormat[Any,Any]]] /* outputFormatClass */,
    //        classOf[Configuration] /* configuration */)
    //      if (saveMethod != null) {
    //        saveMethod.invoke(saveSorted, name, classOf[TileIdWritable], classOf[RasterWritable],
    //          tofp.getOutputFormat.getClass, conf)
    //        //        sorted.saveAsNewAPIHadoopFile(name, classOf[TileIdWritable], classOf[RasterWritable], tofp.getOutputFormat.getClass, conf)
    //        //logInfo("sorted has " + sorted.count() + " tiles in " + sorted.partitions.length + " partitions")
    //      }
    //      else {
    //        val msg = "Unable to find method " + saveMethodName + " in class " + saveSorted.getClass.getName
    //        logError(msg)
    //        throw new IllegalArgumentException(msg)
    //      }
    //    }
    //    else {
    //      // This is an older version of Spark, so use the old partition and sort.
    //      log.info("Saving MrsPyramid using old repartition method")
    //      val wrapped = new PairRDDFunctions(tiles)
    //      val partitioned = wrapped.partitionBy(sparkPartitioner)
    //
    //      //logInfo("partitioned has " + partitioned.count() + " tiles in " + partitioned.partitions.length + " partitions")
    //      // free up the tile's cache, it's not needed any more...
    //
    //      val wrapped1 = new OrderedRDDFunctions[TileIdWritable, RasterWritable, (TileIdWritable, RasterWritable)](partitioned)
    //      var s = new PairRDDFunctions(wrapped1.sortByKey())
    //      s.saveAsNewAPIHadoopFile(name, classOf[TileIdWritable], classOf[RasterWritable], tofp.getOutputFormat.getClass, conf)
    //    }

    val wrappedTiles = new OrderedRDDFunctions[TileIdWritable, RasterWritable, (TileIdWritable, RasterWritable)](tiles)
    val sorted = wrappedTiles.repartitionAndSortWithinPartitions(sparkPartitioner)
    val wrappedSorted = new PairRDDFunctions(sorted)
    wrappedSorted.saveAsNewAPIHadoopDataset(conf1)

    tiles.unpersist()
    sparkPartitioner.generateFileSplits(sorted, output, zoom, conf1)
    //sparkPartitioner.writeSplits(output, zoom, conf) // job.getConfiguration)

    //dp.teardown(job)

    // calculate and save metadata
    MrsImagePyramid.calculateMetadata(output, zoom, provider, stats,
      nodatas, localbounds, conf1,  protectionlevel, providerproperties)
  }

  def calculateStats(rdd: RDD[(TileIdWritable, RasterWritable)], bands: Int,
      nodata: Array[Double]): Array[ImageStats] = {

    val zero = Array.ofDim[ImageStats](bands)

    for (i <- zero.indices) {
      zero(i) = new ImageStats(Double.MaxValue, Double.MinValue, 0, 0)
    }

    val stats = rdd.aggregate(zero)((stats, t) => {
      val tile = RasterWritable.toRaster(t._2)

      for (y <- 0 until tile.getHeight) {
        for (x <- 0 until tile.getWidth) {
          for (b <- 0 until tile.getNumBands) {
            val p = tile.getSampleDouble(x, y, b)
            if (nodata(b).isNaN) {
              if (!p.isNaN) {
                stats(b).count += 1
                stats(b).sum += p
                stats(b).max = Math.max(stats(b).max, p)
                stats(b).min = Math.min(stats(b).min, p)
              }
            }
            else if (p != nodata(b)) {
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

        for (b <- aggstat.indices) {
          aggstat(b).count += stat2(b).count
          aggstat(b).sum += stat2(b).sum
          aggstat(b).max = Math.max(aggstat(b).max, stat2(b).max)
          aggstat(b).min = Math.min(aggstat(b).min, stat2(b).min)
        }

        aggstat
      })

    for (i <- stats.indices) {
      if (stats(i).count > 0) {
        stats(i).mean = stats(i).sum / stats(i).count
      }
    }

    stats
  }

  def calculateBounds(rdd: RDD[(TileIdWritable, RasterWritable)], zoom: Int, tilesize: Int): Bounds = {

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


  def humantokb(human: String): Int = {
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

    val v: Int = trimmed.substring(0, trimmed.length - 1).toInt
    v * mult
  }

  def kbtohuman(kb: Long, maxUnit: String = null): String = {
    if (kb == 0) {
      "0"
    }
    else {
      val suffix = new String("kmgtpe")
      val unit = 1024
      var exp: Int = (Math.log(kb) / Math.log(unit)).toInt

      if (maxUnit != null) {
        val maxexp = suffix.indexOf(maxUnit.trim.toLowerCase)
        if (maxexp > 0 && exp > maxexp) {
          exp = maxexp
        }
      }

      val pre: Char = suffix.charAt(exp)

      "%d%s".format((kb / Math.pow(unit, exp)).toInt, pre)
    }
  }

  def jarForClass(clazz: String, cl: ClassLoader = null): String = {
    // now the hard part, need to look in the dependencies...
    val classFile: String = clazz.replaceAll("\\.", "/") + ".class"

    var iter: java.util.Enumeration[URL] = null

    if (cl != null) {
      iter = cl.getResources(classFile)
    }
    else {
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

  def jarsForClass(clazz: String, cl: ClassLoader = null): Array[String] = {
    // now the hard part, need to look in the dependencies...
    val classFile: String = clazz.replaceAll("\\.", "/") + ".class"

    jarsForPackage(classFile, cl)
  }

  def jarsForPackage(pkg: String, cl: ClassLoader = null): Array[String] = {
    // now the hard part, need to look in the dependencies...
    var iter: java.util.Enumeration[URL] = null

    val pkgFile: String = pkg.replaceAll("\\.", "/")

    if (cl != null) {
      iter = cl.getResources(pkgFile)
    }
    else {
      val cll = getClass.getClassLoader
      iter = cll.getResources(pkgFile)
    }

    val ab: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make()
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


  def address(obj: Object): String = {
    var addr = "0x"

    val array = Array(obj)
    val f = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    val unsafe = f.get(null).asInstanceOf[sun.misc.Unsafe]


    val offset: Long = unsafe.arrayBaseOffset(classOf[Array[Object]])
    val scale = unsafe.arrayIndexScale(classOf[Array[Object]])

    scale match {
    case 4 =>
      val factor = 8
      val i1 = (unsafe.getInt(array, offset) & 0xFFFFFFFFL) * factor
      addr += i1.toHexString
    case 8 =>
      throw new AssertionError("Not supported")
    }

    addr
  }

}
