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

package org.mrgeo.ingest

import java.io._
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{MrGeoRaster, RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{DataProviderFactory, ProtectionLevelUtils, ProviderProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils._
import org.mrgeo.utils.tms.{Bounds, TMSUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object IngestImage extends MrGeoDriver with Externalizable {

  private val Inputs = "inputs"
  private val Output = "output"
  private val Bounds = "bounds"
  private val Zoom = "zoom"
  private val Tilesize = "tilesize"
  private val NoData = "nodata"
  private val Tiletype = "tiletype"
  private val Bands = "bands"
  private val Categorical = "categorical"
  private val SkipCategoryLoad = "skipCategoryLoad"
  private val Tags = "tags"
  private val Protection = "protection"
  private val ProviderProperties = "provider.properties"

  def ingest(inputs: Array[String], output: String,
      categorical: Boolean, skipCategoryLoad: Boolean, conf: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Array[Number], bands: Int, tiletype: Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: ProviderProperties): Boolean = {

    val name = "IngestImage"

    val args = setupParams(inputs.mkString(","), output, categorical, skipCategoryLoad, bounds,
      zoomlevel, tilesize, nodata, bands, tiletype, tags, protectionLevel, providerProperties)

    run(name, classOf[IngestImage].getName, args.toMap, conf)

    true
  }

  private def equalCategories(c1: Map[Int, util.Vector[_]],
                              c2: Map[Int, util.Vector[_]]): Boolean = {
    if (c1.size != c2.size) {
      return false
    }
    c1.foreach(c1Entry => {
      val c1Band = c1Entry._1
      val c1Cats = c1Entry._2
      val c2Value = c2.get(c1Band)
      c2Value match {
        case Some(c2Cats) => {
          if (c1Cats.size() != c2Cats.size()) {
            return false
          }
          // The values stored in c1Cats and c2Cats are strings (from GDAL). Do a case
          // insensitive comparison of them and return false if any don't match.
          c1Cats.zipWithIndex.foreach(c1Entry => {
            if (!c1Entry._1.toString.equalsIgnoreCase(c2Cats.get(c1Entry._2).toString)) {
              return false
            }
          })
        }
        case None => {
          return false
        }
      }
    })
    true
  }

  def ingest(context: SparkContext, inputs:Array[String], zoom:Int, tilesize:Int,
             categorical:Boolean, skipCategoryLoad: Boolean, nodata: Array[Number],
             protectionLevel: String) = {
    var firstCategories: Map[Int, util.Vector[_]] = null
    var categoriesMatch: Boolean = false

    if (categorical && !skipCategoryLoad) {
      val checkResult = checkAndLoadCategories(context, inputs)
      categoriesMatch = checkResult._1
      firstCategories = checkResult._2
    }

    // force 1 partition per file, this will keep the size of each ingest task as small as possible, so we
    // won't eat up too much memory
    val in = context.parallelize(inputs, inputs.length)
    val rawtiles = new PairRDDFunctions(in.flatMap(input => {
      IngestImage.makeTiles(input, zoom, tilesize, categorical, nodata)
    }))

    
    val tiles = rawtiles.reduceByKey((r1, r2) => {
      val src = RasterWritable.toMrGeoRaster(r1)
      val dst = RasterWritable.toMrGeoRaster(r2)

      dst.mosaic(src, nodata)

      RasterWritable.toWritable(dst)
    })

    val meta = SparkUtils.calculateMetadata(RasterRDD(tiles), zoom, nodata, bounds = null, calcStats = false)
    meta.setClassification(if (categorical)
      MrsPyramidMetadata.Classification.Categorical else
      MrsPyramidMetadata.Classification.Continuous)
    meta.setProtectionLevel(protectionLevel)
    // Store categories in metadata if needed
    if (categorical && !skipCategoryLoad && categoriesMatch) {
      setMetadataCategories(meta, firstCategories)
    }

    // repartition, because chances are the RDD only has 1 partition (ingest a single file)
    val numExecutors = math.max(context.getConf.getInt("spark.executor.instances", 0),
      math.max(tiles.partitions.length, meta.getTileBounds(zoom).getHeight.toInt))

    val repartitioned = if (numExecutors > 0) {
      logInfo("Repartitioning to " + numExecutors + " partitions")
      tiles.repartition(numExecutors)
    }
    else {
      //      logInfo("No need to repartition")
      tiles
    }

    (RasterRDD(repartitioned), meta)
  }

  def localingest(context: SparkContext, inputs:Array[String], zoom:Int, tilesize:Int,
                  categorical:Boolean, skipCategoryLoad: Boolean, nodata: Array[Number],
                  protectionLevel: String) = {
    var firstCategories: Map[Int, util.Vector[_]] = null
    var categoriesMatch: Boolean = false

    if (categorical && !skipCategoryLoad) {
      val checkResult = checkAndLoadCategories(context, inputs)
      categoriesMatch = checkResult._1
      firstCategories = checkResult._2
    }

    val tmpname = HadoopFileUtils.createUniqueTmpPath()
    val writer = SequenceFile.createWriter(context.hadoopConfiguration,
      SequenceFile.Writer.file(tmpname),
      SequenceFile.Writer.keyClass(classOf[TileIdWritable]),
      SequenceFile.Writer.valueClass(classOf[RasterWritable]),
      SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
    )

    inputs.foreach(input => {
      val tiles = IngestImage.makeTiles(input, zoom, tilesize, categorical, nodata)

      var cnt = 0
      tiles.foreach(kv => {
        writer.append(new TileIdWritable(kv._1.get()), kv._2)

        cnt += 1
        if (cnt % 1000 == 0) {
          writer.hflush()
        }
      })
    })

    writer.close()

    val input = inputs(0) // there is only 1 input here...

    val format = new SequenceFileInputFormat[TileIdWritable, RasterWritable]

    val job: Job = Job.getInstance(HadoopUtils.createConfiguration())


    val rawtiles = context.sequenceFile(tmpname.toString, classOf[TileIdWritable], classOf[RasterWritable])

    // this is stupid, but because the way hadoop input formats may reuse the key/value objects,
    // if we don't do this, all the data will eventually collapse into a single entry.
    val mapped = rawtiles.map(tile => {
      (new TileIdWritable(tile._1), RasterWritable.toWritable(RasterWritable.toRaster(tile._2)))
    })

    val mergedTiles = new PairRDDFunctions(mapped).reduceByKey((r1, r2) => {
      val src = RasterWritable.toRaster(r1)
      val dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(r2))

      RasterUtils.mosaicTile(src, dst, nodata)

      val mrgeoRaster = MrGeoRaster.fromRaster(dst)

      new RasterWritable(mrgeoRaster.data())
      RasterWritable.toWritable(dst)

    })

    val mrgeo = RasterRDD(mergedTiles.map(tile => {
      val src = RasterWritable.toRaster(tile._2)
      val mrgeoRaster = MrGeoRaster.fromRaster(src)

      (tile._1, new RasterWritable(mrgeoRaster.data()))
    }))

    val meta = SparkUtils.calculateMetadata(mrgeo, zoom, nodata, bounds = null, calcStats = false)
    meta.setClassification(if (categorical)
      MrsPyramidMetadata.Classification.Categorical else
      MrsPyramidMetadata.Classification.Continuous)
    meta.setProtectionLevel(protectionLevel)
    if (categorical && !skipCategoryLoad && categoriesMatch) {
      setMetadataCategories(meta, firstCategories)
    }

    (mrgeo, meta)
  }

  private def loadCategories(input: String): Map[Int, util.Vector[_]] = {
    var src: Dataset = null
      try {
      src = GDALUtils.open(input)

      var result: scala.collection.mutable.Map[Int, util.Vector[_]] = scala.collection.mutable.Map()
      if (src != null) {
        val bands = src.GetRasterCount()
        var b: Integer = 1
        while (b <= bands) {
          val band = src.GetRasterBand(b)
          val categoryNames = band.GetCategoryNames()
          val c: Int = 0
          result += ((b - 1) -> categoryNames)
          b += 1
        }
      }
      result.toMap
    }
    finally {
      if (src != null) {
        GDALUtils.close(src)
      }
    }
  }

  private def checkAndLoadCategories(context: SparkContext, inputs: Array[String]) =
  {
    var firstCategories: Map[Int, util.Vector[_]] = null
    var categoryMatchResult: (String, String) = null
    val inputsHead :: inputsTail = inputs.toList
    val firstInput = inputs(0)
    firstCategories = IngestImage.loadCategories(firstInput)
    // Initialize the value being aggregated with the name of the first input from
    // which the baseline categories were read. The second element will be assigned
    // during aggregation to whichever other input does not match those baseline
    // categories (or will be left blank if all the inputs' categories match.
    val zero = (firstInput, "")
    val inTail = context.parallelize(inputsTail, inputs.length)
    categoryMatchResult = inTail.aggregate(zero)((combinedValue, input) => {
      if (combinedValue._2.isEmpty) {
        // Compare the categories of the first input with those of the current
        // input being aggregated. If they are different, then return this input
        // as the one that differs
        val cats = IngestImage.loadCategories(input)
        if (!equalCategories(firstCategories, cats)) {
          (combinedValue._1, input)
        }
        else {
          combinedValue
        }
      }
      else {
        // Return the inputs with mismatched categories that we already found
        combinedValue
      }
    }, {
      // When merging just return a value that specifies a difference (i.e.
      // the second element of the tuple is not empty.
      (result1, result2) => {
        if (result1._2.length > 0) {
          result1
        }
        else if (result2._2.length > 0) {
          result2
        }
        else {
          result1
        }
      }
    })
    if (!categoryMatchResult._2.isEmpty) {
      throw new Exception("Categories from input " + categoryMatchResult._1 + " are not the same as categories from input " + categoryMatchResult._2)
    }
    (categoryMatchResult._2.isEmpty, firstCategories)
  }

  private def setMetadataCategories(meta: MrsPyramidMetadata,
                                    categoryMap: Map[Int, util.Vector[_]]): Unit = {
    categoryMap.foreach(catEntry => {
      val categories = new Array[String](catEntry._2.size())
      catEntry._2.zipWithIndex.foreach(cat => {
        categories(cat._2) = cat._1.toString
      })
      meta.setCategories(catEntry._1, categories)
    })
  }

  private def setupParams(input: String, output: String, categorical: Boolean, skipCategoryLoad: Boolean,
                          bounds: Bounds, zoomlevel: Int, tilesize: Int, nodata: Array[Number],
                          bands: Int, tiletype: Int, tags: util.Map[String, String],
                          protectionLevel: String,
                          providerProperties: ProviderProperties): mutable.Map[String, String] = {

    val args = mutable.Map[String, String]()

    args += Inputs -> input
    args += Output -> output
    if (bounds != null) {
      args += Bounds -> bounds.toCommaString
    }
    args += Zoom -> zoomlevel.toString
    args += Tilesize -> tilesize.toString
    if (nodata != null) {
      args += NoData -> nodata.mkString(" ")
    }
    args += Bands -> bands.toString
    args += Tiletype -> tiletype.toString
    args += Categorical -> categorical.toString
    args += SkipCategoryLoad -> skipCategoryLoad.toString

    var t: String = ""
    tags.foreach(kv => {
      if (t.length > 0) {
        t += ","
      }
      t += kv._1 + "=" + kv._2
    })

    args += Tags -> t
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(output,
      AccessMode.OVERWRITE, providerProperties)
    args += Protection -> ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel)

    //var p: String = ""
    if (providerProperties != null) {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
    }
    else
    {
      args += ProviderProperties -> ""
    }

    args
  }

  def localIngest(inputs: Array[String], output: String,
      categorical: Boolean, skipCategoryLoad: Boolean, config: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Array[Number], bands: Int, tiletype: Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: ProviderProperties): Boolean = {

    var conf: Configuration = config
    if (conf == null) {
      conf = HadoopUtils.createConfiguration
    }

    val args = setupParams(inputs.mkString(","), output, categorical, skipCategoryLoad, bounds, zoomlevel, tilesize, nodata, bands, tiletype,
      tags, protectionLevel,
      providerProperties)

    val name = "IngestImageLocal"
    run(name, classOf[IngestLocal].getName, args.toMap, conf)
    true
  }

  private def makeTiles(image: String, zoom: Int, tilesize: Int,
      categorical: Boolean, nodata: Array[Number]): TraversableOnce[(TileIdWritable, RasterWritable)] = {

    val result = ListBuffer[(TileIdWritable, RasterWritable)]()

    //val start = System.currentTimeMillis()

    // open the image
    try {
      val src = GDALUtils.open(image)

      if (src != null) {
        val datatype = src.GetRasterBand(1).getDataType
        val datasize = gdal.GetDataTypeSize(datatype) / 8

        val bands = src.GetRasterCount()

        // force the nodata values...
        for (i <- 1 to bands) {
          val band = src.GetRasterBand(i)
          band.SetNoDataValue(nodata(i - 1).doubleValue())
        }

        val imageBounds = GDALUtils.getBounds(src)
        val tiles = TMSUtils.boundsToTile(imageBounds, zoom, tilesize)
        val tileBounds = TMSUtils.tileBounds(imageBounds, zoom, tilesize)

        val w = tiles.width() * tilesize
        val h = tiles.height() * tilesize

        val res = TMSUtils.resolution(zoom, tilesize)

        //val scaledsize = w * h * bands * datasize

        if (log.isDebugEnabled) {
          logDebug("Image info:  " + image)
          logDebug("  bands:  " + bands)
          logDebug("  data type:  " + datatype)
          logDebug("  width:  " + src.getRasterXSize)
          logDebug("  height:  " + src.getRasterYSize)
          logDebug("  bounds:  " + imageBounds)
          logDebug("  tiles:  " + tiles)
          logDebug("  tile width:  " + w)
          logDebug("  tile height:  " + h)
        }

        // TODO:  Figure out chunking...
        val scaled = GDALUtils.createEmptyMemoryRaster(src, w.toInt, h.toInt)

        val xform = Array.ofDim[Double](6)

        xform(0) = tileBounds.w /* top left x */
        xform(1) = res /* w-e pixel resolution */
        xform(2) = 0 /* 0 */
        xform(3) = tileBounds.n /* top left y */
        xform(4) = 0 /* 0 */
        xform(5) = -res /* n-s pixel resolution (negative value) */

        scaled.SetGeoTransform(xform)
        scaled.SetProjection(GDALUtils.EPSG4326)


        val resample =
          if (categorical) {
            // use gdalconstConstants.GRA_Mode for categorical, which may not exist in earlier versions of gdal,
            // in which case we will use GRA_NearestNeighbour
            try {
              val mode = classOf[gdalconstConstants].getDeclaredField("GRA_Mode")
              mode.getInt()
            }
            catch {
              case _ : RuntimeException | _: Exception => gdalconstConstants.GRA_NearestNeighbour
            }
          }
          else {
            gdalconstConstants.GRA_Bilinear
          }

        gdal.ReprojectImage(src, scaled, src.GetProjection(), GDALUtils.EPSG4326, resample)

        //    val time = System.currentTimeMillis() - start
        //    println("scale: " + time)

        //    val band = scaled.GetRasterBand(1)
        //    val minmax = Array.ofDim[Double](2)
        //    band.ComputeRasterMinMax(minmax, 0)

        //GDALUtils.saveRaster(scaled, "/data/export/scaled.tif")

        // close the image
        GDALUtils.close(src)

        val bandlist = Array.ofDim[Int](bands)
        var x: Int = 0
        while (x < bands) {
          bandlist(x) = x + 1 // bands are ones based
          x += 1
        }


        val buffer = Array.ofDim[Byte](datasize * tilesize * tilesize * bands)

        var dty: Int = 0
        while (dty < tiles.height.toInt) {
          var dtx: Int = 0
          while (dtx < tiles.width.toInt) {

            //val start = System.currentTimeMillis()

            val tx: Long = dtx + tiles.w
            val ty: Long = tiles.n - dty

            val x: Int = dtx * tilesize
            val y: Int = dty * tilesize

            val success = scaled.ReadRaster(x, y, tilesize, tilesize, tilesize, tilesize, datatype, buffer, null)

            if (success != gdalconstConstants.CE_None) {
              println("Failed reading raster" + success)
            }

            // switch the byte order...
            GDALUtils.swapBytes(buffer, datatype)

            val writable = RasterWritable.toWritable(buffer, tilesize, tilesize,
              bands, GDALUtils.toRasterDataBufferType(datatype))

            // save the tile...
            //        GDALUtils.saveRaster(RasterWritable.toRaster(writable),
            //          "/data/export/tiles/tile-" + ty + "-" + tx, tx, ty, zoom, tilesize, GDALUtils.getnodata(scaled))

            result.append((new TileIdWritable(TMSUtils.tileid(tx, ty, zoom)), writable))


            //val time = System.currentTimeMillis() - start
            //println(tx + ", " + ty + ", " + time)
            dtx += 1
          }
          dty += 1
        }

        GDALUtils.close(scaled)
      }
      else {
        if (log.isDebugEnabled) {
          logDebug("Could not open " + image)
        }
      }
    }
    catch {
      case ioe: IOException =>
        ioe.printStackTrace()  // no op, this can happen in "skip preprocessing" mode
    }

    if (log.isDebugEnabled) {
      logDebug("Ingested " + result.length + " tiles from " + image)
    }
    result.iterator
  }



  @throws(classOf[Exception])
  def quickIngest(input: InputStream, output: String, categorical: Boolean, config: Configuration,
      overridenodata: Boolean, protectionLevel: String, nodata: Number): Boolean = {
    //    var conf: Configuration = config
    //    if (conf == null) {
    //      conf = HadoopUtils.createConfiguration
    //    }
    //    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.OVERWRITE, conf)
    //    val useProtectionLevel: String = ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel)
    //    val metadata: MrsImagePyramidMetadata = GeotoolsRasterUtils
    //        .calculateMetaData(input, output, false, useProtectionLevel, categorical)
    //    if (overridenodata) {
    //      val defaults: Array[Double] = metadata.getDefaultValues
    //      for (i <- defaults.indices) {
    //        defaults(i) = nodata.doubleValue
    //      }
    //      metadata.setDefaultValues(defaults)
    //    }
    //
    //    val writer: MrsImageWriter = dp.getMrsTileWriter(metadata.getMaxZoomLevel)
    //    val reader: AbstractGridCoverage2DReader = GeotoolsRasterUtils.openImageFromStream(input)
    //    log.info("  reading: " + input.toString)
    //    if (reader != null) {
    //      val geotoolsImage: GridCoverage2D = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326")
    //      val tilebounds: LongRectangle = GeotoolsRasterUtils
    //          .calculateTiles(reader, metadata.getTilesize, metadata.getMaxZoomLevel)
    //      val zoomlevel: Int = metadata.getMaxZoomLevel
    //      val tilesize: Int = metadata.getTilesize
    //      val defaults: Array[Double] = metadata.getDefaultValues
    //      log.info("    zoomlevel: " + zoomlevel)
    //      val extender: BorderExtender = new BorderExtenderConstant(defaults)
    //      val image: PlanarImage = GeotoolsRasterUtils.prepareForCutting(geotoolsImage, zoomlevel, tilesize,
    //        if (categorical) {
    //          Classification.Categorical
    //        }
    //        else {
    //          Classification.Continuous
    //        })
    //
    //      for (ty <- tilebounds.getMinY to tilebounds.getMaxY) {
    //        for (tx <- tilebounds.getMinX to tilebounds.getMaxX) {
    //
    //          val raster =
    //            ImageUtils.cutTile(image, tx, ty, tilebounds.getMinX, tilebounds.getMaxY, tilesize, extender)
    //
    //          writer.append(new TileIdWritable(TMSUtils.tileid(tx, ty, zoomlevel)), raster)
    //        }
    //      }
    //
    //      writer.close()
    //      dp.getMetadataWriter.write(metadata)
    //    }
    true
  }

  @throws(classOf[Exception])
  def quickIngest(input: String, output: String, categorical: Boolean, config: Configuration, overridenodata: Boolean,
      nodata: Number, tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: ProviderProperties): Boolean = {
    //    val provider: MrsImageDataProvider = DataProviderFactory
    //        .getMrsImageDataProvider(output, AccessMode.OVERWRITE, providerProperties)
    //    var conf: Configuration = config
    //    if (conf == null) {
    //      conf = HadoopUtils.createConfiguration
    //    }
    //    val useProtectionLevel: String = ProtectionLevelUtils.getAndValidateProtectionLevel(provider, protectionLevel)
    //    val metadata: MrsImagePyramidMetadata = GeotoolsRasterUtils
    //        .calculateMetaData(Array[String](input), output, false, useProtectionLevel, categorical, overridenodata)
    //    if (tags != null) {
    //      metadata.setTags(tags)
    //    }
    //    if (overridenodata) {
    //      val defaults: Array[Double] = metadata.getDefaultValues
    //      for (i <- defaults.indices) {
    //        defaults(i) = nodata.doubleValue
    //      }
    //      metadata.setDefaultValues(defaults)
    //    }
    //
    //    val writer: MrsImageWriter = provider.getMrsTileWriter(metadata.getMaxZoomLevel)
    //    val reader: AbstractGridCoverage2DReader = GeotoolsRasterUtils.openImage(input)
    //    log.info("  reading: " + input)
    //    if (reader != null) {
    //      val geotoolsImage: GridCoverage2D = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326")
    //      val tilebounds: LongRectangle = GeotoolsRasterUtils
    //          .calculateTiles(reader, metadata.getTilesize, metadata.getMaxZoomLevel)
    //      val zoomlevel: Int = metadata.getMaxZoomLevel
    //      val tilesize: Int = metadata.getTilesize
    //      val defaults: Array[Double] = metadata.getDefaultValues
    //      log.info("    zoomlevel: " + zoomlevel)
    //      val extender: BorderExtender = new BorderExtenderConstant(defaults)
    //      val image: PlanarImage = GeotoolsRasterUtils.prepareForCutting(geotoolsImage, zoomlevel, tilesize,
    //        if (categorical) {
    //          Classification.Categorical
    //        }
    //        else {
    //          Classification.Continuous
    //        })
    //      for (ty <- tilebounds.getMinY to tilebounds.getMaxY) {
    //        for (tx <- tilebounds.getMinX to tilebounds.getMaxX) {
    //
    //          val raster =
    //            ImageUtils.cutTile(image, tx, ty, tilebounds.getMinX, tilebounds.getMaxY, tilesize, extender)
    //
    //          writer.append(new TileIdWritable(TMSUtils.tileid(tx, ty, zoomlevel)), raster)
    //        }
    //      }
    //      writer.close()
    //      provider.getMetadataWriter.write(metadata)
    //    }
    true
    }


  override def readExternal(in: ObjectInput) {}
  override def writeExternal(out: ObjectOutput) {}

  override def setup(job: JobArguments): Boolean = {
    job.isMemoryIntensive = true

    true
  }
}

class IngestImage extends MrGeoJob with Externalizable {
  private[ingest] var inputs: Array[String] = null
  private[ingest] var output:String = null
  private[ingest] var bounds:Bounds = null
  private[ingest] var zoom:Int = -1
  private[ingest] var bands:Int = -1
  private[ingest] var tiletype:Int = -1
  private[ingest] var tilesize:Int = -1
  private[ingest] var nodata:Array[Number] = null
  private[ingest] var categorical:Boolean = false
  private[ingest] var skipCategoryLoad:Boolean = false
  private[ingest] var providerproperties:ProviderProperties = null
  private[ingest] var protectionlevel:String = null


  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes += classOf[Array[String]]

    classes.result()
  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = {

    job.isMemoryIntensive = true

    inputs = job.getSetting(IngestImage.Inputs).split(",")
    // This setting can use lots of memory, so we we'll set it to null here to clean up memory.
    // WARNING!  This definately can have side-effects
    job.setSetting(IngestImage.Inputs, null)
    output = job.getSetting(IngestImage.Output)

    val boundstr = job.getSetting(IngestImage.Bounds, null)
    if (boundstr != null) {
      bounds = Bounds.fromCommaString(boundstr)
    }

    zoom = job.getSetting(IngestImage.Zoom).toInt
    bands = job.getSetting(IngestImage.Bands).toInt
    tiletype = job.getSetting(IngestImage.Tiletype).toInt
    tilesize = job.getSetting(IngestImage.Tilesize).toInt
    if (job.hasSetting(IngestImage.NoData)) {
      nodata = job.getSetting(IngestImage.NoData).split(" ").map(_.toDouble.asInstanceOf[Number])
    }
    else {
      nodata = Array.fill[Number](bands)(Double.NaN)
    }
    categorical = job.getSetting(IngestImage.Categorical).toBoolean
    skipCategoryLoad = job.getSetting(IngestImage.SkipCategoryLoad).toBoolean

    protectionlevel = job.getSetting(IngestImage.Protection)
    if (protectionlevel == null)
    {
      protectionlevel = ""
    }

    providerproperties = ProviderProperties.fromDelimitedString(job.getSetting(IngestImage.ProviderProperties))

    true
  }


  override def execute(context: SparkContext): Boolean = {

    val ingested = IngestImage.ingest(context, inputs, zoom, tilesize,
      categorical, skipCategoryLoad, nodata, protectionlevel)

    val dp = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.OVERWRITE, providerproperties)
    SparkUtils.saveMrsPyramid(ingested._1, dp, ingested._2, zoom, context.hadoopConfiguration, providerproperties)

    true
  }


  override def teardown(job: JobArguments, conf:SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput) {
  }

  override def writeExternal(out: ObjectOutput) {
  }
}
