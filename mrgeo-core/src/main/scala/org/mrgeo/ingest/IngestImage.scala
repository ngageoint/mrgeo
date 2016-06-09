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
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{DataProviderFactory, ProtectionLevelUtils, ProviderProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils._
import org.mrgeo.utils.tms.{Bounds, TMSUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object IngestImage extends MrGeoDriver with Externalizable {

  final private val Inputs = "inputs"
  final private val Output = "output"
  final private val Bounds = "bounds"
  final private val Zoom = "zoom"
  final private val Tilesize = "tilesize"
  final private val NoData = "nodata"
  final private val Tiletype = "tiletype"
  final private val Bands = "bands"
  final private val Categorical = "categorical"
  final private val Tags = "tags"
  final private val Protection = "protection"
  final private val ProviderProperties = "provider.properties"

  def ingest(inputs: Array[String], output: String,
      categorical: Boolean, conf: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Array[Number], bands: Int, tiletype: Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: ProviderProperties): Boolean = {

    val name = "IngestImage"

    val args = setupParams(inputs.mkString(","), output, categorical, bounds, zoomlevel, tilesize, nodata, bands,
      tiletype, tags, protectionLevel,
      providerProperties)

    run(name, classOf[IngestImage].getName, args.toMap, conf)

    true
  }

  def ingest(context: SparkContext, inputs:Array[String], zoom:Int, tilesize:Int, categorical:Boolean, nodata: Array[Number]) = {
    // force 1 partition per file, this will keep the size of each ingest task as small as possible, so we
    // won't eat up too much memory
    val in = context.parallelize(inputs, inputs.length)

    val rawtiles = new PairRDDFunctions(in.flatMap(input => {
      IngestImage.makeTiles(input, zoom, tilesize, categorical, nodata)
    }))

    val tiles = rawtiles.reduceByKey((r1, r2) => {
      val src = RasterWritable.toRaster(r1)
      val dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(r2))

      RasterUtils.mosaicTile(src, dst, nodata)
      RasterWritable.toWritable(dst)
    })

    val meta = SparkUtils.calculateMetadata(RasterRDD(tiles), zoom, nodata, bounds = null, calcStats = false)


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


  private def setupParams(input: String, output: String, categorical: Boolean, bounds: Bounds, zoomlevel: Int,
      tilesize: Int, nodata: Array[Number],
      bands: Int, tiletype: Int, tags: util.Map[String, String], protectionLevel: String,
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
      categorical: Boolean, config: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Array[Number], bands: Int, tiletype: Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: ProviderProperties): Boolean = {

    //    val provider: ImageIngestDataProvider = DataProviderFactory
    //        .getImageIngestDataProvider(HadoopFileUtils.createUniqueTmpPath().toUri.toString, AccessMode.OVERWRITE)


    var conf: Configuration = config
    if (conf == null) {
      conf = HadoopUtils.createConfiguration
    }

    val tmpname = HadoopFileUtils.createUniqueTmpPath()
    val writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.file(tmpname),
      SequenceFile.Writer.keyClass(classOf[TileIdWritable]),
      SequenceFile.Writer.valueClass(classOf[RasterWritable]),
      SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
    )

    inputs.foreach(input => {
      val tiles = IngestImage.makeTiles(input, zoomlevel, tilesize, categorical, nodata)

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

    val args = setupParams(tmpname.toUri.toString, output, categorical, bounds, zoomlevel, tilesize, nodata, bands, tiletype,
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
              if (mode != null) {
                mode.getInt()
              }
              else {
                gdalconstConstants.GRA_NearestNeighbour
              }
            }
            catch {
              case e: Exception => gdalconstConstants.GRA_NearestNeighbour
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
  var inputs: Array[String] = null
  var output:String = null
  var bounds:Bounds = null
  var zoom:Int = -1
  var bands:Int = -1
  var tiletype:Int = -1
  var tilesize:Int = -1
  var nodata:Array[Number] = null
  var categorical:Boolean = false
  var providerproperties:ProviderProperties = null
  var protectionlevel:String = null


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

    protectionlevel = job.getSetting(IngestImage.Protection)
    if (protectionlevel == null)
    {
      protectionlevel = ""
    }

    providerproperties = ProviderProperties.fromDelimitedString(job.getSetting(IngestImage.ProviderProperties))

    true
  }


  override def execute(context: SparkContext): Boolean = {

    val ingested = IngestImage.ingest(context, inputs, zoom, tilesize, categorical, nodata)

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
