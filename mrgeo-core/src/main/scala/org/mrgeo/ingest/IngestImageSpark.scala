package org.mrgeo.ingest

import java.awt.image.{DataBuffer, Raster, WritableRaster}
import java.io._
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{OutputFormat, RecordWriter, Job}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.{MrsImagePyramidWriterContext, MrsImageDataProvider}
import org.mrgeo.data.ingest.{ImageIngestDataProvider, ImageIngestWriterContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.{TiledOutputFormatContext, MrsTileWriter, TileIdWritable}
import org.mrgeo.hdfs.partitioners.{ImageSplitGenerator, TileIdPartitioner}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.MrsImagePyramid
import org.mrgeo.spark.SparkTileIdPartitioner
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object IngestImageSpark extends MrGeoDriver with Externalizable {

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
      zoomlevel: Int, tilesize: Int, nodata: Number, bands: Int, tiletype:Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: Properties):Boolean = {

    val name = "IngestImage"

    val args = setupParams(inputs.mkString(","), output, categorical, bounds, zoomlevel, tilesize, nodata, bands, tiletype, tags, protectionLevel,
      providerProperties)

    run(name, classOf[IngestImageSpark].getName, args.toMap, conf)

    true
  }

  private def setupParams(input: String, output: String, categorical: Boolean, bounds: Bounds, zoomlevel: Int, tilesize: Int, nodata: Number,
      bands: Int, tiletype: Int, tags: util.Map[String, String], protectionLevel: String,
      providerProperties: Properties): mutable.Map[String, String] = {

    val args = mutable.Map[String, String]()

    args += Inputs -> input
    args += Output -> output
    if (bounds != null) {
      args += Bounds -> bounds.toDelimitedString
    }
    args += Zoom -> zoomlevel.toString
    args += Tilesize -> tilesize.toString
    args += NoData -> nodata.toString
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
    args += Protection -> protectionLevel

    var p: String = ""
    providerProperties.foreach(kv => {
      if (p.length > 0) {
        p += "||"
      }
      p += kv._1 + "=" + kv._2
    })
    args += ProviderProperties -> p

    args
  }

  def localIngest(inputs: Array[String], output: String,
      categorical: Boolean, config: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Number, bands: Int, tiletype:Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: Properties):Boolean = {

    val provider: ImageIngestDataProvider = DataProviderFactory
        .getImageIngestDataProvider(HadoopFileUtils.createUniqueTmpPath().toUri.toString, AccessMode.OVERWRITE)

    var conf: Configuration = config
    if (conf == null) {
      conf = HadoopUtils.createConfiguration
    }

    //    final Path unique = HadoopFileUtils.createUniqueTmpPath();
    val context = new ImageIngestWriterContext()
    context.setZoomlevel(zoomlevel)
    context.setPartNum(0)

    val writer = provider.getMrsTileWriter(context)

    inputs.foreach(input => {
      val tiles =  IngestImageSpark.makeTiles(input, zoomlevel, tilesize, categorical)

      tiles.foreach(kv => {
        writer.append(new TileIdWritable(kv._1.get()), RasterWritable.toRaster(kv._2))
      })
    })

    writer.close()

    val args = setupParams(writer.getName, output, categorical, bounds, zoomlevel, tilesize, nodata, bands, tiletype, tags, protectionLevel,
      providerProperties)

    val name = "IngestImageLocal"

    run(name, classOf[IngestLocalSpark].getName, args.toMap, conf)

    provider.delete()

    true
  }

  private def makeTiles(image: String, zoom:Int, tilesize:Int, categorical:Boolean): TraversableOnce[(TileIdWritable, RasterWritable)] = {

    val result = ListBuffer[(TileIdWritable, RasterWritable)]()

    //val start = System.currentTimeMillis()

    // open the image
    try {
      val src = GDALUtils.open(image)

      if (src != null) {
        val datatype = src.GetRasterBand(1).getDataType
        val datasize = gdal.GetDataTypeSize(datatype) / 8

        val bands = src.GetRasterCount()

        val imageBounds = GDALUtils.getBounds(src).getTMSBounds
        val tiles = TMSUtils.boundsToTile(imageBounds, zoom, tilesize)
        val tileBounds = TMSUtils.tileBounds(imageBounds, zoom, tilesize)

        val w = tiles.width() * tilesize
        val h = tiles.height() * tilesize

        val res = TMSUtils.resolution(zoom, tilesize)


        val scaledsize = w * h * bands * datasize

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


        var resample: Int = gdalconstConstants.GRA_Bilinear
        if (categorical) {
          // use gdalconstConstants.GRA_Mode for categorical, which may not exist in earlier versions of gdal,
          // in which case we will use GRA_NearestNeighbour
          try {
            val mode = classOf[gdalconstConstants].getDeclaredField("GRA_Mode")
            if (mode != null) {
              resample = mode.getInt()
            }
          }
          catch {
            case e: Exception => resample = gdalconstConstants.GRA_NearestNeighbour
          }
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
        for (x <- 0 until bands) {
          bandlist(x) = x + 1 // bands are ones based
        }


        val buffer = Array.ofDim[Byte](datasize * tilesize * tilesize * bands)

        for (dty <- 0 until tiles.height.toInt) {
          for (dtx <- 0 until tiles.width().toInt) {

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
          }
        }

        GDALUtils.close(scaled)
      }
    }
    catch {
      case ioe: IOException => // no op, this can happen in "skip preprocessing" mode
    }
    result.iterator
  }



  override def readExternal(in: ObjectInput) {}
  override def writeExternal(out: ObjectOutput) {}

  override def setup(job: JobArguments): Boolean = {
    job.isMemoryIntensive = true

    true
  }
}

class IngestImageSpark extends MrGeoJob with Externalizable {
  var inputs: Array[String] = null
  var output:String = null
  var bounds:Bounds = null
  var zoom:Int = -1
  var bands:Int = -1
  var tiletype:Int = -1
  var tilesize:Int = -1
  var nodata:Number = Double.NaN
  var categorical:Boolean = false
  var providerproperties:Properties = null
  var protectionlevel:String = null


  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes.result()
  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = {

    job.isMemoryIntensive = true

    conf.set("spark.storage.memoryFraction", "0.25") // set the storage amount lower...
    conf.set("spark.shuffle.memoryFraction", "0.50") // set the shuffle higher

    inputs = job.getSetting(IngestImageSpark.Inputs).split(",")
    // This setting can use lots of memory, so we we'll set it to null here to clean up memory.
    // WARNING!  This definately can have side-effects
    job.setSetting(IngestImageSpark.Inputs, null)
    output = job.getSetting(IngestImageSpark.Output)

    val boundstr = job.getSetting(IngestImageSpark.Bounds, null)
    if (boundstr == null) {
      bounds = new Bounds()
    }
    else {
      bounds = Bounds.fromDelimitedString(boundstr)
    }

    zoom = job.getSetting(IngestImageSpark.Zoom).toInt
    bands = job.getSetting(IngestImageSpark.Bands).toInt
    tiletype = job.getSetting(IngestImageSpark.Tiletype).toInt
    tilesize = job.getSetting(IngestImageSpark.Tilesize).toInt
    nodata = job.getSetting(IngestImageSpark.NoData).toDouble
    categorical = job.getSetting(IngestImageSpark.Categorical).toBoolean

    protectionlevel = job.getSetting(IngestImageSpark.Protection)
    if (protectionlevel == null)
    {
      protectionlevel = ""
    }

    val props = job.getSetting(IngestImageSpark.ProviderProperties).split("||")
    providerproperties = new Properties()
    props.foreach (prop => {
      if (prop.contains("=")) {
        val kv = prop.split("=")
        providerproperties.put(kv(0), kv(1))
      }
    })

    true
  }


  private def copyPixel(x: Int, y: Int, b: Int, src: Raster, dst: WritableRaster): Unit = {
    src.getTransferType match {
    case DataBuffer.TYPE_BYTE =>
      val p: Byte = src.getSample(x, y, b).toByte
      if (p != nodata.byteValue()) {
        dst.setSample(x, y, b, p)
      }
    case DataBuffer.TYPE_FLOAT =>
      val p: Float = src.getSampleFloat(x, y, b)
      if (!p.isNaN && p != nodata.floatValue()) {
        dst.setSample(x, y, b, p)
      }
    case DataBuffer.TYPE_DOUBLE =>
      val p: Double = src.getSampleDouble(x, y, b)
      if (!p.isNaN && p != nodata.doubleValue()) {
        dst.setSample(x, y, b, p)
      }
    case DataBuffer.TYPE_INT =>
      val p: Int = src.getSample(x, y, b)
      if (p != nodata.intValue()) {
        dst.setSample(x, y, b, p)
      }
    case DataBuffer.TYPE_SHORT =>
      val p: Short = src.getSample(x, y, b).toShort
      if (p != nodata.shortValue()) {
        dst.setSample(x, y, b, p)
      }
    case DataBuffer.TYPE_USHORT =>
      val p: Int = src.getSample(x, y, b)
      if (p != nodata.intValue()) {
        dst.setSample(x, y, b, p)
      }
    }
  }

  protected def mergeTile(r1: RasterWritable, r2: RasterWritable):RasterWritable = {
    val src = RasterWritable.toRaster(r1)
    val dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(r2))

    for (y <- 0 until src.getHeight) {
      for (x <- 0 until src.getWidth) {
        for (b <- 0 until src.getNumBands) {
          copyPixel(x, y, b, src, dst)
        }
      }
    }

    RasterWritable.toWritable(dst)
  }

  override def execute(context: SparkContext): Boolean = {

    context.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // force 1 partition per file, this will keep the size of each ingest task as small as possible, so we
    // won't eat up too much memory
    val in = context.makeRDD(inputs, inputs.length)

    // This variable can get large, so we'll clear it out here to free up some memory
    inputs = null

    val rawtiles = new PairRDDFunctions(in.flatMap(input => {
      IngestImageSpark.makeTiles(input, zoom, tilesize, categorical)
    }))

    val mergedTiles=rawtiles.reduceByKey((r1, r2) => {
      mergeTile(r1, r2)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    saveRDD(mergedTiles, context.hadoopConfiguration)

    mergedTiles.unpersist()
    true
  }


  protected def saveRDD(tiles: RDD[(TileIdWritable, RasterWritable)], conf:Configuration): Unit = {
    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }
    //val job: Job = new Job(conf)

    val tileIncrement = 1

    //job.getConfiguration.setInt(TileIdPartitioner.INCREMENT_KEY, tileIncrement)
    conf.setInt(TileIdPartitioner.INCREMENT_KEY, tileIncrement)

    if (!bounds.isValid) {
      bounds = SparkUtils.calculateBounds(tiles, zoom, tilesize)
    }

    if (bands <= 0  || tiletype <= 0) {
      val tile = RasterWritable.toRaster(tiles.first()._2)

      bands = tile.getNumBands
      tiletype = tile.getTransferType
    }

    val nodatas = Array.ofDim[Double](bands)
    for (x <- 0 until nodatas.length) {
      nodatas(x) = nodata.doubleValue()
    }

    // calculate stats
    val stats = SparkUtils.calculateStats(tiles, bands, nodatas)


    // save the new pyramid
//    val dp = MrsImageDataProvider.setupMrsPyramidOutputFormat(job, output, bounds, zoom,
//      tilesize, tiletype, bands, protectionlevel, providerproperties)

    val tileBounds = TMSUtils.boundsToTile(bounds.getTMSBounds, zoom, tilesize)

    //val splitGenerator = new ImageSplitGenerator(tileBounds.w, tileBounds.s,
    //  tileBounds.e, tileBounds.n, zoom, tileIncrement)

    val splitGenerator = new ImageSplitGenerator(tileBounds.w, tileBounds.s,
      tileBounds.e, tileBounds.n, zoom, tileIncrement)

    val sparkPartitioner = new SparkTileIdPartitioner(splitGenerator)

    //logInfo("tiles has " + tiles.count() + " tiles in " + tiles.partitions.length + " partitions")

    val partitioned = tiles.partitionBy(sparkPartitioner)

    //logInfo("partitioned has " + partitioned.count() + " tiles in " + partitioned.partitions.length + " partitions")
    // free up the tile's cache, it's not needed any more...

    val sorted = partitioned.sortByKey()
    //logInfo("sorted has " + sorted.count() + " tiles in " + sorted.partitions.length + " partitions")

    // this is missing in early spark APIs
    //val sorted = tiles.repartitionAndSortWithinPartitions(sparkPartitioner)

    // save the image
    //sorted.saveAsNewAPIHadoopDataset(conf) // job.getConfiguration)

    val idp = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.OVERWRITE,
      null.asInstanceOf[Properties])

//    path: String,
//    keyClass: Class[_],
//    valueClass: Class[_],
//    outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
//    conf: Configuration = self.context.hadoopConfiguration)

    val tofc = new TiledOutputFormatContext(output, bounds, zoom, tilesize)
    val tofp = idp.getTiledOutputFormatProvider(tofc)

    val writer = idp.getMrsTileWriter(zoom)
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
    MrsImagePyramid.calculateMetadata(output, zoom, idp.getMetadataWriter, stats,
      nodatas, bounds, conf /* job.getConfiguration */, protectionlevel, providerproperties)
  }

  override def teardown(job: JobArguments, conf:SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput) {
    //    val count = in.readInt()
    //    val ab = mutable.ArrayBuilder.make[String]()
    //    for (i <- 0 until count) {
    //      ab += in.readUTF()
    //    }
    //    inputs = ab.result()
    //
    output = in.readUTF()
    //    bounds = Bounds.fromDelimitedString(in.readUTF())
    zoom = in.readInt()
    tilesize = in.readInt()
    //    tiletype = in.readInt()
    //    bands = in.readInt()
    //    nodata = in.readDouble()
    categorical = in.readBoolean()

    //    providerproperties = in.readObject().asInstanceOf[Properties]
    //    protectionlevel = in.readUTF()
  }

  override def writeExternal(out: ObjectOutput) {
    //      out.writeInt(inputs.length)
    //      inputs.foreach(input => { out.writeUTF(input)})
    out.writeUTF(output)
    //      out.writeUTF(bounds.toDelimitedString)
    out.writeInt(zoom)
    out.writeInt(tilesize)
    //      out.writeInt(tiletype)
    //      out.writeInt(bands)
    //      out.writeDouble(nodata.doubleValue())
    out.writeBoolean(categorical)
    //
    //      out.writeObject(providerproperties)
    //
    //      out.writeUTF(protectionlevel)
  }
}
