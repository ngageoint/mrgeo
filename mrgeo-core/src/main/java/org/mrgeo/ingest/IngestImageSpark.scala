package org.mrgeo.ingest

import java.awt.image.{DataBuffer, Raster, WritableRaster}
import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.partitioners.{TileIdPartitioner, ImageSplitGenerator}
import org.mrgeo.image.MrsImagePyramid
import org.mrgeo.spark.SparkTileIdPartitioner
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils.{Bounds, GDALUtils, HadoopUtils, TMSUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object IngestImageSpark extends MrGeoDriver with Externalizable {

  def ingest(inputs: Array[String], output: String,
      categorical: Boolean, conf: Configuration, bounds: Bounds,
      zoomlevel: Int, tilesize: Int, nodata: Number, bands: Int, tiletype:Int,
      tags: java.util.Map[String, String], protectionLevel: String,
      providerProperties: Properties):Boolean = {

    val args =  mutable.Map[String, String]()

    val in = inputs.mkString(",")

    val name = "IngestImage"

    args += "inputs" -> in
    args += "output" -> output
    args += "bounds" -> bounds.toDelimitedString
    args += "zoom" -> zoomlevel.toString
    args += "tilesize" -> tilesize.toString
    args += "nodata" -> nodata.toString
    args += "bands" -> bands.toString
    args += "tiletype" -> tiletype.toString
    args += "categorical" -> categorical.toString

    var t:String = ""
    tags.foreach(kv => {
      if (t.length > 0) {
        t += ","
      }
      t +=  kv._1 + "=" + kv._2
    })

    args += "tags" -> t
    args += "protection" -> protectionLevel

    var p:String = ""
    providerProperties.foreach(kv => {
      if (p.length > 0) {
        p += "||"
      }
      p +=  kv._1 + "=" + kv._2
    })
    args += "providerproperties" -> p


    run(name, classOf[IngestImageSpark].getName, args.toMap, conf)

    true
  }

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}
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
  var providerproterties:Properties = null
  var protectionlevel:String = null



  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes += classOf[TileIdWritable]
    classes += classOf[RasterWritable]

    classes.result()

  }

  override def setup(job: JobArguments): Boolean = {

    inputs = job.getSetting("inputs").split(",")
    output = job.getSetting("output")

    bounds = Bounds.fromDelimitedString(job.getSetting("bounds"))
    zoom = job.getSetting("zoom").toInt
    bands = job.getSetting("bands").toInt
    tiletype = job.getSetting("tiletype").toInt
    tilesize = job.getSetting("tilesize").toInt
    nodata = job.getSetting("nodata").toDouble
    categorical = job.getSetting("categorical").toBoolean

    protectionlevel = job.getSetting("protection")
    if (protectionlevel == null)
    {
      protectionlevel = ""
    }

    val props = job.getSetting("providerproperties").split("||")
    providerproterties = new Properties()
    props.foreach (prop => {
      if (prop.contains("=")) {
        val kv = prop.split("=")
        providerproterties.put(kv(0), kv(1))
      }
    })

    true
  }

  private def makeTiles(image: String): TraversableOnce[(TileIdWritable, RasterWritable)] = {
    val result = ListBuffer[(TileIdWritable, RasterWritable)]()

    // open the image
    val src = GDALUtils.open(image)

    val imageBounds = GDALUtils.getBounds(src).getTMSBounds
    val tiles = TMSUtils.boundsToTile(imageBounds, zoom, tilesize)
    val tileBounds = TMSUtils.tileBounds(imageBounds, zoom, tilesize)

    val w = tiles.width() * tilesize
    val h = tiles.height() * tilesize

    val res = TMSUtils.resolution(zoom, tilesize)

    val scaled = GDALUtils.createEmptyMemoryRaster(src, w.toInt, h.toInt)
    val xform = Array.ofDim[Double](6)

    xform(0) = tileBounds.w /* top left x */
    xform(1) = res /* w-e pixel resolution */
    xform(2) = 0 /* 0 */
    xform(3) = tileBounds.n /* top left y */
    xform(4) = 0 /* 0 */
    xform(5) = -res /* n-s pixel resolution (negative value) */

    scaled.SetGeoTransform(xform)
    scaled.SetProjection(GDALUtils.epsg4326)

    val sxform = src.GetGeoTransform()

    var mode:Int = gdalconstConstants.GRA_Bilinear
    // use gdalconstConstants.GRA_Mode for categorical...
    if (categorical) {
      mode = gdalconstConstants.GRA_Mode
    }
    gdal.ReprojectImage(src, scaled, src.GetProjection(), GDALUtils.epsg4326, mode)


    //    val band = scaled.GetRasterBand(1)
    //    val minmax = Array.ofDim[Double](2)
    //    band.ComputeRasterMinMax(minmax, 0)

    //    GDALUtils.saveRaster(scaled, "/data/export/scaled.tif")

    // close the image
    GDALUtils.close(image)

    val bands = scaled.GetRasterCount()
    val bandlist = Array.ofDim[Int](bands)
    for (x <- 0 until bands)
    {
      bandlist(x) = x + 1  // bands are ones based
    }

    val datatype = scaled.GetRasterBand(1).getDataType
    val datasize = gdal.GetDataTypeSize(datatype) / 8

    val buf = Array.ofDim[Byte](datasize * tilesize * tilesize * bands)

    for (dty <- 0 until tiles.height.toInt) {
      for (dtx <- 0 until tiles.width().toInt) {

        val tx:Long = dtx + tiles.w
        val ty:Long = tiles.n - dty

        val x: Int = dtx * tilesize
        val y: Int = dty * tilesize

        val success = scaled.ReadRaster(x, y, tilesize, tilesize, tilesize, tilesize, datatype, buf, null)

        if (success != gdalconstConstants.CE_None)
        {
          println("Failed reading raster" + success)
        }

        val raster = GDALUtils.toRaster(tilesize, tilesize, bands, datatype, buf)

        // save the tile...
        //        GeotoolsRasterUtils.saveLocalGeotiff("/data/export/tiles/tile-" + ty + "-" + tx, raster,
        //          tx, ty, zoom, tilesize, nodata.doubleValue())

        //        GDALUtils.saveRaster(raster, "/data/export/tiles/tile-" + ty + "-" + tx, nodata)
        result.append((new TileIdWritable(TMSUtils.tileid(tx, ty, zoom)), RasterWritable.toWritable(raster)))
      }
    }

    scaled.delete()

    result.iterator
  }

  private def copyPixel(x: Int, y: Int, b: Int, src: Raster, dst: WritableRaster) {
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

  private def mergeTile(r1: RasterWritable, r2: RasterWritable):RasterWritable = {
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
    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    val in = context.makeRDD(inputs).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val rawtiles = new PairRDDFunctions(in.flatMap(input => {
      makeTiles(input)
    }).persist(StorageLevel.MEMORY_AND_DISK_SER))

    val mergedTiles=rawtiles.reduceByKey((r1, r2) => {
      mergeTile(r1, r2)
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    
    val job:Job = new Job(HadoopUtils.createConfiguration())

    val tileIncrement = 1

    job.getConfiguration.setInt(TileIdPartitioner.INCREMENT_KEY, tileIncrement)
    // save the new pyramid
    val dp = MrsImageDataProvider.setupMrsPyramidOutputFormat(job, output, bounds, zoom,
      tilesize, tiletype, bands, protectionlevel, providerproterties)

    val tileBounds = TMSUtils.boundsToTile(bounds.getTMSBounds, zoom, tilesize)

    val splitGenerator =  new ImageSplitGenerator(tileBounds.w, tileBounds.s,
      tileBounds.e, tileBounds.n, zoom, tileIncrement)

    val sparkPartitioner = new SparkTileIdPartitioner(splitGenerator)

    val sorted = mergedTiles.sortByKey().partitionBy(sparkPartitioner)
    // this is missing in early spark APIs
    //val sorted = mosaiced.repartitionAndSortWithinPartitions(sparkPartitioner)

    // save the image
    sorted.saveAsNewAPIHadoopDataset(job.getConfiguration)

    dp.teardown(job)

    // calculate and save metadata
    val nodatas = Array.ofDim[Double](bands)
    for (x <- 0 until nodatas.length)
    {
      nodatas(x) = nodata.doubleValue()
    }

    MrsImagePyramid.calculateMetadata(output, zoom, dp.getMetadataWriter, null,
      nodatas, bounds, job.getConfiguration, null, null)

    // write splits
    //sparkPartitioner.writeSplits(output, zoom, job.getConfiguration)


    true
  }

  override def teardown(job: JobArguments): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    inputs = in.readUTF().split(",")
    output = in.readUTF()
    bounds = Bounds.fromDelimitedString(in.readUTF())
    zoom = in.readInt()
    tilesize = in.readInt()
    tiletype = in.readInt()
    bands = in.readInt()
    nodata = in.readDouble()
    categorical = in.readBoolean()

    providerproterties = in.readObject().asInstanceOf[Properties]
    protectionlevel = in.readUTF()


  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(inputs.mkString(","))
    out.writeUTF(output)
    out.writeUTF(bounds.toDelimitedString)
    out.writeInt(zoom)
    out.writeInt(tilesize)
    out.writeInt(tiletype)
    out.writeInt(bands)
    out.writeDouble(nodata.doubleValue())
    out.writeBoolean(categorical)

    out.writeObject(providerproterties)

    out.writeUTF(protectionlevel)

  }
}
