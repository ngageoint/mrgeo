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

package org.mrgeo.utils

import java.awt.image.DataBuffer
import java.io._
import java.net.URI
import java.nio._
import java.nio.file.Files
import java.util.zip.GZIPOutputStream
import javax.xml.bind.DatatypeConverter

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.fs.Path
import org.gdal.gdal.{Band, Dataset, Driver, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr
import org.gdal.osr.{CoordinateTransformation, SpatialReference, osr, osrConstants}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.tms.{Bounds, TMSUtils}

import scala.collection.JavaConversions._


class GDALException extends IOException {
  private var origException:Exception = _

  def this(e:Exception) {
    this()
    origException = e
  }

  def this(msg:String) {
    this()
    origException = new Exception(msg)
  }

  def this(msg:String, e:Exception) {
    this()
    origException = new Exception(msg, e)
  }

  override def printStackTrace() {
    this.origException.printStackTrace()
  }
}

@SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
@SuppressFBWarnings(value = Array("PZLA_PREFER_ZERO_LENGTH_ARRAYS"), justification = "api")
object GDALUtils extends Logging {

  val EPSG4326:String = osrConstants.SRS_WKT_WGS84

  private val VSI_PREFIX:String = "/vsimem/"
  private val GDAL_PAM_ENABLED:String = "GDAL_PAM_ENABLED"

  initializeGDAL()

  // empty method to force static initializer
  def register() = {}

  def isValidDataset(imagename:String):Boolean = {
    try {
      val image:Dataset = GDALUtils.open(imagename)
      GDALUtils.close(image)
      return true
    }
    catch {
      case ignored:IOException =>
    }

    false
  }

  def open(stream:InputStream):Dataset = {
    open(IOUtils.toByteArray(stream))
  }

  def open(bytes:Array[Byte]):Dataset = {
    val imagename:String = "stream" + HadoopUtils.createRandomString(5)

    gdal.FileFromMemBuffer(VSI_PREFIX + imagename, bytes)

    val image = gdal.Open(VSI_PREFIX + imagename)
    if (image != null) {
      logDebug("  Image loaded successfully: " + imagename)
      image
    }
    else {
      logInfo(
        "Image not loaded, but unfortunately no exceptions were thrown, look for a logged explanation somewhere above")
      null
    }
  }

  def createEmptyMemoryRaster(src:Dataset, width:Int, height:Int):Dataset = {

    val bands:Int = src.getRasterCount
    var datatype:Int = -1

    val nodatas = Array.newBuilder[Double]

    val nodata = Array.ofDim[java.lang.Double](1)
    var i:Int = 1
    while (i <= src.GetRasterCount()) {
      val band:Band = src.GetRasterBand(i)
      if (datatype < 0) {
        datatype = band.getDataType
      }

      band.GetNoDataValue(nodata)
      nodatas += (if (nodata(0) == null) {
        java.lang.Double.NaN
      }
      else {
        nodata(0)
      })
      i += 1
    }

    createEmptyMemoryRaster(width, height, bands, datatype, nodatas.result())
  }

  def createEmptyMemoryRaster(width:Int, height:Int, bands:Int, datatype:Int,
                              nodatas:Array[Double] = null):Dataset = {
    val driver:Driver = gdal.GetDriverByName("MEM")

    val dataset = driver.Create("InMem", width, height, bands, datatype)

    if (dataset != null) {
      if (nodatas != null) {
        var i:Int = 1
        while (i <= dataset.getRasterCount) {
          val nodata:Double = if (i < nodatas.length) {
            nodatas(i - 1)
          }
          else {
            nodatas(nodatas.length - 1)
          }
          val band:Band = dataset.GetRasterBand(i)
          band.Fill(nodata)
          band.SetNoDataValue(nodata)
          i += 1
        }
      }
      return dataset
    }

    null
  }

  def createEmptyDiskBasedRaster(src:Dataset, width:Int, height:Int):Dataset = {

    val bands:Int = src.getRasterCount
    var datatype:Int = -1

    val nodatas = Array.newBuilder[Double]

    val nodata = Array.ofDim[java.lang.Double](1)
    var i:Int = 1
    while (i <= src.GetRasterCount()) {
      val band:Band = src.GetRasterBand(i)
      if (datatype < 0) {
        datatype = band.getDataType
      }

      band.GetNoDataValue(nodata)
      nodatas += (if (nodata(0) == null) {
        java.lang.Double.NaN
      }
      else {
        nodata(0)
      })
      i += 1
    }

    createEmptyDiskBasedRaster(width, height, bands, datatype, nodatas.result())
  }

  def createEmptyDiskBasedRaster(width:Int, height:Int, bands:Int, datatype:Int,
                              nodatas:Array[Double] = null):Dataset = {

    val driver:Driver = gdal.GetDriverByName("GTiff")

    val f = File.createTempFile("gdal-tmp-", ".tif")
    val filename = f.getCanonicalPath
    if (!f.delete()) {
      throw new IOException("Error creating tmp file")
    }

    val dataset = driver.Create(filename, width, height, bands, datatype)

    if (dataset != null) {
      if (nodatas != null) {
        var i:Int = 1
        while (i <= dataset.getRasterCount) {
          val nodata:Double = if (i < nodatas.length) {
            nodatas(i - 1)
          }
          else {
            nodatas(nodatas.length - 1)
          }
          val band:Band = dataset.GetRasterBand(i)
          band.Fill(nodata)
          band.SetNoDataValue(nodata)
          i += 1
        }
      }
      return dataset
    }

    null
  }


  def toGDALDataType(rasterType:Int):Int = {
    rasterType match {
      case DataBuffer.TYPE_BYTE => gdalconstConstants.GDT_Byte
      case DataBuffer.TYPE_SHORT => gdalconstConstants.GDT_Int16
      case DataBuffer.TYPE_USHORT => gdalconstConstants.GDT_UInt16
      case DataBuffer.TYPE_INT => gdalconstConstants.GDT_Int32
      case DataBuffer.TYPE_FLOAT => gdalconstConstants.GDT_Float32
      case DataBuffer.TYPE_DOUBLE => gdalconstConstants.GDT_Float64
      case _ => gdalconstConstants.GDT_Unknown
    }
  }

  def toRasterDataBufferType(gdaldatatype:Int):Int = {
    gdaldatatype match {
      case gdalconstConstants.GDT_Byte => DataBuffer.TYPE_BYTE
      case gdalconstConstants.GDT_UInt16 => DataBuffer.TYPE_USHORT
      case gdalconstConstants.GDT_Int16 => DataBuffer.TYPE_SHORT
      case gdalconstConstants.GDT_UInt32 => DataBuffer.TYPE_INT
      case gdalconstConstants.GDT_Int32 => DataBuffer.TYPE_INT
      case gdalconstConstants.GDT_Float32 => DataBuffer.TYPE_FLOAT
      case gdalconstConstants.GDT_Float64 => DataBuffer.TYPE_DOUBLE
      case _ => DataBuffer.TYPE_UNDEFINED
    }
  }


  def swapBytes(bytes:Array[Byte], gdaldatatype:Int) = {

    var tmp:Byte = 0
    var i:Int = 0
    gdaldatatype match {
      // Since it's byte data, there is nothing to swap - do nothing
      case gdalconstConstants.GDT_Byte =>
      // 2 byte value... swap byte 1 with 2
      case gdalconstConstants.GDT_UInt16 | gdalconstConstants.GDT_Int16 =>
        while (i + 1 < bytes.length) {
          tmp = bytes(i)
          bytes(i) = bytes(i + 1)
          bytes(i + 1) = tmp
          i += 2
        }
      // 4 byte value... swap bytes 1 & 4, 2 & 3
      case gdalconstConstants.GDT_UInt32 | gdalconstConstants.GDT_Int32 | gdalconstConstants.GDT_Float32 =>
        while (i + 3 < bytes.length) {
          // swap 0 & 3
          tmp = bytes(i)
          bytes(i) = bytes(i + 3)
          bytes(i + 3) = tmp

          // swap 1 & 2
          tmp = bytes(i + 1)
          bytes(i + 1) = bytes(i + 2)
          bytes(i + 2) = tmp
          i += 4
        }
      // 8 byte value... swap bytes 1 & 8, 2 & 7, 3 & 6, 4 & 5
      case gdalconstConstants.GDT_Float64 =>
        while (i + 7 < bytes.length) {
          // swap 0 & 7
          tmp = bytes(i)
          bytes(i) = bytes(i + 7)
          bytes(i + 7) = tmp

          // swap 1 & 6
          tmp = bytes(i + 1)
          bytes(i + 1) = bytes(i + 6)
          bytes(i + 6) = tmp

          // swap 2 & 5
          tmp = bytes(i + 2)
          bytes(i + 2) = bytes(i + 5)
          bytes(i + 5) = tmp

          // swap 3 & 4
          tmp = bytes(i + 3)
          bytes(i + 3) = bytes(i + 4)
          bytes(i + 4) = tmp
          i += 8
        }
    }
  }

  def getnodatas(imagename:String):Array[Number] = {
    val ds = open(imagename)
    if (ds != null) {
      try {
        return getnodatas(ds)
      }
      finally {
        close(ds)
      }
    }
    throw new GDALException("Error opening image: " + imagename)
  }

  def getnodatas(image:Dataset):Array[Number] = {
    val bands = image.GetRasterCount

    val nodatas = Array.fill[Double](bands)(Double.NaN)

    val v = new Array[java.lang.Double](1)
    for (i <- 1 to bands) {
      val band:Band = image.GetRasterBand(i)
      band.GetNoDataValue(v)
      nodatas(i - 1) =
          if (v(0) != null) {
            v(0)
          }
          else {
            band.getDataType match {
              case gdalconstConstants.GDT_Byte |
                   gdalconstConstants.GDT_UInt16 | gdalconstConstants.GDT_Int16 |
                   gdalconstConstants.GDT_UInt32 | gdalconstConstants.GDT_Int32 => 0
              case gdalconstConstants.GDT_Float32 => Float.NaN
              case gdalconstConstants.GDT_Float64 => Double.NaN
            }
          }

    }

    nodatas
  }

  @SuppressFBWarnings(value = Array("REC_CATCH_EXCEPTION"), justification = "GDAL may have throw exceptions enabled")
  @SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "GDAL only reads image files")
  def open(imagename:String):Dataset = {
    try {
      val uri:URI = new URI(imagename)
      logDebug("Loading image with GDAL: " + imagename)

      val file:File = new File(uri.getPath)
      if (file.exists) {
        val image = gdal.Open(file.getCanonicalPath)
        if (image != null) {
          logDebug("  Image loaded successfully: " + imagename)
          return image
        }
      }

      val p = new Path(uri)
      val fs = HadoopFileUtils.getFileSystem(p)
      val is = fs.open(p)

      val bytes = IOUtils.toByteArray(is)

      val vsiname = VSI_PREFIX + imagename
      gdal.FileFromMemBuffer(vsiname, bytes)

      val image = gdal.Open(vsiname)
      if (image != null) {
        logDebug("  Image loaded successfully: " + imagename)
        return image
      }

      logInfo(
        "Image not loaded, but unfortunately no exceptions were thrown, look for a logged explanation somewhere above")
    }
    catch {
      case e:Exception => throw new GDALException("Error opening image file: " + imagename, e)
    }

    null
  }

  def close(image:Dataset) {
    val files = image.GetFileList

    image.delete()

    // unlink the file from memory if is has been streamed
    for (f <- files) {
      f match {
        case file:String =>
          if (file.startsWith(VSI_PREFIX)) {
            gdal.Unlink(file)
          }
        case _ =>
      }
    }
  }


  def calculateZoom(imagename:String, tilesize:Int):Int = {
    try {
      val image = GDALUtils.open(imagename)
      if (image != null) {
        val b = getBounds(image)
        val px = b.width() / image.GetRasterXSize
        val py = b.height() / image.GetRasterYSize
        val zx = TMSUtils.zoomForPixelSize(Math.abs(px), tilesize)
        val zy = TMSUtils.zoomForPixelSize(Math.abs(py), tilesize)

        GDALUtils.close(image)
        if (zx > zy) {
          return zx
        }
        return zy
      }
    }
    catch {
      case ignored:IOException =>
    }
    -1
  }

  def getBounds(image:Dataset):Bounds = {
    val xform = image.GetGeoTransform

    val srs = new SpatialReference(image.GetProjection)
    val dst = new SpatialReference(EPSG4326)

    val tx = new CoordinateTransformation(srs, dst)

    val w = image.GetRasterXSize
    val h = image.GetRasterYSize

    var c1:Array[Double] = null
    var c2:Array[Double] = null
    var c3:Array[Double] = null
    var c4:Array[Double] = null

    if (tx != null) {
      c1 = tx.TransformPoint(xform(0), xform(3))
      c2 = tx.TransformPoint(xform(0) + xform(1) * w, xform(3) + xform(5) * h)
      c3 = tx.TransformPoint(xform(0) + xform(1) * w, xform(3))
      c4 = tx.TransformPoint(xform(0), xform(3) + xform(5) * h)
    }
    else {
      c1 = Array[Double](xform(0), xform(3))
      c2 = Array[Double](xform(0) + xform(1) * w, xform(3) + xform(5) * h)
      c3 = Array[Double](xform(0) + xform(1) * w, xform(3))
      c4 = Array[Double](xform(0), xform(3) + xform(5) * h)
    }

    new Bounds(Math.min(Math.min(c1(0), c2(0)), Math.min(c3(0), c4(0))),
      Math.min(Math.min(c1(1), c2(1)), Math.min(c3(1), c4(1))),
      Math.max(Math.max(c1(0), c2(0)), Math.max(c3(0), c4(0))),
      Math.max(Math.max(c1(1), c2(1)), Math.max(c3(1), c4(1))))
  }

  @SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "Temp file used for writing to OutputStream")
  def saveRaster(raster:Dataset, output:Either[String, OutputStream],
                 bounds:Bounds = null, nodata:Double = Double.NegativeInfinity,
                 format:String = "GTiff", options:Array[String] = Array.empty[String]):Unit = {

    val filename = output match {
      case Left(f) => f
      case Right(stream) => File.createTempFile("tmp-file", "").getCanonicalPath
    }


    saveRaster(raster, filename, format, bounds, options)

    output match {
      case Right(stream) =>
        Files.copy(new File(filename).toPath, stream)
        stream.flush()
        if (!new File(filename).delete()) {
          throw new IOException("Error deleting temporary file: " + filename)
        }
      case _ =>
    }

  }

  def saveRasterTile(raster:Dataset, output:Either[String, OutputStream],
                     tx:Long, ty:Long, zoom:Int, nodata:Double = Double.NegativeInfinity,
                     format:String = "GTiff", options:Array[String] = Array.empty[String]):Unit = {
    val bounds = TMSUtils.tileBounds(tx, ty, zoom, raster.getRasterXSize)

    saveRaster(raster, output, bounds, nodata, format, options)
  }

  def getRasterDataAsString(ds:Dataset, band:Int, x:Int, y:Int, width:Int, height:Int):String = {
    getRasterDataAsString(ds.GetRasterBand(band), x, y, width, height)
  }

  def getRasterDataAsString(band:Band, x:Int, y:Int, width:Int, height:Int):String = {
    new String(getRasterData(band, x, y, width, height))
  }

  def getRasterDataAsBase64(ds:Dataset, band:Int, x:Int, y:Int, width:Int, height:Int):String = {
    getRasterDataAsBase64(ds.GetRasterBand(band), x, y, width, height)
  }

  def getRasterDataAsBase64(band:Band, x:Int, y:Int, width:Int, height:Int):String = {

    val data = getRasterBuffer(band, x, y, width, height)
    val rastersize:Int = getRasterBytes(band, width, height)

    val chunksize = 3072 // This _must_ be a multiple of 3 for chunking of base64 to work

    val builder = StringBuilder.newBuilder
    val chunk = Array.ofDim[Byte](chunksize)

    var dataremaining = rastersize
    while (dataremaining > chunksize) {
      data.get(chunk)

      builder ++= DatatypeConverter.printBase64Binary(chunk)
      dataremaining -= chunksize
    }
    if (dataremaining > 0) {
      val smallchunk = Array.ofDim[Byte](dataremaining)
      data.get(smallchunk)

      builder ++= DatatypeConverter.printBase64Binary(smallchunk)
    }

    builder.result()
    //DatatypeConverter.printBase64Binary(getRasterData(band, x, y, width, height))
  }

  def getRasterDataAsCompressedBase64(ds:Dataset, band:Int, x:Int, y:Int, width:Int, height:Int):String = {
    getRasterDataAsCompressedBase64(ds.GetRasterBand(band), x, y, width, height)
  }

  def getRasterDataAsCompressedBase64(band:Band, x:Int, y:Int, width:Int, height:Int):String = {
    val data = getRasterDataCompressed(band, x, y, width, height)

    val base64 = DatatypeConverter.printBase64Binary(data)
    logInfo("raster data: base64: " + base64.length)

    base64
  }

  def getRasterDataAsCompressedString(ds:Dataset, band:Int, x:Int, y:Int, width:Int, height:Int):String = {
    getRasterDataAsCompressedString(ds.GetRasterBand(band), x, y, width, height)
  }

  def getRasterDataAsCompressedString(band:Band, x:Int, y:Int, width:Int, height:Int):String = {
    val data = getRasterDataCompressed(band, x, y, width, height)
    new String(data, "UTF-8")
  }

  def getRasterDataCompressed(ds:Dataset, band:Int, x:Int, y:Int, width:Int, height:Int):Array[Byte] = {
    getRasterDataCompressed(ds.GetRasterBand(band), x, y, width, height)
  }

  def getRasterDataCompressed(band:Band, x:Int, y:Int, width:Int, height:Int):Array[Byte] = {
    val data = getRasterBuffer(band, x, y, width, height)
    val rastersize:Int = getRasterBytes(band, width, height)

    logInfo("raster data: original: " + rastersize)

    //    var base64Str = DatatypeConverter.printBase64Binary(data)
    //    println("Base64: " + base64Str.length)
    //    data = null
    //
    //    var base64 = base64Str.getBytes
    //    println("Base64 bytes: " + base64.length)
    //    base64Str = null
    //
    //    val outputStream = new ByteArrayOutputStream(base64.length)
    val outputStream = new ByteArrayOutputStream(rastersize)
    val zipper = new GZIPOutputStream(outputStream)

    val chunksize = 4096
    val chunk = Array.ofDim[Byte](chunksize)

    var dataremaining = rastersize
    while (dataremaining > chunksize) {
      data.get(chunk)

      zipper.write(chunk)

      dataremaining -= chunksize
    }

    if (dataremaining > 0) {
      val smallchunk = Array.ofDim[Byte](dataremaining)
      data.get(smallchunk)

      zipper.write(smallchunk)
    }

    //zipper.write(base64)
    //base64 = null

    zipper.close()
    outputStream.close()
    val output = outputStream.toByteArray

    logInfo("raster data: compressed: " + output.length)

    output
  }

  def getRasterData(ds:Dataset, band:Int, x:Int, y:Int, width:Int, height:Int):Array[Byte] = {
    getRasterData(ds.GetRasterBand(band), x, y, width, height)
  }

  def getRasterData(band:Band, x:Int, y:Int, width:Int, height:Int):Array[Byte] = {
    val rastersize:Int = getRasterBytes(band, width, height)
    val data = getRasterBuffer(band, x, y, width, height)

    val bytes = Array.ofDim[Byte](rastersize)
    data.get(bytes)

    logInfo("read (" + bytes.length + " bytes (I think)")

    bytes
  }

  def getRasterBytes(band:Band):Int = {
    getRasterBytes(band, band.GetXSize(), band.GetYSize())
  }

  def getRasterBytes(ds:Dataset, band:Int):Int = {
    val b = ds.GetRasterBand(band)
    getRasterBytes(b, b.GetXSize(), b.GetYSize())
  }

  private def initializeGDAL() = {
    // Monkeypatch the system library path to use the gdal paths (for loading the gdal libraries
    MrGeoProperties.getInstance().getProperty(MrGeoConstants.GDAL_PATH, "").
        split(File.pathSeparator).reverse.foreach(path => {
      ClassLoaderUtil.addLibraryPath(path)
    })

    osr.UseExceptions()

    if (gdal.GetDriverCount == 0) {
      gdal.AllRegister()
    }

    if (ogr.GetDriverCount == 0) {
      ogr.RegisterAll()
    }

    val drivers:Int = gdal.GetDriverCount
    if (drivers == 0) {
      log.error("GDAL libraries were not loaded!  This probibly an error.")
    }

    log.info(gdal.VersionInfo("--version"))
    println(gdal.VersionInfo("--version"))

    val klass = classOf[gdal]
    val location = klass.getResource('/' + klass.getName().replace('.', '/') + ".class");
    osr.UseExceptions()
    log.info("GDAL jar location: " + location)
    println("GDAL jar location: " + location)

    println("Java library path: " + System.getProperty("java.library.path"));

    if (log.isDebugEnabled) {
      log.debug("GDAL Drivers supported:")

      for (i <- 0 until drivers) {
        val driver:Driver = gdal.GetDriver(i)
        logDebug("  " + driver.getLongName + "(" + driver.getShortName + ")")
      }

      log.debug("GDAL Projections supported:")
      for (o <- osr.GetProjectionMethods) {
        logDebug("  " + o)
      }
    }
  }

  private def saveRaster(ds:Dataset, file:String, format:String, bounds:Bounds, options:Array[String]):Unit = {
    val fmt = mapType(format)
    val driver = gdal.GetDriverByName(fmt)

    val pamEnabled = gdal.GetConfigOption(GDAL_PAM_ENABLED)
    gdal.SetConfigOption(GDAL_PAM_ENABLED, "NO")

    val moreoptions = fmt.toLowerCase match {
      case "gtiff" =>
        var moreoptions = ArrayUtils.add(options, "INTERLEAVE=BAND")
        moreoptions = ArrayUtils.add(moreoptions, "COMPRESS=DEFLATE")
        moreoptions = ArrayUtils.add(moreoptions, "PREDICTOR=1")
        moreoptions = ArrayUtils.add(moreoptions, "ZLEVEL=6")
        moreoptions = ArrayUtils.add(moreoptions, "TILES=YES")
        moreoptions = ArrayUtils
            .add(moreoptions, "BLOCKXSIZE=" + (if (ds.getRasterXSize < 2048) {
              ds.getRasterXSize
            }
            else {
              2048
            }))
        moreoptions = ArrayUtils
            .add(moreoptions, "BLOCKYSIZE=" + (if (ds.getRasterYSize < 2048) {
              ds.getRasterYSize
            }
            else {
              2048
            }))

        moreoptions
      case _ => options
    }

    val copy:Dataset = driver.CreateCopy(file, ds, 1, moreoptions)

    // add the bounds, if sent in.  Reproject if needed
    val xform:Array[Double] = new Array[Double](6)
    if (bounds != null) {
      val proj = ds.GetProjection()
      if (proj.length > 0) {

        val dst = new SpatialReference(proj)
        val src = new SpatialReference(EPSG4326)

        val tx = new CoordinateTransformation(src, dst)

        var c1:Array[Double] = null
        var c2:Array[Double] = null
        var c3:Array[Double] = null
        var c4:Array[Double] = null

        if (tx != null) {
          c1 = tx.TransformPoint(bounds.w, bounds.n)
          c2 = tx.TransformPoint(bounds.e, bounds.s)
          c3 = tx.TransformPoint(bounds.e, bounds.n)
          c4 = tx.TransformPoint(bounds.w, bounds.s)
        }

        val xformed = new Bounds(Math.min(Math.min(c1(0), c2(0)), Math.min(c3(0), c4(0))),
          Math.min(Math.min(c1(1), c2(1)), Math.min(c3(1), c4(1))),
          Math.max(Math.max(c1(0), c2(0)), Math.max(c3(0), c4(0))),
          Math.max(Math.max(c1(1), c2(1)), Math.max(c3(1), c4(1))))


        xform(0) = xformed.w
        xform(1) = xformed.width / copy.GetRasterXSize()
        xform(2) = 0
        xform(3) = xformed.n
        xform(4) = 0
        xform(5) = -xformed.height / copy.GetRasterYSize()

        copy.SetProjection(proj)
      }
      else {
        xform(0) = bounds.w
        xform(1) = bounds.width / copy.GetRasterXSize()
        xform(2) = 0
        xform(3) = bounds.n
        xform(4) = 0
        xform(5) = -bounds.height / copy.GetRasterYSize()

        copy.SetProjection(EPSG4326)
      }
      copy.SetGeoTransform(xform)
    }

    if (pamEnabled != null) {
      gdal.SetConfigOption(GDAL_PAM_ENABLED, pamEnabled)
    }

    if (copy == null) {
      val errno:Int = gdal.GetLastErrorNo
      val error:Int = gdal.GetLastErrorType
      val msg:String = gdal.GetLastErrorMsg
      throw new GDALException("Error saving raster: " + file + "(" + errno + ": " + error + ": " + msg + ")")
    }

    copy.delete()
  }

  private def mapType(format:String):String = {
    format.toLowerCase match {
      case "jpg" => "jpeg"
      case "tiff" |
           "tif" |
           "geotiff" |
           "geotif" |
           "gtif" => "GTiff"
      case _ => format
    }
  }

  private def getRasterBytes(band:Band, width:Int, height:Int):Int = {
    val datatype = band.getDataType
    val pixelsize = gdal.GetDataTypeSize(datatype) / 8

    val linesize = pixelsize * width
    val rastersize = linesize * height
    rastersize
  }

  private def getRasterBuffer(band:Band, x:Int, y:Int, width:Int, height:Int):ByteBuffer = {
    band.ReadRaster_Direct(x, y, width, height, band.getDataType).order(ByteOrder.nativeOrder())
  }

}


