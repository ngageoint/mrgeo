/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.mapalgebra

import java.io._

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.gdal.gdal.Dataset
import org.mrgeo.colorscale.applier.{ColorScaleApplier, JpegColorScaleApplier, PngColorScaleApplier}
import org.mrgeo.colorscale.{ColorScale, ColorScaleManager}
import org.mrgeo.core.MrGeoConstants
import org.mrgeo.data.raster.{MrGeoRaster, RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils._
import org.mrgeo.utils.tms.{Bounds, TMSUtils, Tile}

//import scala.collection.JavaConverters._
import scala.collection.mutable

object ExportMapOp extends MapOpRegistrar {
  val IN_MEMORY = "In-Memory"
  private val X:String = "$X"
  private val Y:String = "$Y"
  private val ZOOM:String = "$ZOOM"
  private val ID:String = "$ID"
  private val LAT:String = "$LAT"
  private val LON:String = "$LON"
  var inMemoryTestPath:String = _

  override def register:Array[String] = {
    Array[String]("export")
  }

  def create(raster:RasterMapOp, name:String, singleFile:Boolean = false,
             zoom:Int = -1, maxSize:String = "", numTiles:Int = -1,
             mosaic:Int = -1, format:String = "tif", randomTiles:Boolean = false,
             tms:Boolean = false, colorscale:String = "", tileids:String = "",
             bounds:String = "", allLevels:Boolean = false, overridenodata:Double = Double.NegativeInfinity):MapOp = {

    new ExportMapOp(Some(raster), name, zoom, maxSize, numTiles,
      mosaic, format, randomTiles, singleFile,
      tms, colorscale, tileids, bounds, allLevels, overridenodata)
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp = {
    new ExportMapOp(node, variables)
  }


}

@SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
@SuppressFBWarnings(value = Array("UPM_UNCALLED_PRIVATE_METHOD"), justification = "Scala constant")
@SuppressFBWarnings(value = Array("PREDICTABLE_RANDOM"), justification = "OK, just getting random tileids")
class ExportMapOp extends RasterMapOp with Logging with Externalizable {
  private var rasterRDD:Option[RasterRDD] = None
  private var raster:Option[RasterMapOp] = None

  private var name:String = _
  private var numTiles:Option[Int] = None
  private var zoom:Option[Int] = None
  private var maxSizeInKb:Option[Int] = None
  private var mosaic:Option[Int] = None
  private var format:Option[String] = None
  private var randomtile:Boolean = false
  private var singlefile:Boolean = false
  private var tms:Boolean = false
  private var colorscale:Option[String] = None
  private var tileids:Option[Seq[Long]] = None
  private var bounds:Option[Bounds] = None
  private var alllevels:Boolean = false
  private var overridenodata:Option[Double] = None

  private var mergedimage:Option[MrGeoRaster] = None
  private var mergedbounds: Option[Bounds] = None
  private var mergednodata: Option[Array[Double]] = None

  def this(raster:Option[RasterMapOp], name:String, zoom:Int, maxSize: String, numTiles:Int,
           mosaic:Int, format:String,
           randomTiles:Boolean, singleFile:Boolean, tms:Boolean, colorscale:String, tileids:String,
           bounds:String, allLevels:Boolean, overridenodata:Double = Double.NegativeInfinity) = {
    this()

    this.raster = raster
    this.name = name
    this.zoom = if (zoom > 0) {
      Some(zoom)
    }
    else {
      None
    }
    this.maxSizeInKb = if (maxSize != null && maxSize.length > 0) {
      Some(SparkUtils.humantokb(maxSize))
    }
    else {
      None
    }
    this.numTiles = if (numTiles > 0) {
      Some(numTiles)
    }
    else {
      None
    }
    this.mosaic = if (mosaic > 0) {
      Some(mosaic)
    }
    else {
      None
    }
    //    this.format = if (format != null && format.length > 0) Some(format) else None
    this.colorscale = if (colorscale != null && colorscale.length > 0) {
      Some(colorscale)
    }
    else {
      None
    }
    this.tileids = if (tileids != null && tileids.length > 0) {
      Some(tileids.split(",").map(_.toLong).toSeq)
    }
    else {
      None
    }
    this.bounds = if (bounds != null && bounds.length > 0) {
      Some(Bounds.fromCommaString(bounds))
    }
    else {
      None
    }
    this.randomtile = randomTiles
    this.singlefile = singleFile
    this.tms = tms
    this.alllevels = allLevels
    this.overridenodata = if (overridenodata != Double.NegativeInfinity) {
      Some(overridenodata)
    }
    else {
      None
    }


    this.format = Some(format match {
      case "tiff" | "geotiff" | "geotif" => "tif"
      case "jpeg" => "jpg"
      case _ => format
    })

  }

  override def rdd():Option[RasterRDD] = rasterRDD

  def image():Dataset = {
    mergedimage match {
      case Some(d) => d.toDataset(mergedbounds.get, mergednodata.get)
      case None => null
    }
  }

  override def getZoomLevel(): Int = {
    zoom.getOrElse(raster.getOrElse(throw new IOException("No raster input specified")).getZoomLevel())
  }

  override def execute(context:SparkContext):Boolean = {

    val input:RasterMapOp = raster getOrElse (throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse
               (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    val applier: Option[ColorScaleApplier] =
      if (colorscale.isDefined || !"tif".equals(format.getOrElse(""))) {
          format match {
            case Some(fmt) => fmt match {
              case "jpg" | "jpeg" => Some(new JpegColorScaleApplier)
              case _ => Some(new PngColorScaleApplier) // png, tif, etc...
            }
            case None => Some(new PngColorScaleApplier)
          }
      }
      else {
        None
      }

    maxSizeInKb match {
      case Some(mis) =>
        if (zoom.isDefined) {
          log.warn("The zoom level will be re-computed based on the maximum image size")
        }
        if (!singlefile) {
          throw new IOException("The maxSize argument only applies when outputting a single image")
        }
        val (bytesPerPixelPerBand, bands) = applier match {
          case Some(a) =>
            (a.getBytesPerPixelPerBand, a.getBands(meta.getBands))
          case None => (raster.get.toRaster().bytesPerPixel(), 1)
        }
        val rasterSize = RasterUtils.getMaxPixelsForSize(mis, bounds.getOrElse(meta.getBounds),
          bytesPerPixelPerBand, bands, meta.getTilesize)
        zoom = Some(math.min(rasterSize.getZoom, meta.getMaxZoomLevel))
      case None =>
        if (zoom.isEmpty) {
          zoom = Some(meta.getMaxZoomLevel)
        }
    }

    val bnds = TMSUtils.tileBounds(bounds.getOrElse(meta.getBounds), zoom.get, meta.getTilesize)

    // println("Exporting " + meta.getPyramid + " at zoom level " + zoom.get)

    do {
      val rdd =
        input.rdd(zoom.get) getOrElse (throw new IOException("Can't load RDD! Ouch! " + input.getClass.getName))

      val tiles = calculateTiles(meta)

      if (singlefile) {
        saveImage(rdd, tiles, meta, applier, bnds, reformat = false)
      }
      else if (mosaic.isDefined) {
        tiles.foreach(start => {
          val mosaiced = mutable.Set.newBuilder[Long]

          val startid = TMSUtils.tileid(start, zoom.get)
          for (ty <- startid.ty to startid.ty + mosaic.get) {
            for (tx <- startid.tx to startid.tx + mosaic.get) {
              mosaiced += TMSUtils.tileid(tx, ty, zoom.get)
            }
          }
          saveImage(rdd, mosaiced.result().toSet, meta, applier, bnds)
        })
      }
      else {
        tiles.foreach(tile => {
          saveImage(rdd, Array[Long](tile).toSet, meta, applier, bnds)
        })
      }
      zoom = Some(zoom.get - 1)
    } while (alllevels && zoom.get > 0)

    // make the outputs the inputs, no modification was done here...
    rasterRDD = raster.get.rdd()
    metadata(raster.get.metadata().get)

    if (ExportMapOp.inMemoryTestPath != null) {
      val output = makeOutputName(ExportMapOp.inMemoryTestPath, format.get, 0, 0, 0, reformat = false)
      GDALUtils.saveRaster(mergedimage.get.toDataset(mergedbounds.get, mergednodata.get),
        output, null, meta.getDefaultValueDouble(0), format.get)
    }
    true
  }

  override def setup(job:JobArguments, conf:SparkConf) = true

  override def teardown(job:JobArguments, conf:SparkConf) = true

  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    raster = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    name = MapOp.decodeString(node.getChild(1), variables) getOrElse
           (throw new ParserException("Output name not spectified"))

    // Check for optional single file flag
    if (node.getNumChildren > 2) {
      MapOp.decodeBoolean(node.getChild(2), variables) match {
        case Some(b) => singlefile = b
        case _ =>
      }
    }

    // Check for optional zoom
    if (node.getNumChildren > 3) {
      MapOp.decodeInt(node.getChild(3), variables) match {
        case Some(i) => zoom = if (i > 0) Some(i) else None
        case _ =>
      }
    }

    // Check for optional num tiles
    if (node.getNumChildren > 4) {
      MapOp.decodeInt(node.getChild(4), variables) match {
        case Some(i) => numTiles = if (i > 0) Some(i) else None
        case _ =>
      }
    }

    // Check for optional mosaic count
    if (node.getNumChildren > 5) {
      MapOp.decodeInt(node.getChild(5), variables) match {
        case Some(i) => mosaic = if (i > 0) Some(i) else None
        case _ =>
      }
    }

    // Check for optional format string
    if (node.getNumChildren > 6) {
      MapOp.decodeString(node.getChild(6), variables) match {
        case Some(s) => format = Some(s)
        case _ =>
      }
    }

    // Check for optional random tiles flag
    if (node.getNumChildren > 7) {
      MapOp.decodeBoolean(node.getChild(7), variables) match {
        case Some(b) => randomtile = b
        case _ =>
      }
    }


    // Check for optional tms naming scheme flag
    if (node.getNumChildren > 8) {
      MapOp.decodeBoolean(node.getChild(8), variables) match {
        case Some(b) => tms = b
        case _ =>
      }
    }

    // Check for optional color scale name string
    if (node.getNumChildren > 9) {
      MapOp.decodeString(node.getChild(9), variables) match {
        case Some(s) => colorscale = if (s.length > 0) Some(s) else None
        case _ =>
      }
    }

    // Check for optional tileids string
    if (node.getNumChildren > 10) {
      MapOp.decodeString(node.getChild(10), variables) match {
        case Some(s) => tileids = if (s.length > 0) Some(s.split(",").map(_.toLong).toSeq) else None
        case _ =>
      }
    }

    // Check for optional bounds string
    if (node.getNumChildren > 11) {
      MapOp.decodeString(node.getChild(11), variables) match {
        case Some(s) => bounds = if (s.length > 0) Some(Bounds.fromCommaString(s)) else None
        case _ =>
      }
    }

    // Check for optional all zoom levels flag
    if (node.getNumChildren > 12) {
      MapOp.decodeBoolean(node.getChild(12), variables) match {
        case Some(b) => alllevels = b
        case _ =>
      }
    }

    // Check for optional max image size
    if (node.getNumChildren > 13) {
      MapOp.decodeString(node.getChild(13), variables) match {
        case Some(strMaxSize) =>
          maxSizeInKb = Some(SparkUtils.humantokb(strMaxSize))
        case _ =>
      }
    }

    format = Some(format match {
      case Some(s) => s match {
        case "tiff" | "geotiff" | "geotif" => "tif"
        case "jpeg" => "jpg"
        case _ => s
      }
      case _ => "tif"
    })

  }

  private def saveImage(rdd:RasterRDD, tiles:Set[Long],
                        meta:MrsPyramidMetadata,
                        applier:Option[ColorScaleApplier],
                        bnds:Bounds,
                        reformat:Boolean = true) = {
    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x:TileIdWritable, y:TileIdWritable):Int = x.compareTo(y)
    }

    val filtered = if (tiles.isEmpty) {
      rdd
    }
    else {
      rdd.filter(tile => tiles.contains(tile._1.get))
    }

    val replaced = if (overridenodata.isDefined) {
      val nodatas = meta.getDefaultValues
      val over = overridenodata.get

      var s = "Overriding nodata ["
      nodatas.foreach(n => s += n + " ")
      s += "] with " + over

      logInfo(s)

      filtered.map(tile => {

        def isNodata(value:Double, nodata:Double):Boolean = {
          if (nodata.isNaN) {
            value.isNaN
          }
          else {
            FloatUtils.isEqual(nodata, value)
          }
        }

        val raster = RasterWritable.toMrGeoRaster(tile._2)

        var b = 0
        while (b < raster.bands()) {
          val nodata = nodatas(b)
          var y = 0
          while (y < raster.height()) {
            var x = 0
            while (x < raster.width()) {
              if (isNodata(raster.getPixelDouble(x, y, b), nodata)) {
                raster.setPixel(x, y, b, over)
              }
              x += 1
            }
            y += 1
          }
          b += 1
        }
        (tile._1, RasterWritable.toWritable(raster))
      })
    }
    else {
      filtered
    }

    val nd = meta.getDefaultValues
    if (overridenodata.isDefined) {
      for (x <- 0 until nd.length) {
        nd(x) = overridenodata.get
      }
    }

    // val bnds = SparkUtils.calculateBounds(RasterRDD(replaced), zoom.get, meta.getTilesize)
    val image = SparkUtils.mergeTiles(RasterRDD(replaced), zoom.get, meta.getTilesize, nd)

    // Load the optionally specified color scale
    var cs: Option[ColorScale] = colorscale match {
      case Some(csname) =>
        if (!csname.isEmpty) {
          try {
            Some(ColorScaleManager.fromName(csname))
          }
          catch {
            case _: ColorScale.ColorScaleException => throw new IOException("Invalid color scale name: " + csname)
          }
        }
        else {
          None
        }
      case None => None
    }
    val rasterToSave: MrGeoRaster =
      cs match {
        case Some(c) => colorRaster(applier.get, meta, image, c, zoom.get)
        case None =>
          if (!"tif".equals(format.get)) {
            val csDefaultName = meta.getTag(MrGeoConstants.MRGEO_DEFAULT_COLORSCALE)
            if (csDefaultName != null && !csDefaultName.isEmpty) {
              var csDefault:ColorScale = null
              try {
                csDefault = ColorScaleManager.fromName(csDefaultName)
              }
              catch {
                case _: ColorScale.ColorScaleException => throw new IOException("Image's default color scale is invalid: " + csDefaultName)
              }
              if (csDefault == null) {
                throw new IOException("Cannot load default color scale: " + csDefaultName)
              }
              cs = Some(csDefault)
            }
            else {
              cs = Some(ColorScale.createDefaultGrayScale())
            }
            colorRaster(applier.get, meta, image, cs.get, zoom.get)
          }
          else {
            image
          }
      }
    if (name == ExportMapOp.IN_MEMORY) {
      mergedimage = Some(rasterToSave)
      mergedbounds = Some(bnds)
      mergednodata = Some(nd)
    }
    else {
      val output = makeOutputName(name, format.get, replaced.keys.min().get(), zoom.get, meta.getTilesize, reformat)
      GDALUtils.saveRaster(rasterToSave.toDataset(bnds, nd), output, bnds, nd(0), format.get)
    }
  }

  private def colorRaster(applier: ColorScaleApplier,
                          metadata: MrsPyramidMetadata,
                          raster: MrGeoRaster,
                          colorscale: ColorScale,
                          zoom: Int): MrGeoRaster =
  {
    applier.applyColorScale(raster, colorscale, metadata.getExtrema(zoom),
      metadata.getDefaultValues, metadata.getQuantiles)
  }

  private def calculateTiles(meta:MrsPyramidMetadata):Set[Long] = {
    val tiles = mutable.Set.newBuilder[Long]

    val tilebounds = meta.getTileBounds(zoom.get)
    if (mosaic.isDefined) {
      val num = mosaic.get
      for (ty <- tilebounds.getMinY to tilebounds.getMaxY by num) {
        for (tx <- tilebounds.getMinX to tilebounds.getMaxX by num) {
          if (numTiles.isDefined && numTiles.get > 0) {
            tiles += TMSUtils.tileid(tx, ty, zoom.get)
            numTiles = Some(numTiles.get - (num * num))
          }
        }
      }
    }
    else if (bounds.isDefined) {
      val tilebounds = TMSUtils.boundsToTile(bounds.get, zoom.get, meta.getTilesize)
      for (ty <- tilebounds.s to tilebounds.n) {
        for (tx <- tilebounds.w to tilebounds.e) {
          tiles += TMSUtils.tileid(tx, ty, zoom.get)
        }
      }
    }
    else if (randomtile && numTiles.isDefined) {
      for (_ <- 0 until numTiles.get) {
        val tx = tilebounds.getMinX +
                 (Math.random() * (tilebounds.getMaxX - tilebounds.getMinX)).toLong
        val ty = tilebounds.getMinY +
                 (Math.random() * (tilebounds.getMaxY - tilebounds.getMinY)).toLong

        val id = TMSUtils.tileid(tx, ty, zoom.get)
        if (!tiles.result().contains(id)) {
          tiles += id
        }
      }
    }
    else {
      for (ty <- tilebounds.getMinY to tilebounds.getMaxY) {
        for (tx <- tilebounds.getMinX to tilebounds.getMaxX) {
          val tileid = TMSUtils.tileid(tx, ty, zoom.get)
          if (numTiles.isDefined && numTiles.get > 0) {
            tiles += tileid
            numTiles = Some(numTiles.get - 1)
          }
          else {
            tiles += tileid
          }
        }
      }
    }

    tiles.result().toSet
  }

  @SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "Use File() to get parent directory")
  private def makeOutputName(template:String, format:String, tileid:Long, zoom:Int, tilesize:Int,
                             reformat:Boolean):String = {

    if (tms) {
      makeTMSOutputName(template, format, tileid, zoom)
    }
    else {
      val t:Tile = TMSUtils.tileid(tileid, zoom)
      val bounds:Bounds = TMSUtils.tileBounds(t.tx, t.ty, zoom, tilesize)
      var output:String = null
      if (template.contains(ExportMapOp.X) || template.contains(ExportMapOp.Y) ||
          template.contains(ExportMapOp.ZOOM) || template.contains(ExportMapOp.ID) ||
          template.contains(ExportMapOp.LAT) || template.contains(ExportMapOp.LON)) {
        output = template.replace(ExportMapOp.X, "%03d".format(t.tx))
        output = output.replace(ExportMapOp.Y, "%03d".format(t.ty))
        output = output.replace(ExportMapOp.ZOOM, "%d".format(zoom))
        output = output.replace(ExportMapOp.ID, "%d".format(tileid))
        if (output.contains(ExportMapOp.LAT)) {
          val lat:Double = bounds.s
          var dir:String = "N"
          if (lat < 0) {
            dir = "S"
          }
          output = output.replace(ExportMapOp.LAT, "%s%3d".format(dir, Math.abs(lat.toInt)))
        }
        if (output.contains(ExportMapOp.LON)) {
          val lon:Double = bounds.w
          var dir:String = "E"
          if (lon < 0) {
            dir = "W"
          }
          output = output.replace(ExportMapOp.LON, "%s%3d".format(dir, Math.abs(lon.toInt)))
        }
      }
      else {
        output = template
        if (reformat) {
          if (template.endsWith("/")) {
            output += "tile-"
          }
          output += "%d".format(tileid) + "-" + "%03d".format(t.ty) + "-" + "%03d".format(t.tx)
        }
      }
      if ((format == "tif") && !(output.endsWith(".tif") || output.endsWith(".tiff"))) {
        output += ".tif"
      }
      else if ((format == "png") && !output.endsWith(".png")) {
        output += ".png"
      }
      else if ((format == "jpg") && !(output.endsWith(".jpg") || output.endsWith(".jpeg"))) {
        output += ".jpg"
      }
      val f:File = new File(output)
      FileUtils.createDir(f.getParentFile)

      output
    }
  }

  @SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "Use File() to get parent directory")
  @throws(classOf[IOException])
  private def makeTMSOutputName(base:String, format:String, tileid:Long, zoom:Int):String = {
    val t:Tile = TMSUtils.tileid(tileid, zoom)
    val output:String = "%s/%d/%d/%d.%s".format(base, zoom, t.tx, t.ty,
      format match {
        case "tif" | "tiff" =>
          "tif"
        case "png" =>
          "png"
        case "jpg" | "jpeg" =>
          "jpg"
      })

    val f:File = new File(output)
    FileUtils.createDir(f.getParentFile)

    output
  }

}
