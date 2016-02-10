package org.mrgeo.mapalgebra

import java.awt.image.WritableRaster
import java.io._

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils._

import org.mrgeo.utils.MrGeoImplicits._

//import scala.collection.JavaConverters._
import scala.collection.mutable

object ExportMapOp extends MapOpRegistrar {
  private val X: String = "$X"
  private val Y: String = "$Y"
  private val ZOOM: String = "$ZOOM"
  private val ID: String = "$ID"
  private val LAT: String = "$LAT"
  private val LON: String = "$LON"

  override def register: Array[String] = {
    Array[String]("export")
  }

  def create(raster:RasterMapOp, name:String, singleFile:Boolean = false, zoom:Int = -1, numTiles:Int = -1,
      mosaic:Int = -1, format:String = "tif", randomTiles:Boolean = false,
      tms:Boolean = false, colorscale:String = "", tileids:String = "",
      bounds:String = "", allLevels:Boolean = false, overridenodata:Double = Double.NegativeInfinity):MapOp = {

    new ExportMapOp(Some(raster), name, zoom, numTiles, mosaic, format, randomTiles, singleFile,
      tms, colorscale, tileids, bounds, allLevels, overridenodata)
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp = {
    new ExportMapOp(node, variables)
  }



}

class ExportMapOp extends RasterMapOp with Logging with Externalizable {
  private var rasterRDD: Option[RasterRDD] = None
  private var raster: Option[RasterMapOp] = None

  private var name:String = null
  private var numTiles:Option[Int] = None
  private var zoom:Option[Int] = None
  private var mosaic:Option[Int] = None
  private var format:Option[String] = None
  private var randomtile:Boolean = false
  private var singlefile:Boolean = false
  private var tms:Boolean = false
  private var colorscale:Option[String] = None
  private var tileids:Option[Seq[Long]] = None
  private var bounds:Option[TMSUtils.Bounds] = None
  private var alllevels:Boolean = false
  private var overridenodata:Option[Double] = None

  def this(raster:Option[RasterMapOp], name:String, zoom:Int, numTiles:Int, mosaic:Int, format:String,
      randomTiles:Boolean, singleFile:Boolean, tms:Boolean, colorscale:String, tileids:String,
      bounds:String, allLevels:Boolean, overridenodata:Double = Double.NegativeInfinity) = {
    this()

    this.raster = raster
    this.name = name
    this.zoom = if (zoom > 0) Some(zoom) else None
    this.numTiles = if (numTiles > 0) Some(numTiles) else None
    this.mosaic = if (mosaic > 0) Some(mosaic) else None
    //    this.format = if (format != null && format.length > 0) Some(format) else None
    this.colorscale = if (colorscale != null && colorscale.length > 0) Some(colorscale) else None
    this.tileids = if (tileids != null && tileids.length > 0) Some(tileids.split(",").map(_.toLong).toSeq) else None
    this.bounds = if (bounds != null && bounds.length > 0) Some(TMSUtils.Bounds.fromCommaString(bounds)) else None
    this.randomtile = randomTiles
    this.singlefile = singleFile
    this.tms = tms
    this.alllevels = allLevels
    this.overridenodata = if (overridenodata != Double.NegativeInfinity) Some(overridenodata) else None


    this.format = Some(format match {
    case "tiff" | "geotiff" | "geotif" => "tif"
    case "jpeg" => "jpg"
    case _ => format
    })

  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    raster = RasterMapOp.decodeToRaster(node.getChild(0), variables)

    name = MapOp.decodeString(node.getChild(1), variables) getOrElse(throw new ParserException("Output name not spectified"))

    // Check for optional single file flag
    if (node.getNumChildren > 2)
      MapOp.decodeBoolean(node.getChild(2), variables) match {
      case Some(b) => singlefile = b
      case _ =>
      }

    // Check for optional zoom
    if (node.getNumChildren > 3)
      MapOp.decodeInt(node.getChild(3), variables) match {
      case Some(i) => zoom = Some(i)
      case _ =>
      }

    // Check for optional num tiles
    if (node.getNumChildren > 4)
      MapOp.decodeInt(node.getChild(4), variables) match {
      case Some(i) => numTiles = Some(i)
      case _ =>
      }

    // Check for optional mosaic count
    if (node.getNumChildren > 5)
      MapOp.decodeInt(node.getChild(5), variables) match {
      case Some(i) => mosaic = Some(i)
      case _ =>
      }

    // Check for optional format string
    if (node.getNumChildren > 6)
      MapOp.decodeString(node.getChild(6), variables) match {
      case Some(s) => format = Some(s)
      case _ =>
      }

    // Check for optional random tiles flag
    if (node.getNumChildren > 7)
      MapOp.decodeBoolean(node.getChild(7), variables) match {
      case Some(b) => randomtile = b
      case _ =>
      }


    // Check for optional tms naming scheme flag
    if (node.getNumChildren > 8)
      MapOp.decodeBoolean(node.getChild(8), variables) match {
      case Some(b) => tms = b
      case _ =>
      }

    // Check for optional color scale name string
    if (node.getNumChildren > 9)
      MapOp.decodeString(node.getChild(9), variables) match {
      case Some(s) => colorscale = Some(s)
      case _ =>
      }

    // Check for optional format string
    if (node.getNumChildren > 10)
      MapOp.decodeString(node.getChild(10), variables) match {
      case Some(s) => tileids = Some(s.split(",").map(_.toLong).toSeq)
      case _ =>
      }

    // Check for optional format string
    if (node.getNumChildren > 11)
      MapOp.decodeString(node.getChild(11), variables) match {
      case Some(s) => bounds = Some(TMSUtils.Bounds.fromCommaString(s))
      case _ =>
      }

    // Check for optional single file flag
    if (node.getNumChildren > 12)
      MapOp.decodeBoolean(node.getChild(12), variables) match {
      case Some(b) => alllevels = b
      case _ =>
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

  override def rdd(): Option[RasterRDD] = rasterRDD


  override def execute(context: SparkContext): Boolean = {

    val input:RasterMapOp = raster getOrElse(throw new IOException("Input MapOp not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    if (zoom.isEmpty) {
      zoom = Some(meta.getMaxZoomLevel)
    }

    do {
      val rdd =
        input.rdd(zoom.get) getOrElse(throw new IOException("Can't load RDD! Ouch! " + input.getClass.getName))

      val tiles = calculateTiles(meta)

      if (singlefile) {
        saveImage(rdd, tiles, meta, reformat = false)
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
          saveImage(rdd, mosaiced.result().toSet, meta)
        })
      }
      else {
        tiles.foreach(tile => {
          saveImage(rdd, Array[Long](tile).toSet, meta)
        })
      }
      zoom = Some(zoom.get - 1)
    } while (alllevels && zoom.get > 0)

    // make the outputs the inputs, no modification was done here...
    rasterRDD = raster.get.rdd()
    metadata(raster.get.metadata().get)

    true
  }

  private def saveImage(rdd: RasterRDD, tiles: Set[Long], meta:MrsPyramidMetadata, reformat:Boolean = true) = {
    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    val filtered = rdd.filter(tile => tiles.contains(tile._1.get))

    val replaced = if (overridenodata.isDefined) {
      val nodatas = meta.getDefaultValues
      val over = overridenodata.get

      filtered.map(tile => {

        def isNodata(value:Double, nodata:Double):Boolean = {
          if (nodata.isNaN) {
            value.isNaN
          }
          else {
            nodata == value
          }
        }

        val raster = RasterWritable.toRaster(tile._2).asInstanceOf[WritableRaster]

        var y = 0
        var x = 0
        var b = 0

        while (b < raster.getNumBands) {
          val nodata = nodatas(b)
          while (y < raster.getHeight) {
            while (x < raster.getWidth) {
              val v = raster.getSampleDouble(x, y, b)

              if (isNodata(v, nodata)) {
                raster.setSample(x, y, b, over)
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
    val image = SparkUtils.mergeTiles(RasterRDD(replaced), zoom.get, meta.getTilesize, nd)
    val output = makeOutputName(name, format.get, replaced.keys.min().get(), zoom.get, meta.getTilesize, reformat)

    val bnds = SparkUtils.calculateBounds(RasterRDD(replaced), zoom.get, meta.getTilesize)
    GDALUtils.saveRaster(image, output, bnds, nd(0), format.get)
  }

  override def setup(job: JobArguments, conf: SparkConf) = true
  override def teardown(job: JobArguments, conf: SparkConf) = true


  private def calculateTiles(meta:MrsPyramidMetadata):Set[Long] = {
    val tiles = mutable.Set.newBuilder[Long]

    val tilebounds = meta.getTileBounds(zoom.get)
    if (mosaic.isDefined)
    {
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
    else if (bounds.isDefined)
    {
      val tilebounds = TMSUtils.boundsToTile(bounds.get, zoom.get, meta.getTilesize)
      for (ty <- tilebounds.s to tilebounds.n) {
        for (tx <- tilebounds.w to tilebounds.e) {
          tiles += TMSUtils.tileid(tx, ty, zoom.get)
        }
      }
    }
    else if (randomtile && numTiles.isDefined)
    {
      for (i <- 0 until numTiles.get)
      {
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
      for (ty <- tilebounds.getMinY to tilebounds.getMaxY)
        for (tx <- tilebounds.getMinX to tilebounds.getMaxX)
        {
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

    tiles.result().toSet
  }

  private def makeOutputName(template: String, format: String, tileid: Long, zoom: Int, tilesize: Int,
      reformat: Boolean): String = {

    if (tms) {
      makeTMSOutputName(template, format, tileid, zoom)
    }
    else {
      val t: TMSUtils.Tile = TMSUtils.tileid(tileid, zoom)
      val bounds: TMSUtils.Bounds = TMSUtils.tileBounds(t.tx, t.ty, zoom, tilesize)
      var output: String = null
      if (template.contains(ExportMapOp.X) || template.contains(ExportMapOp.Y) ||
          template.contains(ExportMapOp.ZOOM) || template.contains(ExportMapOp.ID) ||
          template.contains(ExportMapOp.LAT) || template.contains(ExportMapOp.LON)) {
        output = template.replace(ExportMapOp.X, "%03d".format(t.tx))
        output = output.replace(ExportMapOp.Y, "%03d".format(t.ty))
        output = output.replace(ExportMapOp.ZOOM, "%d".format(zoom))
        output = output.replace(ExportMapOp.ID, "%d".format(tileid))
        if (output.contains(ExportMapOp.LAT)) {
          val lat: Double = bounds.s
          var dir: String = "N"
          if (lat < 0) {
            dir = "S"
          }
          output = output.replace(ExportMapOp.LAT, "%s%3d".format(dir, Math.abs(lat.toInt)))
        }
        if (output.contains(ExportMapOp.LON)) {
          val lon: Double = bounds.w
          var dir: String = "E"
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
      if ((format == "tif") && (!output.endsWith(".tif") || !output.endsWith(".tiff"))) {
        output += ".tif"
      }
      else if ((format == "png") && !output.endsWith(".png")) {
        output += ".png"
      }
      else if ((format == "jpg") && (!output.endsWith(".jpg") || !output.endsWith(".jpeg"))) {
        output += ".jpg"
      }
      val f: File = new File(output)
      FileUtils.createDir(f.getParentFile)

      output
    }
  }

  @throws(classOf[IOException])
  private def makeTMSOutputName(base: String, format: String, tileid: Long, zoom: Int): String = {
    val t: TMSUtils.Tile = TMSUtils.tileid(tileid, zoom)
    val output: String = "%s/%d/%d/%d.%s".format(base, zoom, t.tx, t.ty,
      format match {
      case "tif" | "tiff" =>
        "tif"
      case "png" =>
        "png"
      case "jpg" | "jpeg" =>
        "jpg"
      })

    val f: File = new File(output)
    FileUtils.createDir(f.getParentFile)

    output
  }

  override def readExternal(in: ObjectInput) = {}
  override def writeExternal(out: ObjectOutput) = {}

}
