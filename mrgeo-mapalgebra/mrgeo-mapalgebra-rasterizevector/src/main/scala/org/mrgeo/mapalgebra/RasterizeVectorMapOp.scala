package org.mrgeo.mapalgebra

import java.awt.image.DataBuffer
import java.io.{IOException, ObjectInput, ObjectOutput, Externalizable}

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkContext, SparkConf}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.rdd.{VectorRDD, RasterRDD}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.Geometry
import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapreduce.RasterizeVectorPainter
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils._

import scala.collection.mutable.ListBuffer

object RasterizeVectorMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("RasterizeVector")
  }
  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new RasterizeVectorMapOp(node, variables)
}

class RasterizeVectorMapOp extends RasterMapOp with Externalizable
{
  var rasterRDD: Option[RasterRDD] = None
  var vectorMapOp: Option[VectorMapOp] = None
  var aggregationType: RasterizeVectorPainter.AggregationType = RasterizeVectorPainter.AggregationType.MASK
  var tilesize: Int = -1
  var zoom: Int = -1
  var column: Option[String] = None
  var bounds: Option[TMSUtils.Bounds] = None

  def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def rdd(): Option[RasterRDD] = {
    rasterRDD;
  }

  override def readExternal(in: ObjectInput): Unit = {
    aggregationType = RasterizeVectorPainter.AggregationType.valueOf(in.readUTF())
    tilesize = in.readInt()
    zoom = in.readInt()
    val hasColumn = in.readBoolean()
    column = hasColumn match {
      case true => {
        Some(in.readUTF())
      }
      case false => None
    }
    val hasBounds = in.readBoolean()
    bounds = hasBounds match {
      case true => {
        Some(new TMSUtils.Bounds(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble()))
      }
      case false => None
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(aggregationType.toString)
    out.writeInt(tilesize)
    out.writeInt(zoom)
    column match {
      case Some(c) => {
        out.writeBoolean(true)
        out.writeUTF(c)
      }
      case None => out.writeBoolean(false)
    }
    bounds match {
      case Some(b) => {
        out.writeBoolean(true)
        out.writeDouble(b.w)
        out.writeDouble(b.s)
        out.writeDouble(b.e)
        out.writeDouble(b.n)
      }
      case None => out.writeBoolean(false)
    }
  }

  override def execute(context: SparkContext): Boolean = {
    val vectorRDD: VectorRDD = vectorMapOp.getOrElse(throw new IOException("Missing vector input")).
      rdd().getOrElse(throw new IOException("Missing vector RDD"))
    val tiledVectors = vectorRDD.flatMap(U => {
      val geom = U._2
      var result = new ListBuffer[(TileIdWritable, Geometry)]
      // For each geometry, compute the tile(s) that it intersects and output the
      // the geometry to each of those tiles.
      val envelope: Envelope = calculateEnvelope(geom)
      val b: TMSUtils.Bounds = new TMSUtils.Bounds(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)

      bounds match {
        case Some(filterBounds) => {
          logWarning("filtering bounds = " + filterBounds.toString)
          if (filterBounds.intersect(b)) {
            val tiles: List[TileIdWritable] = getOverlappingTiles(zoom, tilesize, b)
            for (tileId <- tiles) {
              logWarning("adding feature to result at 1")
              result += ((tileId, geom))
            }
          }
        }
        case None => {
          val tiles: List[TileIdWritable] = getOverlappingTiles(zoom, tilesize, b)
          logWarning("result of getOverlapping for tile " + U._1.get() + " in bounds " + b + " is " + tiles.size)
          for (tileId <- tiles) {
            logWarning("adding feature to result at 2")
            result += ((tileId, geom))
          }
        }
      }
      result
    })
    logWarning("tiled vectors count " + tiledVectors.count())
    val localRdd = new PairRDDFunctions(tiledVectors)
    logWarning("local key count " + localRdd.keys.count())
    val groupedGeometries = localRdd.groupByKey()
    logWarning("grouped count " + groupedGeometries.count())
    val noData = Float.NaN
    val result = groupedGeometries.map(U => {
      val tileId = U._1
      logWarning("grouped tile id " + U._1.get())
      val rvp = new RasterizeVectorPainter(zoom,
        aggregationType,
        column match {
          case Some(c) => c
          case None => null
        },
        tilesize)
      rvp.beforePaintingTile(tileId.get)
      for (geom <- U._2) {
        rvp.paintGeometry(geom)
      }
      val raster = rvp.afterPaintingTile()
      (tileId, raster)
    })
    rasterRDD = Some(RasterRDD(result))
    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, noData))
    true
  }

  def calculateEnvelope(f: Geometry): Envelope = {
    f.toJTS().getEnvelopeInternal()
  }

  def getOverlappingTiles(zoom: Int, tileSize: Int, bounds: TMSUtils.Bounds): List[TileIdWritable] = {
    var tiles = new ListBuffer[TileIdWritable]
    val tb: TMSUtils.TileBounds = TMSUtils.boundsToTile(bounds, zoom, tileSize)
    logWarning("getOverlapping zoom = " + zoom + ", tileSize = " + tileSize + ", bounds = " + bounds + ", tileBounds = " + tb.toString)
    for (tx <- tb.w to tb.e) {
      for (ty <- tb.s to tb.n) {
        val tile: TileIdWritable = new TileIdWritable(TMSUtils.tileid(tx, ty, zoom))
        tiles += tile
      }
    }
    logWarning("returning tile list of size " + tiles.size)
    tiles.toList
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  def initialize(node:ParserNode, variables: String => Option[ParserNode]): Unit = {
    if (!(node.getNumChildren() == 3 || node.getNumChildren == 4 ||
      node.getNumChildren == 7 || node.getNumChildren == 8))
    {
      throw new ParserException(
        "RasterizeVector takes these arguments. (source vector, aggregation type, cellsize, [column], [bounds])")
    }
    vectorMapOp = VectorMapOp.decodeToVector(node.getChild(0), variables)
    if (vectorMapOp.isEmpty) {
      throw new ParserException("Only vector inputs are supported.")
    }

    aggregationType = MapOp.decodeString(node.getChild(1)) match {
      case Some(aggType) => {
        RasterizeVectorPainter.AggregationType.valueOf(aggType.toUpperCase)
      }
      case None => {
        throw new ParserException("Aggregation type must be one of: " + StringUtils.join(RasterizeVectorPainter.AggregationType.values, ", "))
      }
    }

    tilesize = MrGeoProperties.getInstance.getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE,
      MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT).toInt

    val cellSize = MapOp.decodeString(node.getChild(2)) match {
      case Some(cs) => {
        if (cs.endsWith("m")) {
          val meters = cs.replace("m", "").toDouble
          meters / LatLng.METERS_PER_DEGREE
        }
        else if (cs.endsWith("z")) {
          val zoom = cs.replace("z", "").toInt
          TMSUtils.resolution(zoom, tilesize)
        }
        else {
          if (cs.endsWith("d")) {
            cs.replace("d", "").toDouble
          }
          else {
            cs.toDouble
          }
        }
      }
      case None => {
        throw new ParserException("Missing cellSize argument")
      }
    }
    zoom = TMSUtils.zoomForPixelSize(cellSize, tilesize)
    // Check that the column name of the vector is provided when it is needed
    val nextPosition = aggregationType match {
      case RasterizeVectorPainter.AggregationType.MASK => {
        if (node.getNumChildren == 4 || node.getNumChildren == 8) {
          throw new ParserException("A column name must not be specified with MASK")
        }
        3
      }
      // SUM can be used with or without a column name being specified. If used
      // with a column name, it sums the values of that column for all features
      // that intersect that pixel. Without the column, it sums the number of
      // features that intersect the pixel.
      case RasterizeVectorPainter.AggregationType.SUM => {
        if (node.getNumChildren == 4 || node.getNumChildren == 8) {
          column = MapOp.decodeString(node.getChild(3))
          column match {
            case None => {
              throw new ParserException("A column name must be specified")
            }
            case _ => 4
          }
        }
        else {
          3
        }
      }
      case _ => {
        if (node.getNumChildren == 4 || node.getNumChildren == 8) {
          column = MapOp.decodeString(node.getChild(3))
          column match {
            case None => {
              throw new ParserException("A column name must be specified")
            }
            case _ => 4
          }
        }
        else {
          throw new ParserException("A column name must be specified")
        }
      }
    }

    // Get bounds if they were included
    if (node.getNumChildren > 4) {
      val b: Array[Double] = new Array[Double](4)
      for (i <- nextPosition until nextPosition + 4) {
        b(i) = MapOp.decodeDouble(node.getChild(i), variables) match {
          case Some(boundsVal) => boundsVal
          case None => {
            throw new ParserException("You must provide minX, minY, maxX, maxY bounds values")
          }
        }
      }
      bounds = Some(new TMSUtils.Bounds(b(0), b(1), b(2), b(3)))
    }
  }
}
