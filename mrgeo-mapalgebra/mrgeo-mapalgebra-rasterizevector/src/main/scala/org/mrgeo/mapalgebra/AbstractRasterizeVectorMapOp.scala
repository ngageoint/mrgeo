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

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.{RasterRDD, VectorRDD}
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.geometry.GeometryFactory
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.mapalgebra.vector.paint.VectorPainter.AggregationType
import org.mrgeo.utils.tms.{Bounds, TMSUtils}
import org.mrgeo.utils.{LatLng, SparkUtils, StringUtils}


abstract class AbstractRasterizeVectorMapOp extends RasterMapOp with Externalizable {
  var rasterRDD:Option[RasterRDD] = None
  var vectorMapOp:Option[VectorMapOp] = None
  var aggregationType:VectorPainter.AggregationType = VectorPainter.AggregationType.MASK
  var tilesize:Int = -1
  var zoom:Int = -1
  var column:Option[String] = None
  var bounds:Option[Bounds] = None
  var rasterForBoundsMapOp:Option[RasterMapOp] = None

  override def rdd():Option[RasterRDD] = {
    rasterRDD
  }

  override def registerClasses():Array[Class[_]] = {
    // get all the Geometry classes from the GeometryFactory
    GeometryFactory.getClasses
  }


  override def readExternal(in:ObjectInput):Unit = {
    aggregationType = VectorPainter.AggregationType.valueOf(in.readUTF())
    tilesize = in.readInt()
    zoom = in.readInt()
    val hasColumn = in.readBoolean()
    column = hasColumn match {
      case true =>
        Some(in.readUTF())
      case _ => None
    }
    val hasBounds = in.readBoolean()
    bounds = hasBounds match {
      case true =>
        Some(new Bounds(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble()))
      case _ => None
    }
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeUTF(aggregationType.toString)
    out.writeInt(tilesize)
    out.writeInt(zoom)
    column match {
      case Some(c) =>
        out.writeBoolean(true)
        out.writeUTF(c)
      case None => out.writeBoolean(false)
    }
    bounds match {
      case Some(b) =>
        out.writeBoolean(true)
        out.writeDouble(b.w)
        out.writeDouble(b.s)
        out.writeDouble(b.e)
        out.writeDouble(b.n)
      case None => out.writeBoolean(false)
    }
  }

  override def execute(context:SparkContext):Boolean = {
    val vectorRDD:VectorRDD = vectorMapOp.getOrElse(throw new IOException("Missing vector input")).
        rdd().getOrElse(throw new IOException("Missing vector RDD"))
    if (rasterForBoundsMapOp.isDefined) {
      bounds = Some(rasterForBoundsMapOp.get.metadata().getOrElse(
        throw new IOException("Unable to get metadata for the bounds raster")).getBounds)
    }
    rasterRDD = Some(RasterRDD(rasterize(vectorRDD)))

    val noData = if (aggregationType == AggregationType.MASK || aggregationType == AggregationType.MASK2) {
      RasterUtils.getDefaultNoDataForType(DataBuffer.TYPE_BYTE)
    }
    else {
      Float.NaN
    }
    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, noData,
      bounds = null, calcStats = false))
    true
  }

  /**
    * The input RDD contains one tuple for each tile that intersects at least one
    * feature. The first element of the tuple is the tile id, and the second element
    * is an Iterable containing all of the features that intersects that tile id. This
    * method is responsible for "painting" the set of features onto a raster of that
    * tile and returning the tile id and raster as a tuple. The returned RDD is the
    * collection of all the tiles containing features along with the "painted" rasters
    * for each of those tiles.
    *
    */
  def rasterize(vectorRDD:VectorRDD):RDD[(TileIdWritable, RasterWritable)]

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  def initialize(node:ParserNode, variables:String => Option[ParserNode]):Unit = {
    val usageMsg = "RasterizeVector and RasterizePoints take these arguments. (source vector, aggregation type, cellsize, [column], [bounds])"
    if (!(node.getNumChildren == 3 || node.getNumChildren == 4 || node.getNumChildren == 5 ||
          node.getNumChildren == 7 || node.getNumChildren == 8)) {
      throw new ParserException(usageMsg)
    }
    vectorMapOp = VectorMapOp.decodeToVector(node.getChild(0), variables)
    if (vectorMapOp.isEmpty) {
      throw new ParserException("Only vector inputs are supported.")
    }

    aggregationType = MapOp.decodeString(node.getChild(1)) match {
      case Some(aggType) =>
        try {
          VectorPainter.AggregationType.valueOf(aggType.toUpperCase)
        }
        catch {
          case e:java.lang.IllegalArgumentException => throw new ParserException("Aggregation type must be one of: " +
                                                                                 StringUtils.join(
                                                                                   VectorPainter.AggregationType.values,
                                                                                   ", "))
        }
      case None =>
        throw new ParserException(
          "Aggregation type must be one of: " + StringUtils.join(VectorPainter.AggregationType.values, ", "))
    }

    if (aggregationType == VectorPainter.AggregationType.GAUSSIAN) {
      throw new ParserException("Invalid aggregation type for rasterize vector")
    }
    tilesize = MrGeoProperties.getInstance.getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE,
      MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT).toInt

    val cellSize = MapOp.decodeString(node.getChild(2)) match {
      case Some(cs) =>
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
      case None =>
        throw new ParserException("Missing cellSize argument")
    }
    zoom = TMSUtils.zoomForPixelSize(cellSize, tilesize)

    if (node.getNumChildren > 3) {
      node.getNumChildren match {
        case 4 => {
          try {
            rasterForBoundsMapOp = RasterMapOp.decodeToRaster(node.getChild(3), variables)
          }
          catch {
            case e:ParserException => {
              // Since the fourth and last argument is not a raster, it must be a column
              column = MapOp.decodeString(node.getChild(3))
            }
          }
        }
        case 5 => {
          rasterForBoundsMapOp = RasterMapOp.decodeToRaster(node.getChild(4), variables)
          column = MapOp.decodeString(node.getChild(3))
        }
        case 7 => {
          parseBounds(node, variables, 3)
        }
        case 8 => {
          column = MapOp.decodeString(node.getChild(3))
          parseBounds(node, variables, 4)
        }
        case _ => throw new ParserException(usageMsg)
      }
    }

    // All the arguments have been parsed, now validate the column based on the aggregation type
    aggregationType match {
      case VectorPainter.AggregationType.MASK | VectorPainter.AggregationType.MASK2 =>
        if (column.isDefined) {
          throw new ParserException("A column name must not be specified with MASK or MASK2")
        }
      case VectorPainter.AggregationType.SUM => {
        // SUM can be used with or without a column name being specified. If used
        // with a column name, it sums the values of that column for all features
        // that intersects that pixel. Without the column, it sums the number of
        // features that intersects the pixel.
      }
      case _ =>
        // All other aggregation types require a column name
        if (column.isEmpty) {
          throw new ParserException("A column name must be specified")
        }
    }
  }

  def initialize(vector:Option[VectorMapOp], aggregator:String, cellsize:String,
                 bounds:Either[String, Option[RasterMapOp]], column:String):Unit = {
    vectorMapOp = vector
    aggregationType =
        try {
          VectorPainter.AggregationType.valueOf(aggregator.toUpperCase)
        }
        catch {
          case e:IllegalArgumentException => throw new ParserException("Aggregation type must be one of: " +
                                                                       StringUtils
                                                                           .join(VectorPainter.AggregationType.values,
                                                                             ", "))
        }

    if (aggregationType == VectorPainter.AggregationType.GAUSSIAN) {
      throw new ParserException("Invalid aggregation type for rasterize vector")
    }
    tilesize = MrGeoProperties.getInstance.getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE,
      MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT).toInt

    val cs =
      if (cellsize.endsWith("m")) {
        val meters = cellsize.replace("m", "").toDouble
        meters / LatLng.METERS_PER_DEGREE
      }
      else if (cellsize.endsWith("z")) {
        val zoom = cellsize.replace("z", "").toInt
        TMSUtils.resolution(zoom, tilesize)
      }
      else {
        if (cellsize.endsWith("d")) {
          cellsize.replace("d", "").toDouble
        }
        else {
          cellsize.toDouble
        }
      }

    zoom = TMSUtils.zoomForPixelSize(cs, tilesize)

    this.column = if (column == null || column.length == 0) {
      None
    }
    else {
      Some(column)
    }

    if (aggregationType != VectorPainter.AggregationType.SUM && this.column.isEmpty) {
      throw new ParserException("A column name must not be specified with " + aggregationType)
    }

    bounds match {
      case Left(b) => {
        if (b != null && b.length > 0) {
          this.bounds = Some(Bounds.fromCommaString(b))
        }
      }
      case Right(rasterForBoundsMapOp) => {
        this.rasterForBoundsMapOp = rasterForBoundsMapOp
      }
    }
  }

  private def parseBounds(node:ParserNode, variables:String => Option[ParserNode], startIndex:Int):Unit = {
    // Make sure there are enough child nodes
    if (node.getNumChildren < startIndex + 4) {
      throw new ParserException("Cannot define bounds from fewer than four values")
    }
    val b:Array[Double] = new Array[Double](4)
    for (i <- 0 until 4) {
      b(i) = MapOp.decodeDouble(node.getChild(startIndex + i), variables) match {
        case Some(boundsVal) => boundsVal
        case None =>
          throw new ParserException("You must provide minX, minY, maxX, maxY bounds values")
      }
    }
    bounds = Some(new Bounds(b(0), b(1), b(2), b(3)))
  }

}
