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
import org.apache.hadoop.fs.Path
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.vector.FeatureIdWritable
import org.mrgeo.geometry.{Geometry, GeometryFactory}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.mapalgebra.parser.ParserNode
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.mapalgebra.vector.VectorMapOp
import org.mrgeo.mapalgebra.vector.paint.VectorPainter
import org.mrgeo.utils.GeometryUtils
import org.mrgeo.utils.tms.{Bounds, TMSUtils, TileBounds}

import scala.collection.mutable.ListBuffer

object RasterizeVectorMapOp extends MapOpRegistrar {

  private val MAX_TILES_PER_FEATURE = 1000

  override def register:Array[String] = {
    Array[String]("rasterizevector", "rasterize")
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new RasterizeVectorMapOp(node, variables)

  def create(vector:VectorMapOp, aggregator:String, cellsize:String, column:String = null):RasterizeVectorMapOp = {
    new RasterizeVectorMapOp(Some(vector), aggregator, cellsize, column, null.asInstanceOf[String])
  }
}


class RasterizeVectorMapOp extends AbstractRasterizeVectorMapOp with Externalizable {

  def this(vector:Option[VectorMapOp], aggregator:String, cellsize:String, column:String, bounds:String) = {
    this()

    initialize(vector, aggregator, cellsize, Left(bounds), column)
  }

  def this(vector:Option[VectorMapOp], aggregator:String, cellsize:String, column:String,
           rasterForBounds:Option[RasterMapOp]) = {
    this()

    initialize(vector, aggregator, cellsize, Right(rasterForBounds), column)
  }


  def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    initialize(node, variables)
  }

  override def registerClasses():Array[Class[_]] = {
    GeometryFactory.getClasses ++ Array[Class[_]](classOf[FeatureIdWritable])
  }

  override def rasterize(vectorRDD:VectorRDD):RDD[(TileIdWritable, RasterWritable)] = {
    val tiledVectors = vectorsToTiledRDD(vectorRDD)
    val localRdd = new PairRDDFunctions(tiledVectors)
    val groupedGeometries = localRdd.groupByKey()

    rasterize(groupedGeometries)
  }

  def rasterize(groupedGeometries:RDD[(TileIdWritable, Iterable[Geometry])]):RDD[(TileIdWritable, RasterWritable)] = {
    val result = groupedGeometries.map(U => {
      val tileId = U._1
      val rvp = new VectorPainter(zoom,
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
    result
  }

  def splitFeature(fid:FeatureIdWritable, geom:Geometry, maxfeaturesize:Double):
  TraversableOnce[(FeatureIdWritable, Geometry)] = {
    var result = new ListBuffer[(FeatureIdWritable, Geometry)]

    try {
      if (geom != null) {
        val bounds:Bounds = geom.getBounds
        if (bounds.width() * bounds.height() > maxfeaturesize) {
          val center = bounds.center()

          val ul = new Bounds(bounds.w, center.lat, center.lon, bounds.n)
          val ll = new Bounds(bounds.w, bounds.s, center.lon, center.lat)
          val ur = new Bounds(center.lon, center.lat, bounds.e, bounds.n)
          val lr = new Bounds(center.lon, bounds.s, bounds.e, center.lat)

          var r = new ListBuffer[(FeatureIdWritable, Geometry)]

          // JTS could exception with a "TopologyException: side location conflict" if the feature comes too close
          // to the clip bounds.  If it does, we can only add the entire feature to the result...
          try {
            val gul = geom.clip(ul)
            if (gul == null) throw new Exception("Bad Clip")
            r ++= splitFeature(fid, gul, maxfeaturesize)

            val gll = geom.clip(ll)
            if (gll == null) throw new Exception("Bad Clip")
            r ++= splitFeature(fid, gll, maxfeaturesize)

            val gur = geom.clip(ur)
            if (gur == null) throw new Exception("Bad Clip")
            r ++= splitFeature(fid, gur, maxfeaturesize)

            val glr = geom.clip(lr)
            if (glr == null) throw new Exception("Bad Clip")
            r ++= splitFeature(fid, glr, maxfeaturesize)

            result ++= r
          }
          catch {
            case _:Throwable => result += ((fid, geom))
          }
        }
        else {
          result += ((fid, geom))
        }
      }
    }
    catch {
      case _:Throwable =>
        result.clear()
        result += ((fid, geom))
    }
    result
  }

  /**
    * This method iterates through each of the features in the vectorRDD input and
    * returns a new RDD of TileIdWritable and Geometry tuples. The idea is that for
    * each feature, it identifies which tiles that feature intersects and then adds
    * a tuple to the resulting RDD for each of this tiles paired with that feature.
    * For example, if a feature intersects 5 tiles, then it adds 5 records for that
    * feature to the returned RDD.
    */
  def vectorsToTiledRDD(vectorRDD:VectorRDD):RDD[(TileIdWritable, Geometry)] = {
    val sizeAccumulator = vectorRDD.context.accumulator(0)(MaxSizeAccumulator)

    val singletile = TMSUtils.tileBounds(0, 0, zoom, tilesize)
    val maxfeaturesize = singletile.width() * singletile.height() * RasterizeVectorMapOp.MAX_TILES_PER_FEATURE
    val splitfeatures = vectorRDD.flatMap(feature => {
      var result = new ListBuffer[(FeatureIdWritable, Geometry)]

      var maxsize:Int = 0
      val geom = feature._2

      val process = bounds match {
        case Some(filterBounds) =>
          if (filterBounds.intersects(geom.getBounds)) {
            true
          }
          else {
            false
          }
        case None => true
      }

      if (process) {
        // make sure each geometry isn't too "large".  Split it in 4 peices if it is
        splitFeature(feature._1, geom, maxfeaturesize).foreach(feature => {
          val baos = new ByteArrayOutputStream(1024)
          val dos = new DataOutputStream(baos)
          try {

            feature._2.write(dos)
            val size = baos.size()
            if (size > maxsize) {
              maxsize = size
            }
          }
          finally {
            dos.close()
          }

          result += feature
        })
      }

      sizeAccumulator.add(maxsize)
      result
    })


    // An individual partition cannot serialize to more than Int.MaxValue bytes
    // because Spark cannot to load the partition into memory, and it results in
    // java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE
    // To prevent that, we re-partition based on the maximum serialized size of
    // the input geometries. This is not ideal because there will likely be a lot
    // of partially filled partitions

    // Need to materialize the RDD in order for the accumulator to compute the
    // max geometry size.

    val splitfeaturescnt = splitfeatures.count()
    val maxSize = sizeAccumulator.value
    log.info("Max geometry serialized size is " + maxSize)
    // The divide by 2 is not scientific. It simply doubles the number of partitions
    // which, during testing, significatnly improved performance.
    //val geomsPerPartition = Integer.MAX_VALUE / maxSize / 8

    val path = new Path("/")
    val fs = HadoopFileUtils.getFileSystem(path)
    val blocksize = fs.getDefaultBlockSize(path)

    val geomsPerPartition = blocksize / maxSize

    log.info("Can fit " + geomsPerPartition + " geometries in a partition")
    val splitpartitions = (splitfeaturescnt / geomsPerPartition).toInt + 1
    log.info("Using " + splitpartitions + " partitions for RasterizeVector")


    val tilefeatures = splitfeatures.repartition(splitpartitions).flatMap(U => {
      val geom = U._2
      var result = new ListBuffer[(TileIdWritable, Geometry)]

      val tiles:List[TileIdWritable] = getOverlappingTiles(zoom, tilesize, geom.getBounds)
      for (tileId <- tiles) {
        // make sure the geometry actually intersects this tile
        val t = TMSUtils.tileid(tileId.get(), zoom)
        val tb = GeometryUtils.toPoly(TMSUtils.tileBounds(t, zoom, tilesize))
        if (GeometryUtils.intersects(tb, geom)) {
          result += ((tileId, geom))
        }
      }

      result
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val count = tilefeatures.count()
    // The divide by 2 is not scientific. It simply doubles the number of partitions
    // which, during testing, significatnly improved performance.
    val partitions = (count / geomsPerPartition).toInt + 1
    log.info("Using " + partitions + " partitions for RasterizeVector")

    try {
      tilefeatures.repartition(partitions)
    }
    finally {
      tilefeatures.unpersist()
    }
  }

  def getOverlappingTiles(zoom:Int, tileSize:Int, bounds:Bounds):List[TileIdWritable] = {
    var tiles = new ListBuffer[TileIdWritable]
    val tb:TileBounds = TMSUtils.boundsToTile(bounds, zoom, tileSize)
    var tx:Long = tb.w
    while (tx <= tb.e) {
      var ty:Long = tb.s
      while (ty <= tb.n) {
        val tile:TileIdWritable = new TileIdWritable(TMSUtils.tileid(tx, ty, zoom))
        tiles += tile
        ty += 1
      }
      tx += 1
    }
    tiles.toList
  }

  @SuppressFBWarnings(value = Array("NM_FIELD_NAMING_CONVENTION"), justification = "Scala generated code")
  object MaxSizeAccumulator extends AccumulatorParam[Int] {
    override def addInPlace(r1:Int, r2:Int):Int = {
      Math.max(r1, r2)
    }

    override def zero(initialValue:Int):Int = {
      initialValue
    }
  }

}
