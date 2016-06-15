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

package org.mrgeo.mapalgebra

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.{MrsPyramidMapOp, RasterMapOp}
import org.mrgeo.utils.MrGeoImplicits._
import org.mrgeo.utils.SparkUtils
import org.mrgeo.utils.tms.{Bounds, TMSUtils, TileBounds}

object CropMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("crop")
  }

  def create(raster:RasterMapOp, w:Double, s:Double, e:Double, n:Double):MapOp =
    new CropMapOp(Some(raster), w, s, e, n)

  def create(raster:RasterMapOp, rasterForBounds: RasterMapOp):MapOp =
    new CropMapOp(Some(raster), Some(rasterForBounds))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new CropMapOp(node, variables)
}


class CropMapOp extends RasterMapOp with Externalizable {
  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  protected var rasterForBoundsMapOp: Option[RasterMapOp] = None
  protected var bounds:TileBounds = null
  protected var cropBounds:Bounds = null
  private var doFiltering = true

  private[mapalgebra] def this(raster:Option[RasterMapOp], w:Double, s:Double, e:Double, n:Double) = {
    this()

    cropBounds = new Bounds(w, s, e, n)
    setInputMapOp(raster)
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], rasterForBounds: Option[RasterMapOp]) = {
    this()

    rasterForBoundsMapOp = rasterForBounds
    setInputMapOp(raster)
  }

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 5 && node.getNumChildren != 2) {
      throw new ParserException("Usage: crop(raster, w, s, e, n) or crop(raster, rasterForBounds)")
    }
    parseChildren(node, variables)
  }

  protected def parseChildren(node: ParserNode, variables: String => Option[ParserNode]) = {
    if (node.getNumChildren == 2) {
      rasterForBoundsMapOp = RasterMapOp.decodeToRaster(node.getChild(1), variables)
    }
    else {
      cropBounds = new Bounds(MapOp.decodeDouble(node.getChild(1), variables).get,
        MapOp.decodeDouble(node.getChild(2), variables).get,
        MapOp.decodeDouble(node.getChild(3), variables).get,
        MapOp.decodeDouble(node.getChild(4), variables).get)
    }
    setInputMapOp(RasterMapOp.decodeToRaster(node.getChild(0), variables))
  }

  /**
    * Checks to see if the raster input to the crop is a MrsPyramidInputFormat. If so,
    * it takes advantage of the MrsPyramidMapOp's ability to directly filter the
    * input image to bounds, which is significantly faster than filtering all of
    * the image's tiles using the Spark filter function. This is because it limits
    * the number of splits and tiles while the data is being read rather than having
    * to push the data through the system to be filtered out during processing.
    *
    * Because MrsPyramidMapOps can be shared (when the same input source is referenced
    * in multiple places in the map algebra), we clone the input map op before
    * setting the bounds information into it. This prevents affecting any other places
    * where the same input source is used without the crop.
    */
  protected def setInputMapOp(inputMapOp: Option[RasterMapOp]): Unit = {
    inputMapOp match {
      case Some(op) => {
        op match {
          case op: MrsPyramidMapOp => {
            rasterForBoundsMapOp match {
              case Some(bmo) => {
                val clonedOp = op.clone
                clonedOp.asInstanceOf[MrsPyramidMapOp].setBounds(bmo)
                this.inputMapOp = Some(clonedOp)
                doFiltering = false
              }
              case None => {
                if (cropBounds != null) {
                  val clonedOp = op.clone
                  clonedOp.asInstanceOf[MrsPyramidMapOp].setBounds(cropBounds)
                  this.inputMapOp = Some(clonedOp)
                  doFiltering = false
                }
              }
            }
          }
          case _ => this.inputMapOp = inputMapOp
        }
      }
      case _ => this.inputMapOp = inputMapOp
    }
  }

  override def context(cont: SparkContext) = {
    super.context(cont)
    this.inputMapOp match {
      case Some(op) => {
        op.context(cont)
      }
      case None => {}
    }
  }

  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = true

  override def execute(context: SparkContext): Boolean = {

    val input: RasterMapOp = inputMapOp getOrElse (throw new IOException("Input MapOp not valid!"))

    // If we had to clone the input map op, then the Spark context will not be
    // set in the clone. We need to set the context before accessing the input RDD
    if (!doFiltering) {
      input.context(context)
    }
    val meta = input.metadata() getOrElse
        (throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))
    val rdd = input.rdd() getOrElse (throw new IOException("Can't load RDD! Ouch! " + inputMapOp.getClass.getName))

    val zoom = meta.getMaxZoomLevel
    val tilesize = meta.getTilesize

    if (rasterForBoundsMapOp.isDefined) {
      cropBounds = rasterForBoundsMapOp.get.metadata().getOrElse(
        throw new IOException("Unable to get metadata for the bounds raster")).getBounds
    }
    bounds = TMSUtils.boundsToTile(cropBounds, zoom, tilesize)
    val nodatas = meta.getDefaultValues

    // When doFiltering is true, it means the input raster has not been restricted to
    // the bounds for the crop, so we need to do that filtering here. When false, it
    // means that the input raster RDD already contains only tiles within the bounds
    // of the crop, so no filtering is required.
    if (doFiltering) {
      //    val filtered = rdd.filter(tile => {
      //      val t = TMSUtils.tileid(tile._1.get(), zoom)
      //      (t.tx >= bounds.w) && (t.tx <= bounds.e) && (t.ty >= bounds.s) && (t.ty <= bounds.n)
      //    })

      rasterRDD = Some(RasterRDD(
        rdd.flatMap(tile => {
          val t = TMSUtils.tileid(tile._1.get(), zoom)
          if ((t.tx >= bounds.w) && (t.tx <= bounds.e) && (t.ty >= bounds.s) && (t.ty <= bounds.n)) {
            Array(processTile(tile, zoom, tilesize, nodatas)).iterator
          }
          else {
            Array.empty[(TileIdWritable, RasterWritable)].iterator
          }
        })
      ))
    }
    else {
      rasterRDD = Some(RasterRDD(processAllTiles(rdd, zoom, tilesize, nodatas)))
    }

    val b = getOutputBounds(zoom, tilesize)

    metadata(SparkUtils.calculateMetadata(rasterRDD.get, zoom, meta.getDefaultValues,
      bounds = b, calcStats = false))

    true
  }

  protected def getOutputBounds(zoom: Int, tilesize: Int): Bounds = {
    // Use the bounds of the tiles processed
    TMSUtils.tileToBounds(bounds, zoom, tilesize)
  }

  protected def processTile(tile: (TileIdWritable, RasterWritable),
                            zoom: Int,
                            tilesize: Int,
                            nodatas: Array[Double]): (TileIdWritable, RasterWritable) = {
    tile
  }

  /**
    * This function is called when all of the tiles from the input raster RDD
    * are part of the area being cropped. For the basic crop operation
    * implemented by this map op, no processing is required on the tiles, we
    * can simply return the input raster RDD.
    *
    * However, this method is protected so that more complicated crop operations
    * can extend this class and perform processing on the individual tiles in
    * within the bounds of the crop area if needed.
    */
  protected def processAllTiles(inputRDD: RasterRDD,
                                zoom: Int,
                                tilesize: Int,
                                nodats: Array[Double]): RDD[(TileIdWritable, RasterWritable)] = {
    inputRDD
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
    bounds = TileBounds.fromCommaString(in.readUTF())
    cropBounds = Bounds.fromCommaString(in.readUTF())
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(bounds.toCommaString)
    out.writeUTF(cropBounds.toCommaString)
  }
}

