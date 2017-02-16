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

package org.mrgeo.buildpyramid

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.Properties

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.aggregators.{Aggregator, AggregatorRegistry, MeanAggregator}
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.{ImageOutputFormatContext, MrsImageDataProvider, MrsImageWriter}
import org.mrgeo.data.raster.{MrGeoRaster, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.image.{ImageStats, MrsPyramidMetadata}
import org.mrgeo.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils._
import org.mrgeo.utils.tms._

import scala.beans.BeanProperty
import scala.collection.mutable

object BuildPyramid extends MrGeoDriver with Externalizable {

  private val Pyramid = "pyramid"
  private val Aggregator = "aggregator"
  private val ProviderProperties = "provider.properties"

  @BeanProperty
  var MIN_TILES_FOR_SPARK = 1000 // made a var so the tests can muck with it...

  def build(pyramidName:String, aggregator:Aggregator, conf:Configuration,
            providerProperties:ProviderProperties):Boolean = {

    val name = "BuildPyramid"

    val args = setupArguments(pyramidName, aggregator, providerProperties)

    run(name, classOf[BuildPyramid].getName, args.toMap, conf)

    true
  }

  def buildlevel(pyramidName:String, level:Int, aggregator:Aggregator,
                 conf:Configuration, providerProperties:Properties):Boolean = {
    throw new NotImplementedException("Not yet implemented")
  }

  // this build method allows buildpyramid to be called from within an existing spark job...
  def build(pyramidName:String, aggregator:Aggregator, context:SparkContext,
            providerProperties:ProviderProperties):Boolean = {
    val bp = new BuildPyramid(pyramidName, aggregator, providerProperties)

    bp.execute(context)
  }

  override def setup(job:JobArguments):Boolean = {
    job.isMemoryIntensive = true
    true
  }

  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}

  private def setupArguments(pyramid:String, aggregator:Aggregator,
                             providerProperties:ProviderProperties):mutable.Map[String, String] = {
    val args = mutable.Map[String, String]()

    args += Pyramid -> pyramid
    args += Aggregator -> aggregator.getClass.getName

    if (providerProperties != null) {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
    }
    else {
      args += ProviderProperties -> ""
    }

    args
  }
}

class BuildPyramid extends MrGeoJob with Externalizable {

  var pyramidName:String = _
  var aggregator:Aggregator = _
  var providerproperties:ProviderProperties = _

  override def registerClasses():Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes.result()
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = {
    pyramidName = job.getSetting(BuildPyramid.Pyramid)
    val aggclass = job.getSetting(BuildPyramid.Aggregator, classOf[MeanAggregator].getName)

    makeAggregator(aggclass)

    providerproperties = ProviderProperties.fromDelimitedString(
      job.getSetting(BuildPyramid.ProviderProperties))

    true
  }

  override def execute(context:SparkContext):Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x:TileIdWritable, y:TileIdWritable):Int = x.compareTo(y)
    }

    log.warn("Building pyramid for " + pyramidName)
    val provider:MrsImageDataProvider =
      DataProviderFactory.getMrsImageDataProvider(pyramidName, AccessMode.READ, null.asInstanceOf[ProviderProperties])

    var metadata:MrsPyramidMetadata = provider.getMetadataReader.read

    val maxLevel:Int = metadata.getMaxZoomLevel

    val tilesize:Int = metadata.getTilesize

    val nodatas = metadata.getDefaultValuesDouble

    DataProviderFactory.saveProviderPropertiesToConfig(providerproperties, context.hadoopConfiguration)

    var pyramid = SparkUtils.loadMrsPyramid(provider, maxLevel, context)

    var builtlocally = false
    // build the levels
    for (level <- maxLevel until 1 by -1) {
      val fromlevel = level
      val tolevel = fromlevel - 1

      if (!builtlocally) {
        logInfo("Building pyramid for: " + pyramidName + " from: " + fromlevel + " to: " + tolevel)
        val tb = metadata.getTileBounds(fromlevel)

        // if we have less than 1000 tiles total, we'll use the local buildpyramid
        if (tb.getWidth * tb.getHeight > BuildPyramid.MIN_TILES_FOR_SPARK) {

          val decimated:RDD[(TileIdWritable, RasterWritable)] = pyramid.map(tile => {
            val fromkey = tile._1
            val fromtile:Tile = TMSUtils.tileid(fromkey.get, fromlevel)
            val frombounds:Bounds = TMSUtils.tileBounds(fromtile.tx, fromtile.ty, fromlevel, tilesize)

            val fromraster = RasterWritable.toMrGeoRaster(tile._2)

            // calculate the starting pixel for the from-tile (make sure to use the NW coordinate)
            val fromcorner:Pixel = TMSUtils.latLonToPixelsUL(frombounds.n, frombounds.w, fromlevel, tilesize)

            val totile:Tile = TMSUtils.latLonToTile(frombounds.s, frombounds.w, tolevel, tilesize)
            val tobounds:Bounds = TMSUtils.tileBounds(totile.tx, totile.ty, tolevel, tilesize)

            // calculate the starting pixel for the to-tile (make sure to use the NW coordinate) in the from-tile's pixel space
            val tocorner:Pixel = TMSUtils.latLonToPixelsUL(tobounds.n, tobounds.w, fromlevel, tilesize)

            val tokey = new TileIdWritable(TMSUtils.tileid(totile.tx, totile.ty, tolevel))

            val reduced = fromraster.reduce(2, 2, aggregator, nodatas)

            // create a compatible writable raster
            logDebug("from  tx: " + fromtile.tx + " ty: " + fromtile.ty + " (" + fromlevel + ") to tx: " + totile.tx +
                     " ty: " + totile.ty + " (" + tolevel + ") x: "
                     + ((fromcorner.px - tocorner.px) / 2) + " y: " + ((fromcorner.py - tocorner.py) / 2) +
                     " w: " + reduced.width() + " h: " + reduced.height())

            val toraster = fromraster.createCompatibleRaster(tilesize, tilesize)
            toraster.fill(nodatas)

            toraster.copyFrom(0, 0, reduced.width(), reduced.height(), reduced,
              (fromcorner.px - tocorner.px).toInt / 2, (fromcorner.py - tocorner.py).toInt / 2)

            (tokey, RasterWritable.toWritable(toraster))
          })

          val mergedTiles = new PairRDDFunctions(decimated).reduceByKey((r1, r2) => {
            val src = RasterWritable.toMrGeoRaster(r1)
            val dst = RasterWritable.toMrGeoRaster(r2)

            dst.mosaic(src, nodatas)

            RasterWritable.toWritable(dst)
          })

          // while we were running, there is chance the pyramid was removed from the cache and
          // reopened by another process. Re-loading it here will avoid some potential conflicts.
          metadata = provider.getMetadataReader.reload()

          // make sure the level is deleted
          deletelevel(tolevel, metadata, provider)

          pyramid = RasterRDD(mergedTiles)

          SparkUtils.saveMrsPyramid(pyramid, provider, tolevel,
            context.hadoopConfiguration, providerproperties = this.providerproperties)

          //TODO: Fix this in S3
          // in S3, sometimes the just-written data isn't available to read yet.  This sleep just gives
          // S3 a chance to catch up...
          //Thread.sleep(5000)
        }
        else {
          buildlevellocal(provider, pyramid, fromlevel, 1)
          builtlocally = true
        }
      }
    }

    true
  }

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = {
    true
  }

  override def readExternal(in:ObjectInput):Unit = {
    pyramidName = in.readUTF()
    val ac = in.readUTF()
    makeAggregator(ac)
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeUTF(pyramidName)
    out.writeUTF(aggregator.getClass.getName)
  }

  private[buildpyramid] def this(pyramidName:String, aggregator:Aggregator,
                                 providerProperties:ProviderProperties) = {
    this()

    this.pyramidName = pyramidName
    this.aggregator = aggregator
    this.providerproperties = providerproperties
  }

  private def makeAggregator(classname:String) = {
    val cl = getClass.getClassLoader
    val clazz = cl.loadClass(classname)

    aggregator = clazz.newInstance().asInstanceOf[Aggregator]
  }

  private def deletelevel(level:Int, metadata:MrsPyramidMetadata, provider:MrsImageDataProvider) {
    val imagedata:Array[MrsPyramidMetadata.ImageMetadata] = metadata.getImageMetadata

    // delete the level
    provider.delete(level)

    // remove the metadata for the level
    imagedata(level) = new MrsPyramidMetadata.ImageMetadata
    provider.getMetadataWriter.write()
  }

  // this method was stolen from the old Hadoop M/R version of BuildPyramid.  I really haven't looked much
  // into it to see if it really is still OK or could be improved
  @SuppressFBWarnings(value = Array("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"),
    justification = "tileIdOrdering() - false positivie")
  private def buildlevellocal(provider:MrsImageDataProvider, pyramid:RasterRDD, maxlevel:Int,
                              minlevel:Int):Boolean = {
    var inputTiles = mutable.HashMap.empty[TileIdWritable, MrGeoRaster]

    pyramid.collect.foreach(tile => {
      inputTiles.put(tile._1, RasterWritable.toMrGeoRaster(tile._2))
    })

    var metadata:MrsPyramidMetadata = provider.getMetadataReader.read
    val nodatas = metadata.getDefaultValuesDouble

    val bounds:Bounds = metadata.getBounds
    val tilesize:Int = metadata.getTilesize

    var fromlevel = maxlevel
    while (fromlevel > minlevel) {
      val tolevel = fromlevel - 1

      deletelevel(tolevel, metadata, provider)

      val outputTiles = mutable.HashMap.empty[TileIdWritable, MrGeoRaster]

      inputTiles.foreach(tile => {
        val fromkey = tile._1
        val fromtile:Tile = TMSUtils.tileid(fromkey.get, fromlevel)
        val frombounds:Bounds = TMSUtils.tileBounds(fromtile.tx, fromtile.ty, fromlevel, tilesize)

        val fromraster = tile._2

        // calculate the starting pixel for the from-tile (make sure to use the NW coordinate)
        val fromcorner:Pixel = TMSUtils.latLonToPixelsUL(frombounds.n, frombounds.w, fromlevel, tilesize)

        val totile:Tile = TMSUtils.latLonToTile(frombounds.s, frombounds.w, tolevel, tilesize)
        val tobounds:Bounds = TMSUtils.tileBounds(totile.tx, totile.ty, tolevel, tilesize)

        // calculate the starting pixel for the to-tile (make sure to use the NW coordinate)
        // in the from-tile's pixel space
        val tocorner:Pixel = TMSUtils.latLonToPixelsUL(tobounds.n, tobounds.w, fromlevel, tilesize)

        val tokey = new TileIdWritable(TMSUtils.tileid(totile.tx, totile.ty, tolevel))

        val reduced = fromraster.reduce(2, 2, aggregator, nodatas)

        // create a compatible writable raster
        logDebug("from  tx: " + fromtile.tx + " ty: " + fromtile.ty + " (" + fromlevel + ") to tx: " + totile.tx +
                 " ty: " + totile.ty + " (" + tolevel + ") x: "
                 + ((fromcorner.px - tocorner.px) / 2) + " y: " + ((fromcorner.py - tocorner.py) / 2) +
                 " w: " + reduced.width() + " h: " + reduced.height())

        val toraster = if (!outputTiles.contains(tokey)) {
          val raster = fromraster.createCompatibleRaster(tilesize, tilesize)
          raster.fill(nodatas)

          outputTiles.put(tokey, raster)
          raster
        }
        else {
          outputTiles(tokey)
        }

        toraster.copyFrom(0, 0, reduced.width(), reduced.height(), reduced,
          (fromcorner.px - tocorner.px).toInt / 2, (fromcorner.py - tocorner.py).toInt / 2)
      })

      val stats:Array[ImageStats] = ImageStats.initializeStatsArray(metadata.getBands)

      log.debug("Writing output file: " + provider.getResourceName + " level: " + tolevel)

      val writer:MrsImageWriter = provider.getMrsTileWriter(tolevel, metadata.getProtectionLevel)

      outputTiles.toSeq.sortBy(_._1.get()).foreach(tile => {
        logDebug("  writing tile: " + tile._1.get)
        writer.append(tile._1, tile._2)
        ImageStats.computeAndUpdateStats(stats, tile._2, nodatas)
      })
      writer.close()

      val tb:TileBounds = TMSUtils.boundsToTile(bounds, tolevel, tilesize)
      val b:LongRectangle = new LongRectangle(tb.w, tb.s, tb.e, tb.n)
      val psw:Pixel = TMSUtils.latLonToPixels(bounds.s, bounds.w, tolevel, tilesize)
      val pne:Pixel = TMSUtils.latLonToPixels(bounds.n, bounds.e, tolevel, tilesize)

      metadata = provider.getMetadataReader.reload()

      metadata.setPixelBounds(tolevel, new LongRectangle(0, 0, pne.px - psw.px, pne.py - psw.py))
      metadata.setTileBounds(tolevel, b)
      metadata.setName(tolevel)
      metadata.setImageStats(tolevel, stats)

      if (tolevel == metadata.getMaxZoomLevel) {
        metadata.setStats(stats)
      }
      metadata.setResamplingMethod(AggregatorRegistry.aggregatorRegistry.inverse.get(aggregator.getClass))

      provider.getMetadataWriter(null).write()

      val tofc = new ImageOutputFormatContext(provider.getResourceName, bounds, tolevel,
        tilesize, metadata.getProtectionLevel)
      val tofp = provider.getTiledOutputFormatProvider(tofc)
      // Don't use teardownForSpark because we built this level in locally, so we want
      // to avoid using Spark at this point
      tofp.finalizeExternalSave(HadoopUtils.createConfiguration())

      inputTiles = outputTiles

      fromlevel -= 1
    }

    true
  }
}
