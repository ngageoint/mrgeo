/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.buildpyramid

import java.awt.image.{Raster, WritableRaster}
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.aggregators.{Aggregator, AggregatorRegistry, MeanAggregator}
import org.mrgeo.data
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.tile.{MrsTileReader, MrsTileWriter, TileIdWritable}
import org.mrgeo.data.{ProviderProperties, CloseableKVIterator, DataProviderFactory, KVIterator}
import org.mrgeo.image.{ImageStats, MrsImagePyramid, MrsImagePyramidMetadata}
import org.mrgeo.mapreduce.job.JobListener
import org.mrgeo.progress.Progress
import org.mrgeo.spark.job.{JobArguments, MrGeoDriver, MrGeoJob}
import org.mrgeo.utils.{Bounds, LongRectangle, SparkUtils, TMSUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable

object BuildPyramidSpark extends MrGeoDriver with Externalizable {

  final private val Pyramid = "pyramid"
  final private val Aggregator = "aggregator"
  final private val ProviderProperties = "provider.properties"

  def build(pyramidName: String, aggregator: Aggregator, conf: Configuration,
      providerProperties: ProviderProperties):Boolean = {

    val name = "BuildPyramid"

    val args = setupArguments(pyramidName, aggregator, providerProperties)

    run(name, classOf[BuildPyramidSpark].getName, args.toMap, conf)

    true
  }

  def build (pyramidName: String, aggregator: Aggregator,
      conf: Configuration, progress: Progress, jobListener: JobListener, providerProperties: ProviderProperties): Boolean = {
    build(pyramidName, aggregator, conf, providerProperties)
  }

  def buildlevel (pyramidName: String, level: Int, aggregator: Aggregator,
      conf: Configuration, providerProperties: Properties):Boolean = {
    throw new NotImplementedError("Not yet implemented")
  }

  def buildlevel (pyramidName: String, level: Int, aggregator: Aggregator,
      conf: Configuration, progress: Progress, jobListener: JobListener,
      providerProperties: ProviderProperties): Boolean = {
    throw new NotImplementedError("Not yet implemented")
  }

  private def setupArguments(pyramid: String, aggregator: Aggregator, providerProperties: ProviderProperties):mutable.Map[String, String] = {
    val args = mutable.Map[String, String]()

    args += Pyramid -> pyramid
    args += Aggregator -> aggregator.getClass.getName

    if (providerProperties != null)
    {
      args += ProviderProperties -> data.ProviderProperties.toDelimitedString(providerProperties)
        }
    else
    {
      args += ProviderProperties -> ""
    }

    args
  }


  override def setup(job: JobArguments): Boolean = {
    job.isMemoryIntensive = true
    true
  }

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}


class BuildPyramidSpark extends MrGeoJob with Externalizable {

  var pyramidName:String = null
  var aggregator:Aggregator = null
  var providerproperties:ProviderProperties = null

  override def registerClasses(): Array[Class[_]] = {
    val classes = Array.newBuilder[Class[_]]

    classes.result()
  }

  private def makeAggregator(classname: String) = {
    val cl = getClass.getClassLoader
    val clazz = cl.loadClass(classname)

    aggregator = clazz.newInstance().asInstanceOf[Aggregator]
  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    pyramidName = job.getSetting(BuildPyramidSpark.Pyramid)
    val aggclass = job.getSetting(BuildPyramidSpark.Aggregator, classOf[MeanAggregator].getName)

    makeAggregator(aggclass)

    providerproperties = ProviderProperties.fromDelimitedString(
      job.getSetting(BuildPyramidSpark.ProviderProperties))

    true
  }

  override def execute(context: SparkContext): Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    log.warn("Building pyramid for " + pyramidName);
    val provider: MrsImageDataProvider =
      DataProviderFactory.getMrsImageDataProvider(pyramidName, AccessMode.READ, null.asInstanceOf[ProviderProperties])

    var metadata: MrsImagePyramidMetadata = provider.getMetadataReader.read

    val maxLevel: Int = metadata.getMaxZoomLevel

    val tilesize: Int = metadata.getTilesize

    val nodatas:Array[Number] = Array.ofDim[Number](metadata.getBands)
    for (i <- nodatas.indices) {
      nodatas(i) = metadata.getDefaultValueDouble(i)
    }

    DataProviderFactory.saveProviderPropertiesToConfig(providerproperties, context.hadoopConfiguration)
    // build the levels
    for (level <- maxLevel until 1 by -1) {
      val fromlevel = level
      val tolevel = fromlevel - 1

      logInfo("Building pyramid for: " + pyramidName + " from: " + fromlevel + " to: " + tolevel)
      val tb = metadata.getTileBounds(fromlevel)

      // if we have less than 1000 tiles total, we'll use the local buildpyramid
      if (tb.getWidth * tb.getHeight > 1000) {
        val pyramid = SparkUtils.loadMrsPyramid(provider, fromlevel, context)

        val decimated: RDD[(TileIdWritable, RasterWritable)] = pyramid.map(tile => {
          val fromkey = tile._1
          val fromraster = RasterWritable.toRaster(tile._2)

          val fromtile: TMSUtils.Tile = TMSUtils.tileid(fromkey.get, fromlevel)
          val frombounds: TMSUtils.Bounds = TMSUtils.tileBounds(fromtile.tx, fromtile.ty, fromlevel, tilesize)

          // calculate the starting pixel for the from-tile (make sure to use the NW coordinate)
          val fromcorner: TMSUtils.Pixel = TMSUtils.latLonToPixelsUL(frombounds.n, frombounds.w, fromlevel, tilesize)

          val totile: TMSUtils.Tile = TMSUtils.latLonToTile(frombounds.s, frombounds.w, tolevel, tilesize)
          val tobounds: TMSUtils.Bounds = TMSUtils.tileBounds(totile.tx, totile.ty, tolevel, tilesize)

          // calculate the starting pixel for the to-tile (make sure to use the NW coordinate) in the from-tile's pixel space
          val tocorner: TMSUtils.Pixel = TMSUtils.latLonToPixelsUL(tobounds.n, tobounds.w, fromlevel, tilesize)

          val tokey = new TileIdWritable(TMSUtils.tileid(totile.tx, totile.ty, tolevel))

          // create a compatible writable raster
          val toraster: WritableRaster =
            RasterUtils.createCompatibleEmptyRaster(fromraster, tilesize, tilesize, nodatas)

          logDebug("from  tx: " + fromtile.tx + " ty: " + fromtile.ty + " (" + fromlevel + ") to tx: " + totile.tx +
              " ty: " + totile.ty + " (" + tolevel + ") x: "
              + ((fromcorner.px - tocorner.px) / 2) + " y: " + ((fromcorner.py - tocorner.py) / 2) +
              " w: " + fromraster.getWidth + " h: " + fromraster.getHeight)

          RasterUtils.decimate(fromraster, toraster,
            (fromcorner.px - tocorner.px).toInt / 2, (fromcorner.py - tocorner.py).toInt / 2,
            aggregator, nodatas)

          (tokey, RasterWritable.toWritable(toraster))
        })


        val tileBounds = TMSUtils.boundsToTile(metadata.getBounds.getTMSBounds, tolevel, tilesize)

        val wrappedDecimated = new PairRDDFunctions(decimated)
        val mergedTiles = wrappedDecimated.reduceByKey((r1, r2) => {
          val src = RasterWritable.toRaster(r1)
          val dst = RasterUtils.makeRasterWritable(RasterWritable.toRaster(r2))

          RasterUtils.mosaicTile(src, dst, metadata.getDefaultValues)

          RasterWritable.toWritable(dst)
        })

        // while we were running, there is chance the pyramid was removed from the cache and
        // reopened by another process. Re-opening it here will avoid some potential conflicts.
        metadata = MrsImagePyramid.open(provider).getMetadata

        // make sure the level is deleted
        deletelevel(tolevel, metadata, provider)

        SparkUtils.saveMrsPyramid(mergedTiles, provider, tolevel,
          context.hadoopConfiguration, providerproperties = this.providerproperties)

        //TODO: Fix this in S3
        // in S3, sometimes the just-written data isn't available to read yet.  This sleep just gives
        // S3 a chance to catch up...
        Thread.sleep(5000)
      }
      else {
        buildlevellocal(provider, fromlevel)
      }
    }

    true
  }

  private def deletelevel(level: Int, metadata: MrsImagePyramidMetadata, provider: MrsImageDataProvider) {
    val imagedata: Array[MrsImagePyramidMetadata.ImageMetadata] = metadata.getImageMetadata

    // delete the level
    provider.delete(level)

    // remove the metadata for the level
    imagedata(level) = new MrsImagePyramidMetadata.ImageMetadata
    provider.getMetadataWriter.write()
  }

  // this method was stolen from the old Hadoop M/R version of BuildPyramid.  I really haven't looked much
  // into it to see if it really is still OK or could be improved
  private def buildlevellocal(provider:MrsImageDataProvider, inputLevel: Int): Boolean = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    var metadata: MrsImagePyramidMetadata = provider.getMetadataReader.read

    val bounds: Bounds = metadata.getBounds
    val tilesize: Int = metadata.getTilesize
    val outputLevel: Int = inputLevel - 1

    deletelevel(outputLevel, metadata, provider)

    val outputTiles: util.TreeMap[TileIdWritable, WritableRaster] = new util.TreeMap[TileIdWritable, WritableRaster]()

    val lastImage: MrsTileReader[Raster] = provider.getMrsTileReader(inputLevel)
    val iter: KVIterator[TileIdWritable, Raster] = lastImage.get
    while (iter.hasNext) {
      val fromraster: Raster = iter.next

      val tileid: Long = iter.currentKey.get
      val inputTile: TMSUtils.Tile = TMSUtils.tileid(tileid, inputLevel)

      val toraster: WritableRaster = fromraster.createCompatibleWritableRaster(tilesize / 2, tilesize / 2)

      RasterUtils.decimate(fromraster, toraster, aggregator, metadata)

      val outputTile: TMSUtils.Tile = TMSUtils.calculateTile(inputTile, inputLevel, outputLevel, tilesize)
      val outputkey: TileIdWritable = new TileIdWritable(TMSUtils.tileid(outputTile.tx, outputTile.ty, outputLevel))
      var outputRaster: WritableRaster = null

      if (!outputTiles.containsKey(outputkey)) {
        outputRaster = fromraster.createCompatibleWritableRaster(tilesize, tilesize)
        RasterUtils.fillWithNodata(outputRaster, metadata)
        outputTiles.put(outputkey, outputRaster)
      }
      else {
        outputRaster = outputTiles(outputkey)
      }

      val outputBounds: TMSUtils.Bounds = TMSUtils.tileBounds(outputTile.tx, outputTile.ty, outputLevel, tilesize)
      val corner: TMSUtils.Pixel = TMSUtils.latLonToPixelsUL(outputBounds.n, outputBounds.w, outputLevel, tilesize)
      val inputBounds: TMSUtils.Bounds = TMSUtils.tileBounds(inputTile.tx, inputTile.ty, inputLevel, tilesize)
      val start: TMSUtils.Pixel = TMSUtils.latLonToPixelsUL(inputBounds.n, inputBounds.w, outputLevel, tilesize)
      val tox: Int = (start.px - corner.px).toInt
      val toy: Int = (start.py - corner.py).toInt
      logDebug(
        "Calculating tile from  tx: " + inputTile.tx + " ty: " + inputTile.ty + " (" + inputLevel + ") to tx: " +
            outputTile.tx + " ty: " + outputTile.ty + " (" + outputLevel + ") x: " + tox + " y: " + toy)
      outputRaster.setDataElements(tox, toy, toraster)
    }

    iter match {
    case value: CloseableKVIterator[_, _] =>
      try {
        value.close()
      }
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
    case _ =>
    }


    val stats: Array[ImageStats] = ImageStats.initializeStatsArray(metadata.getBands)
    log.debug("Writing output file: " + provider.getResourceName + " level: " + outputLevel)
    val writer: MrsTileWriter[Raster] = provider.getMrsTileWriter(outputLevel, metadata.getProtectionLevel)
    import scala.collection.JavaConversions._
    for (tile <- outputTiles.entrySet) {
      logDebug("  writing tile: " + tile.getKey.get)
      writer.append(tile.getKey, tile.getValue)
      ImageStats.computeAndUpdateStats(stats, tile.getValue, metadata.getDefaultValues)
    }
    writer.close()
    val tb: TMSUtils.TileBounds = TMSUtils
        .boundsToTile(new TMSUtils.Bounds(bounds.getMinX, bounds.getMinY, bounds.getMaxX, bounds.getMaxY),
          outputLevel, tilesize)
    val b: LongRectangle = new LongRectangle(tb.w, tb.s, tb.e, tb.n)
    val psw: TMSUtils.Pixel = TMSUtils.latLonToPixels(bounds.getMinY, bounds.getMinX, outputLevel, tilesize)
    val pne: TMSUtils.Pixel = TMSUtils.latLonToPixels(bounds.getMaxY, bounds.getMaxX, outputLevel, tilesize)


    // while we were running, there is chance the pyramid was removed from the cache and
    // reopened by another process. Re-opening it here will avoid some potential conflicts.
    metadata = MrsImagePyramid.open(provider).getMetadata

    metadata.setPixelBounds(outputLevel, new LongRectangle(0, 0, pne.px - psw.px, pne.py - psw.py))
    metadata.setTileBounds(outputLevel, b)
    metadata.setName(outputLevel)
    metadata.setImageStats(outputLevel, stats)
    metadata.setResamplingMethod(AggregatorRegistry.aggregatorRegistry.inverse.get(aggregator.getClass))

    lastImage.close()
    provider.getMetadataWriter(null).write()

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    pyramidName = in.readUTF()
    val ac = in.readUTF()
    makeAggregator(ac)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(pyramidName)
    out.writeUTF(aggregator.getClass.getName)
  }
}
