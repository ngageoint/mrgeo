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
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.SparkConf
import org.mrgeo.data.raster.MrGeoRaster
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.LatLng
import org.mrgeo.utils.tms.TMSUtils

object FocalStatMapOp extends MapOpRegistrar {

  val Count:String = "count"
  val Max:String = "max"
  val Min:String = "min"
  val Mean:String = "mean"
  val Median:String = "median"
  val Range:String = "range"
  val StdDev:String = "stddev"
  val Sum:String = "sum"
  val Variance:String = "variance"

  def create(raster:RasterMapOp, stat:String, neighborhoodSize:String, ignoreNoData:Boolean):MapOp =
    new FocalStatMapOp(Some(raster), stat, neighborhoodSize, ignoreNoData)

  override def register:Array[String] = {
    Array[String]("focalstat")
  }

  override def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp =
    new FocalStatMapOp(node, variables)
}

class FocalStatMapOp extends RawFocalMapOp with Externalizable {
  private var stat:String = _
  private var neighborhoodSize:String = _
  private var neighborhoodPixels:Int = 0
  private var ignoreNoData:Boolean = false
  private var outputTileType:Option[Int] = None
  private var outputNoDatas:Option[Array[Double]] = None
  private var neighborhoodValues:Array[Double] = _

  override def registerClasses():Array[Class[_]] = {
    Array[Class[_]](classOf[Array[Float]])
  }

  def isNodata(noDataValue:Double, value:Double):Boolean = {
    if (noDataValue.isNaN) {
      value.isNaN
    }
    else {
      value == noDataValue
    }
  }

  override def beforeExecute(meta:MrsPyramidMetadata):Unit = {
    // Make sure that neighborhood values is re-initialized at the start of map op execution
    neighborhoodValues = null
    neighborhoodPixels = neighborhoodSize match {
      case ns if ns.endsWith("p") => ns.dropRight(1).toInt
      case ns if ns.endsWith("m") =>
        val degPerPixel = TMSUtils.resolution(meta.getMaxZoomLevel, meta.getTilesize)
        val sizeInMeters = ns.dropRight(1).toFloat
        val metersPerPixel = degPerPixel * LatLng.METERS_PER_DEGREE
        (sizeInMeters / metersPerPixel).ceil.toInt
      case _ => throw new IllegalArgumentException(
        "Invalid value for neighborhood size. Must specifiy either meters (e.g. 300m) or pixels (e.g. 10p)")
    }
    stat.toLowerCase match {
      case FocalStatMapOp.Count =>
        outputTileType = Some(DataBuffer.TYPE_INT)
        outputNoDatas = Some(Array.fill[Double](meta.getBands)(Int.MinValue))
      case FocalStatMapOp.Max | FocalStatMapOp.Min | FocalStatMapOp.Range =>
        outputTileType = Some(meta.getTileType)
        val nodatas = meta.getDefaultValuesNumber
        outputNoDatas = Some(nodatas)
      case FocalStatMapOp.Mean | FocalStatMapOp.Median | FocalStatMapOp.StdDev |
           FocalStatMapOp.Sum | FocalStatMapOp.Variance =>
        outputTileType = Some(DataBuffer.TYPE_FLOAT)
        outputNoDatas = Some(Array.fill[Double](meta.getBands)(Float.NaN))
    }
  }

  override def getNeighborhoodInfo:(Int, Int) = {
    (this.neighborhoodPixels, this.neighborhoodPixels)
  }

  override def computePixelValue(raster:MrGeoRaster, notnodata:MrGeoRaster,
                                 outNoData:Double, rasterWidth:Int,
                                 processX:Int, processY:Int, processBand:Int,
                                 xLeftOffset:Int, neighborhoodWidth:Int,
                                 yAboveOffset:Int, neighborhoodHeight:Int, tileId:Long):Double = {
    var x:Int = processX - xLeftOffset
    val maxX = x + neighborhoodWidth
    var y:Int = processY - yAboveOffset
    val maxY = y + neighborhoodHeight
    val processPixel:Double = raster.getPixelDouble(processX, processY, processBand)
    var sum:Double = 0.0
    if (neighborhoodValues == null) {
      neighborhoodValues = Array.ofDim[Double](neighborhoodWidth * neighborhoodHeight)
    }
    var neighborhoodValueIndex:Int = 0
    while (y < maxY) {
      x = processX - xLeftOffset
      while (x < maxX) {
        if (notnodata.getPixelByte(x, y, 0) == 1) {
          neighborhoodValues(neighborhoodValueIndex) = raster.getPixelDouble(x, y, processBand)
          neighborhoodValueIndex += 1
        }
        else if (!ignoreNoData) {
          // If there is a NoData pixel anywhere in the neighborhood, and we're not ignoring
          // NoData, then just return NoData for the pixel.
          return outNoData
        }
        x += 1
      }
      y += 1
    }

    stat match {
      case FocalStatMapOp.Count => neighborhoodValueIndex
      case FocalStatMapOp.Max => computeMax(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.Min => computeMin(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.Mean => computeStat(neighborhoodValues, neighborhoodValueIndex, stat)
      case FocalStatMapOp.Median => computeMedian(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.Range => computeRange(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.StdDev => computeStat(neighborhoodValues, neighborhoodValueIndex, stat)
      case FocalStatMapOp.Sum => computeSum(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.Variance => computeStat(neighborhoodValues, neighborhoodValueIndex, stat)
    }
  }

  // The following was adapted from http://www.johndcook.com/blog/standard_deviation/ which in
  // turn came from Donald Knuth’s Art of Computer Programming, Vol 2, page 232, 3rd edition
  def computeStat(values:Array[Double], maxIndex:Int, stat:String):Double = {
    if (maxIndex <= 0 || values.length <= 0) {
      throw new IllegalArgumentException("computeStat expects at least one input value")
    }

    var oldM = values(0)
    var newM = values(0)
    var oldS = 0.0
    var newS:Double = 0.0
    var n:Int = 1
    while (n < maxIndex) {
      val value = values(n)
      n += 1
      val delta = value - oldM
      newM = oldM + delta / n
      newS = oldS + delta * (value - newM)
      oldM = newM
      oldS = newS
    }
    stat match {
      case FocalStatMapOp.StdDev =>
        math.sqrt(if (n > 1) {
          newS / (n - 1)
        }
        else {
          0.0
        })
      case FocalStatMapOp.Mean =>
        if (n > 0) {
          newM
        }
        else {
          0.0
        }
      case FocalStatMapOp.Variance =>
        if (n > 1) {
          newS / (n - 1)
        }
        else {
          0.0
        }
      case _ => throw new IllegalArgumentException("computeStat does not handle " + stat)
    }
  }

  override def setup(job:JobArguments, conf:SparkConf):Boolean = true

  override def teardown(job:JobArguments, conf:SparkConf):Boolean = true

  override def readExternal(in:ObjectInput):Unit = {
    stat = in.readUTF()
    neighborhoodSize = in.readUTF()
    ignoreNoData = in.readBoolean()
    neighborhoodPixels = in.readInt()
    val hasTileType = in.readBoolean()
    outputTileType = if (hasTileType) {
      Some(in.readInt())
    }
    else {
      None
    }
    val hasNoData = in.readBoolean()
    outputNoDatas = if (hasNoData) {
      val len = in.readInt()
      val nd = Array.fill[Double](len)(0.0)
      for (i <- 0 until len) {
        nd(i) = in.readDouble()
      }
      Some(nd)
    }
    else {
      None
    }
    val hasNeighborhoodValues = in.readBoolean()
    if (hasNeighborhoodValues) {
      val len = in.readInt()
      neighborhoodValues = Array.ofDim[Double](len)
      for (i <- 0 until len) {
        neighborhoodValues(i) = in.readDouble()
      }
    }
    else {
      neighborhoodValues = null
    }
  }

  override def writeExternal(out:ObjectOutput):Unit = {
    out.writeUTF(stat)
    out.writeUTF(neighborhoodSize)
    out.writeBoolean(ignoreNoData)
    out.writeInt(neighborhoodPixels)
    outputTileType match {
      case None => out.writeBoolean(false)
      case Some(tt) =>
        out.writeBoolean(true)
        out.writeInt(tt)
    }
    outputNoDatas match {
      case None => out.writeBoolean(false)
      case Some(nd) =>
        out.writeBoolean(true)
        out.writeInt(nd.length)
        for (v <- nd) {
          out.writeDouble(v.doubleValue())
        }
    }
    if (neighborhoodValues == null) {
      out.writeInt(0)
    }
    else {
      out.writeInt(neighborhoodValues.length)
      for (v <- neighborhoodValues) {
        out.writeDouble(v)
      }
    }
  }

  override protected def getOutputTileType:Int = {
    outputTileType match {
      case Some(tt) => tt
      case None => throw new IllegalStateException("The output tile type has not been set")
    }
  }

  override protected def getOutputNoData:Array[Double] = {
    outputNoDatas match {
      case Some(nodatas) => nodatas
      case None => throw new IllegalStateException("The output nodata values have not been set")
    }
  }

  private[mapalgebra] def this(raster:Option[RasterMapOp], stat:String, neighborhoodSize:String,
                               ignoreNoData:Boolean) = {
    this()
    inputMapOp = raster
    this.stat = stat
    this.neighborhoodSize = neighborhoodSize
    this.ignoreNoData = ignoreNoData
    init
  }

  private[mapalgebra] def this(node:ParserNode, variables:String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 4) {
      throw new ParserException("Usage: focalStat(<stat>, <raster>, <neighborhood size>, <ignoreNoData>)")
    }

    stat = MapOp.decodeString(node.getChild(0), variables) match {
      case Some(s) => s.toLowerCase
      case _ => throw new ParserException("Error decoding string for stat")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(1), variables)

    MapOp.decodeString(node.getChild(2), variables) match {
      case Some(s) => neighborhoodSize = s
      case _ => throw new ParserException("Error decoding string for neighborhood size")
    }
    MapOp.decodeBoolean(node.getChild(3), variables) match {
      case Some(b) => ignoreNoData = b
      case _ => throw new ParserException("Error decoding boolean for ignoreNoData")
    }
    init
  }

  private def init:Unit = {
    stat.toLowerCase match {
      case FocalStatMapOp.Count =>
      case FocalStatMapOp.Max =>
      case FocalStatMapOp.Min =>
      case FocalStatMapOp.Mean =>
      case FocalStatMapOp.Median =>
      case FocalStatMapOp.Range =>
      case FocalStatMapOp.StdDev =>
      case FocalStatMapOp.Sum =>
      case FocalStatMapOp.Variance =>
      case _ => throw new ParserException("Bad focalStat stat: " + stat)
    }
  }

  private def computeMax(values:Array[Double], maxIndex:Int):Double = {
    var maxValue:Double = values(0)
    var index:Int = 1
    while (index < maxIndex) {
      maxValue = maxValue.max(values(index))
      index += 1
    }
    maxValue
  }

  private def computeMin(values:Array[Double], maxIndex:Int):Double = {
    var minValue:Double = values(0)
    var index:Int = 1
    while (index < maxIndex) {
      minValue = minValue.min(values(index))
      index += 1
    }
    minValue
  }

  private def computeMedian(values:Array[Double], maxIndex:Int):Double = {
    // Set the end of the array that we're not interested in to the max double value
    // so they are sorted at the end. We don't want these values to be considered.
    var index = maxIndex
    while (index < values.length) {
      values(index) = Double.MaxValue
      index += 1
    }
    val sortedValues = values.sorted
    if ((maxIndex & 1) == 1) {
      // Odd number of elements, return the one right in the middle
      sortedValues(maxIndex / 2)
    }
    else {
      // Even number of elements, return the average of the two in the middle
      val midIndex = maxIndex / 2
      (sortedValues(midIndex) + sortedValues(midIndex - 1)) / 2.0
    }
  }

  private def computeRange(values:Array[Double], maxIndex:Int):Double = {
    var minValue:Double = values(0)
    var maxValue:Double = minValue
    var index:Int = 1
    while (index < maxIndex) {
      val v = values(index)
      minValue = minValue.min(v)
      maxValue = maxValue.max(v)
      index += 1
    }
    maxValue - minValue
  }

  private def computeSum(values:Array[Double], maxIndex:Int):Double = {
    var sumValue:Double = 0.0
    var index:Int = 0
    while (index < maxIndex) {
      sumValue += values(index)
      index += 1
    }
    sumValue
  }
}
