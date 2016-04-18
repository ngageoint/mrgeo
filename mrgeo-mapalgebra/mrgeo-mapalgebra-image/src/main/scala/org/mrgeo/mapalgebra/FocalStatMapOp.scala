package org.mrgeo.mapalgebra

import java.awt.image.DataBuffer
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.SparkConf
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.LatLng
import org.mrgeo.utils.tms.TMSUtils

object FocalStatMapOp extends MapOpRegistrar {

  val Count: String = "count"
  val Max: String = "max"
  val Min: String = "min"
  val Mean: String = "mean"
  val Median: String = "median"
  val Range: String = "range"
  val StdDev: String = "stddev"
  val Sum: String = "sum"
  val Variance: String = "variance"

  def create(raster:RasterMapOp, stat:String, neighborhoodSize:String, ignoreNoData: Boolean):MapOp =
    new FocalStatMapOp(Some(raster), stat, neighborhoodSize, ignoreNoData)

  override def register: Array[String] = {
    Array[String]("focalstat")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new FocalStatMapOp(node, variables)
}

class FocalStatMapOp extends RawFocalMapOp with Externalizable {
  private var stat: String = null
  private var neighborhoodSize: String = null
  private var neighborhoodPixels: Int = 0
  private var ignoreNoData: Boolean = false
  private var outputTileType: Option[Int] = None
  private var outputNoDatas: Option[Array[Number]] = None
  private var neighborhoodValues: Array[Double] = null

  private[mapalgebra] def this(raster:Option[RasterMapOp], stat:String, neighborhoodSize: String,
                               ignoreNoData: Boolean) = {
    this()
    inputMapOp = raster
    this.stat = stat
    init
  }

  private[mapalgebra] def this(node:ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren != 4) {
      throw new ParserException("Usage: focalStat(<stat>, <raster>, <neighborhood size>, <ignoreNoData>)")
    }

    stat = MapOp.decodeString(node.getChild(0), variables) match  {
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

  private def init: Unit = {
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

  override def registerClasses(): Array[Class[_]] = {
    Array[Class[_]](classOf[Array[Float]])
  }

  def isNodata(noDataValue: Double, value:Double):Boolean = {
    if (noDataValue.isNaN) {
      value.isNaN
    }
    else {
      value == noDataValue
    }
  }

  override def beforeExecute(meta: MrsPyramidMetadata): Unit = {
    // Make sure that neighborhood values is re-initialized at the start of map op execution
    neighborhoodValues = null
    neighborhoodPixels = neighborhoodSize match {
      case ns if (ns.endsWith("p")) => ns.dropRight(1).toInt
      case ns if (ns.endsWith("m")) => {
        val degPerPixel = TMSUtils.resolution(meta.getMaxZoomLevel, meta.getTilesize)
        val sizeInMeters = ns.dropRight(1).toFloat
        val metersPerPixel = degPerPixel * LatLng.METERS_PER_DEGREE
        (sizeInMeters / metersPerPixel).ceil.toInt
      }
      case _ => throw new IllegalArgumentException("Invalid value for neighborhood size. Must specifiy either meters (e.g. 300m) or pixels (e.g. 10p)")
    }
    stat.toLowerCase match {
      case FocalStatMapOp.Count =>
        outputTileType = Some(DataBuffer.TYPE_INT)
        outputNoDatas = Some(Array.fill[Number](meta.getBands)(Int.MinValue))
      case FocalStatMapOp.Max | FocalStatMapOp.Min | FocalStatMapOp.Range =>
        outputTileType = Some(meta.getTileType)
        val nodatas = meta.getDefaultValuesNumber
        outputNoDatas = Some(nodatas)
      case FocalStatMapOp.Mean | FocalStatMapOp.Median | FocalStatMapOp.StdDev |
           FocalStatMapOp.Sum | FocalStatMapOp.Variance =>
        outputTileType = Some(DataBuffer.TYPE_FLOAT)
        outputNoDatas = Some(Array.fill[Number](meta.getBands)(Float.NaN))
    }
  }

  override def getNeighborhoodInfo: (Int, Int) = {
    (this.neighborhoodPixels, this.neighborhoodPixels)
  }

  override protected def getOutputTileType: Int = {
    outputTileType match {
      case Some(tt) => tt
      case None => throw new IllegalStateException("The output tile type has not been set")
    }
  }

  override protected def getOutputNoData: Array[Number] = {
    outputNoDatas match {
      case Some(nodatas) => nodatas
      case None => throw new IllegalStateException("The output nodata values have not been set")
    }
  }

  override def computePixelValue(rasterValues: Array[Double], notnodata: Array[Boolean],
                                 outNoData: Double, rasterWidth: Int,
                                 processX: Int, processY: Int,
                                 xLeftOffset: Int, neighborhoodWidth: Int,
                                 yAboveOffset: Int, neighborhoodHeight: Int, tileId: Long): Double = {
    var x: Int = processX - xLeftOffset
    val maxX = x + neighborhoodWidth
    var y: Int = processY - yAboveOffset
    val maxY = y + neighborhoodHeight
    val processPixel: Double = rasterValues(calculateRasterIndex(rasterWidth, processX, processY))
    var sum: Double = 0.0
    if (neighborhoodValues == null) {
      neighborhoodValues = Array.ofDim[Double](neighborhoodWidth * neighborhoodHeight)
    }
    var neighborhoodValueIndex: Int = 0
    while (y < maxY) {
      x = processX - xLeftOffset
      while (x < maxX) {
        val index = calculateRasterIndex(rasterWidth, x, y)
        if (notnodata(index)) {
          neighborhoodValues(neighborhoodValueIndex) = rasterValues(index)
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
      case FocalStatMapOp.Mean => computeSum(neighborhoodValues, neighborhoodValueIndex) / neighborhoodValueIndex
      case FocalStatMapOp.Median => computeMedian(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.Range => computeRange(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.StdDev => {
        val mean = computeSum(neighborhoodValues, neighborhoodValueIndex) / neighborhoodValueIndex
        val variance = computeVariance(neighborhoodValues, neighborhoodValueIndex, mean)
        math.sqrt(variance)
      }
      case FocalStatMapOp.Sum => computeSum(neighborhoodValues, neighborhoodValueIndex)
      case FocalStatMapOp.Variance => {
        val mean = computeSum(neighborhoodValues, neighborhoodValueIndex) / neighborhoodValueIndex
        computeVariance(neighborhoodValues, neighborhoodValueIndex, mean)
      }
    }
  }

  private def computeMax(values: Array[Double], maxIndex: Int): Double = {
    var maxValue: Double = values(0)
    var index: Int = 1
    while (index < maxIndex) {
      maxValue = maxValue.max(values(index))
      index += 1
    }
    maxValue
  }

  private def computeMin(values: Array[Double], maxIndex: Int): Double = {
    var minValue: Double = values(0)
    var index: Int = 1
    while (index < maxIndex) {
      minValue = minValue.min(values(index))
      index += 1
    }
    minValue
  }

  private def computeMedian(values: Array[Double], maxIndex: Int): Double = {
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
      sortedValues((maxIndex / 2).toInt)
    }
    else {
      // Even number of elements, return the average of the two in the middle
      val midIndex = (maxIndex / 2).toInt
      (sortedValues(midIndex) + sortedValues(midIndex - 1)) / 2.0
    }
  }

  private def computeRange(values: Array[Double], maxIndex: Int): Double = {
    var minValue: Double = values(0)
    var maxValue: Double = minValue
    var index: Int = 1
    while (index < maxIndex) {
      val v = values(index)
      minValue = minValue.min(v)
      maxValue = maxValue.max(v)
      index += 1
    }
    maxValue - minValue
  }

  private def computeSum(values: Array[Double], maxIndex: Int): Double = {
    var sumValue: Double = 0.0
    var index: Int = 0
    while (index < maxIndex) {
      sumValue += values(index)
      index += 1
    }
    sumValue
  }

  private def computeVariance(values: Array[Double], maxIndex: Int, mean: Double): Double = {
    var variance: Double = 0.0
    var index: Int = 0
    while (index < maxIndex) {
      val delta = values(index) - mean
      variance += (delta * delta)
      index += 1
    }
    variance
  }

  override def setup(job: JobArguments, conf:SparkConf): Boolean = true
  override def teardown(job: JobArguments, conf:SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {
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
      val nd = Array.fill[Number](len)(0.0)
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

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(stat)
    out.writeUTF(neighborhoodSize)
    out.writeBoolean(ignoreNoData)
    out.writeInt(neighborhoodPixels)
    outputTileType match {
      case None => out.writeBoolean(false)
      case Some(tt) => {
        out.writeBoolean(true)
        out.writeInt(tt)
      }
    }
    outputNoDatas match {
      case None => out.writeBoolean(false)
      case Some(nd) => {
        out.writeBoolean(true)
        out.writeInt(nd.length)
        for (v <- nd) {
          out.writeDouble(v.doubleValue())
        }
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
}
