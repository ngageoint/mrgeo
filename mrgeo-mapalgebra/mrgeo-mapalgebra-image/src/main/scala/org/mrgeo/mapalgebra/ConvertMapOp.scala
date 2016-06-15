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

import java.awt.image.DataBuffer
import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.data.raster.{RasterUtils, RasterWritable}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.image.MrsPyramidMetadata
import org.mrgeo.job.JobArguments
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.utils.{FloatUtils, SparkUtils}
import org.mrgeo.utils.MrGeoImplicits._

@SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
object ConvertMapOp extends MapOpRegistrar {
  override def register: Array[String] = {
    Array[String]("convert")
  }

  def create(raster: RasterMapOp, toType: String, conversionMethod: String = "truncate") =
    new ConvertMapOp(Some(raster), Some(toType), Some(conversionMethod))

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new ConvertMapOp(node, variables)

  def truncateValue(value: Double, oldMin: Double, oldMax: Double, toType: Int): Double =
  {
    toType match {
      // We want values from 0 .. 254 rather than -127 .. 127
      case DataBuffer.TYPE_BYTE => Math.max(Math.min(value, 254.0), 0.0)
      case DataBuffer.TYPE_SHORT => Math.max(Math.min(value, Short.MaxValue.toDouble), (Short.MinValue + 1).toDouble)
      case DataBuffer.TYPE_INT => Math.max(Math.min(value, Int.MaxValue.toDouble), (Int.MinValue + 1).toDouble)
      case DataBuffer.TYPE_FLOAT => Math.max(Math.min(value, Float.MaxValue.toDouble), Float.MinValue.toDouble)
      case DataBuffer.TYPE_DOUBLE => Math.max(Math.min(value, Double.MaxValue), Double.MinValue) // should not get called
    }
  }

  def modValue(value: Double, oldMin: Double, oldMax: Double, toType: Int): Double =
  {
    toType match {
      // We want values from 0 .. 255 rather than -127 .. 127
      case DataBuffer.TYPE_BYTE => value % 254.0
      case DataBuffer.TYPE_SHORT => value % Short.MaxValue
      case DataBuffer.TYPE_INT => value % Int.MaxValue
      case DataBuffer.TYPE_FLOAT => value % Float.MaxValue
      case DataBuffer.TYPE_DOUBLE => value % Double.MaxValue
    }
  }

  def fitValue(value: Double, oldMin: Double, oldMax: Double, toType: Int): Double =
  {
    val range = toType match {
      // We want values from 0 .. 254 rather than -127 .. 127
      case DataBuffer.TYPE_BYTE => (0.0, 254.0)
      case DataBuffer.TYPE_SHORT => ((Short.MinValue + 1).toDouble, Short.MaxValue.toDouble)
      case DataBuffer.TYPE_INT => ((Int.MinValue + 1).toDouble, Int.MaxValue.toDouble)
      case DataBuffer.TYPE_FLOAT => (Float.MinValue.toDouble, Float.MaxValue.toDouble)
      case DataBuffer.TYPE_DOUBLE => (Double.MinValue, Double.MaxValue)
    }
    if (oldMax == oldMin) {
      oldMin
    }
    else {
      val oldDelta = oldMax - oldMin
      val newDelta = range._2 - range._1
      range._1 + (value / oldDelta) * newDelta
    }
  }

  /**
    * The output nodata value will be set to the input nodata value if the conversion method is
    * "truncate" and the value is in the data range for the new type. Otherwise, for byte, the
    * output nodata will be 255, for short and int it will be MinValue. For float32 and
    * float64, it will be NaN regardless of the conversion method.
    *
    * @param meta
    * @param conversionMethod
    * @param tileType
    * @return
    */
  def computeOutputNodata(meta: MrsPyramidMetadata, conversionMethod: String,
                          tileType: String): Array[Number] = {
    val newNodata = new Array[Number](meta.getBands)
    var b: Int = 0
    while (b < newNodata.length) {
      val inputNodata = meta.getDefaultValue(b)
      newNodata(b) = tileType.toLowerCase() match {
        case "byte" =>
          if (conversionMethod.equalsIgnoreCase("truncate")) {
            if (inputNodata >= Byte.MinValue && inputNodata <= Byte.MaxValue) {
              inputNodata.toByte.toDouble
            }
            else {
              255.0
            }
          }
          else {
            255.0
          }
        case "short" =>
          if (conversionMethod.equalsIgnoreCase("truncate")) {
            if (inputNodata >= Short.MinValue && inputNodata <= Short.MaxValue) {
              inputNodata.toShort.toDouble
            }
            else {
              Short.MinValue.toDouble
            }
          }
          else {
            Short.MinValue.toDouble
          }
        case "int" =>
          if (conversionMethod.equalsIgnoreCase("truncate")) {
              if (inputNodata >= Int.MinValue && inputNodata <= Int.MaxValue) {
                inputNodata.toInt.toDouble
              }
            else {
              Int.MinValue.toDouble
            }
          }
          else {
            Int.MinValue.toDouble
          }
        case "float32" => Float.NaN
        case "float64" => Double.NaN
      }
      b += 1
    }
    newNodata
  }
}

@SuppressFBWarnings(value = Array("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"), justification = "Scala generated code")
class ConvertMapOp extends RasterMapOp with Externalizable {
  // Available tile types in increasing order of range
  def tileTypeSizeOrdering = Array[Int] (
    DataBuffer.TYPE_BYTE,
    DataBuffer.TYPE_SHORT,
    DataBuffer.TYPE_USHORT,
    DataBuffer.TYPE_INT,
    DataBuffer.TYPE_FLOAT,
    DataBuffer.TYPE_DOUBLE
  )
  private var rasterRDD: Option[RasterRDD] = None

  private var inputMapOp: Option[RasterMapOp] = None
  private var toType: Option[String] = None
  private var conversionMethod: Option[String] = None

  private[mapalgebra] def this(raster:Option[RasterMapOp], toType: Option[String],
    conversionMethod:Option[String]) =
  {
    this()

    this.inputMapOp = raster
    this.toType = toType
    this.conversionMethod = conversionMethod
  }

  def this(node: ParserNode, variables: String => Option[ParserNode]) {
    this()

    if ((node.getNumChildren < 2) || (node.getNumChildren > 3)) {
      throw new ParserException(
        "convert usage: convert(source raster, toType, [conversionMethod])")
    }

    inputMapOp = RasterMapOp.decodeToRaster(node.getChild(0), variables)
    toType = MapOp.decodeString(node.getChild(1), variables)
    toType match {
      case Some(tt) => {
        tt.toLowerCase match {
          case "byte" | "short" | "int" | "float32" | "float64" => {}
          case _ => throw new ParserException("Invalid toType - expected byte, short, int, float32 or float64")
        }
      }
      case None => throw new ParserException("Invalid toType - expected byte, short, int, float32 or float64")
    }
    if (node.getNumChildren == 3) {
      conversionMethod = MapOp.decodeString(node.getChild(2), variables)
      conversionMethod match {
        case Some(cm) => {
          cm.toLowerCase match {
            case "truncate" | "mod" | "fit" => { }
            case _ => throw new ParserException("conversionMethod must be truncate, mod or fit")
          }
        }
        case None => throw new ParserException("conversionMethod must be truncate, mod or fit");
      }
    }
    else {
      conversionMethod = Some("truncate")
    }
  }

  override def rdd(): Option[RasterRDD] = {
    rasterRDD
  }

  override def execute(context: SparkContext): Boolean = {
    val input:RasterMapOp = inputMapOp getOrElse(throw new IOException("Input MapOp not valid!"))
    val tt = toType getOrElse(throw new IOException("toType not valid!"))
    val cm = conversionMethod getOrElse(throw new IOException("conversionMethod not valid!"))

    val meta = input.metadata() getOrElse(throw new IOException("Can't load metadata! Ouch! " + input.getClass.getName))

    val newTileType = tt.toLowerCase() match {
      case "byte" => DataBuffer.TYPE_BYTE
      case "short" => DataBuffer.TYPE_SHORT
      case "int" => DataBuffer.TYPE_INT
      case "float32" => DataBuffer.TYPE_FLOAT
      case "float64" => DataBuffer.TYPE_DOUBLE
    }
    val oldTypeIndex = tileTypeSizeOrdering.indexOf(meta.getTileType)
    val newTypeIndex = tileTypeSizeOrdering.indexOf(newTileType)

    if (newTileType == meta.getTileType) {
      // No work to be done, just reuse the input raster and metadata
      rasterRDD = input.rdd()
      metadata(meta)
    }
    else {
      if (newTypeIndex > oldTypeIndex) {
        if (!cm.equalsIgnoreCase("truncate")) {
          throw new IOException("When converting to a bigger tile type, only 'truncate' is supported")
        }
      }
      // Perform conversion work
      val rdd = input.rdd() getOrElse(throw new IOException("Can't load RDD! Ouch! " +
        inputMapOp.getClass.getName))
      val newNodata = ConvertMapOp.computeOutputNodata(meta, cm, tt)
      // For performance, pre-compute whether each band's nodata value is NaN or not
      val oldNodata = meta.getDefaultValues
      val oldNodataIsNan = new Array[Boolean](oldNodata.length)
      oldNodata.zipWithIndex.foreach(U => {
        oldNodataIsNan(U._2) = java.lang.Double.isNaN(U._1)
      })
      var stats = meta.getStats
      if (stats == null) {
        stats = SparkUtils.calculateStats(rdd, meta.getBands,
          meta.getDefaultValues)
      }
      val result = rdd.map(U => {
        val src = RasterWritable.toRaster(U._2)
        val dst = RasterUtils.createEmptyRaster(meta.getTilesize, meta.getTilesize, meta.getBands,
          newTileType, newNodata)
        val converter = cm match {
          case "truncate" => ConvertMapOp.truncateValue(_, _, _, _)
          case "mod" => ConvertMapOp.modValue(_, _, _, _)
          case "fit" => ConvertMapOp.fitValue(_, _, _, _)
        }
        var py = 0
        while (py < src.getHeight) {
          log.warn("Processing row " + py)
          var px = 0
          while (px < src.getWidth) {
            var b: Int = 0
            while (b < src.getNumBands) {
              val srcVal = src.getSampleDouble(px, py, b)
              val dstVal = if (oldNodataIsNan(b)) {
                if (java.lang.Double.isNaN(srcVal)) {
                  newNodata(b).doubleValue()
                }
                else {
                  if (newTypeIndex > oldTypeIndex) {
                    srcVal
                  }
                  else {
                    converter(srcVal, stats(b).min, stats(b).max, newTileType)
                  }
                }
              }
              else {
                if (FloatUtils.isEqual(srcVal, oldNodata(b))) {
                  newNodata(b).doubleValue()
                }
                else {
                  converter(srcVal, stats(b).min, stats(b).max, newTileType)
                }
              }
              dst.setSample(px, py, b, dstVal)
              b += 1
            }
            px += 1
          }
          py += 1
        }
        (U._1, RasterWritable.toWritable(dst))
      })
      rasterRDD = Some(RasterRDD(result))
      metadata(SparkUtils.calculateMetadata(rasterRDD.get, meta.getMaxZoomLevel, newNodata,
        bounds = meta.getBounds, calcStats = false))
    }
    true
  }

//  override def registerClasses(): Array[Class[_]] = {
//  }

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = {
    true
  }

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}
}
