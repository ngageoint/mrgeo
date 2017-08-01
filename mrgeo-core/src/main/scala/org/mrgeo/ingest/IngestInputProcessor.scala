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

package org.mrgeo.ingest

import java.io.{File, IOException}
import java.net.URI

import edu.umd.cs.findbugs.annotations.{CheckForNull, SuppressFBWarnings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.gdal.gdal.{Band, Dataset}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.utils.tms.{Bounds, TMSUtils}
import org.mrgeo.utils.{GDALUtils, Logging}

import scala.collection.mutable.ListBuffer

@SuppressFBWarnings(value = Array("PZLA_PREFER_ZERO_LENGTH_ARRAYS"), justification = "Need to return null")
class IngestInputProcessor extends Logging {
  val tilesize = MrGeoProperties.getInstance.getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE,
    MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT).toInt
  private val inputs = new ListBuffer[String]()
  var skippreprocessing:Boolean = false
  var local:Boolean = false
  private var conf:Configuration = null
  private var nodataOverride:Array[Double] = null
  private var nodata:Option[Array[Double]] = None
  private var bounds:Option[Bounds] = None
  private var firstInput:Boolean = true
  private var zoomlevel:Int = -1
  private var bands:Int = -1
  private var tiletype:Int = -1

  def this(conf:Configuration, nodataOverride:Array[Double] = null, zoomlevel:Int = -1,
           skippreprocessing:Boolean = false) {
    this()
    this.conf = conf
    this.nodataOverride = nodataOverride
    this.zoomlevel = zoomlevel
    this.skippreprocessing = skippreprocessing
    if (this.skippreprocessing && this.zoomlevel < 0) {
      throw new Exception("When you skip preprocessing during ingest, you must specify a zoom level")
    }
  }

  def getInputs = {
    inputs.toList
  }

  def getZoomlevel = {
    zoomlevel
  }

  def getTiletype = {
    tiletype
  }

  def getBands = {
    bands
  }

  def getBounds = bounds.orNull

  def getNodata = nodata.orNull

  def processInput(arg:String, recurse:Boolean):Unit = {
    processInput(arg, recurse, true, false)
  }

  // Should only be used privately - it is called recursively with the additional internal
  // parameters existsCheck and argIsDir that help prevent excessive HDFS/S3 access.
  @SuppressFBWarnings(value = Array("PATH_TRAVERSAL_IN"), justification = "File() used to find GDAL images only")
  private def processInput(arg:String, recurse:Boolean,
                           existsCheck:Boolean, argIsDir:Boolean):ListBuffer[String] = {
    var f:File = null
    try {
      f = new File(new URI(arg))
    }
    catch {
      case ignored:Any => {
        f = new File(arg)
      }
    }

    // recurse through directories
    if (f.isDirectory) {
      val dir:Array[File] = f.listFiles
      if (dir != null) {
        for (s <- dir) {
          try {
            if (s.isFile || (s.isDirectory && recurse)) {
              processInput(s.getCanonicalFile.toURI.toString, recurse, false, s.isDirectory)
            }
          }
          catch {
            case ignored:IOException => {
            }
          }
        }
      }
    }
    else if (f.isFile) {
      try {
        System.out.print("*** checking (local file) " + f.getCanonicalPath)
        val name:String = f.getCanonicalFile.toURI.toString
        if (skippreprocessing) {
          if (firstInput) {
            firstInput = false
            val dataset:Dataset = GDALUtils.open(name)
            if (dataset != null) {
              try {
                calculateMinimalParams(dataset)
              }
              finally {
                GDALUtils.close(dataset)
              }
            }
          }
          inputs += name
          local = true
          System.out.println(" accepted ***")
        }
        else {
          val dataset:Dataset = GDALUtils.open(name)
          if (dataset != null) {
            calculateParams(dataset)
            GDALUtils.close(dataset)
            inputs += name
            local = true
            System.out.println(" accepted ***")
          }
          else {
            System.out.println(" can't load ***")
          }
        }
      }
      catch {
        case ignored:IOException => {
          System.out.println(" can't load ***")
        }
      }
    }
    else {
      try {
        val p:Path = new Path(arg)
        val fs:FileSystem = HadoopFileUtils.getFileSystem(conf, p)
        if (!existsCheck || fs.exists(p)) {
          var isADirectory:Boolean = argIsDir
          if (existsCheck) {
            val status:FileStatus = fs.getFileStatus(p)
            isADirectory = status.isDirectory
          }
          if (isADirectory && recurse) {
            val files:Array[FileStatus] = fs.listStatus(p)
            for (file <- files) {
              processInput(file.getPath.toUri.toString, true, false, file.isDirectory)
            }
          }
          else {
            System.out.print("*** checking  " + p.toString)
            val name:String = p.toUri.toString
            if (skippreprocessing) {
              if (firstInput) {
                firstInput = false
                val dataset:Dataset = GDALUtils.open(name)
                if (dataset != null) {
                  try {
                    calculateMinimalParams(dataset)
                  }
                  finally {
                    GDALUtils.close(dataset)
                  }
                }
              }
              inputs += name
              System.out.println(" accepted ***")
            }
            else {
              val dataset:Dataset = GDALUtils.open(name)
              if (dataset != null) {
                calculateParams(dataset)
                GDALUtils.close(dataset)
                inputs += name
                System.out.println(" accepted ***")
              }
              else {
                System.out.println(" can't load ***")
              }
            }
          }
        }
      }
      catch {
        case ignored:IOException => {
        }
      }
    }

    return inputs
  }

  private def calculateParams(image:Dataset):Unit = {
    val imageBounds:Bounds = GDALUtils.getBounds(image)
    log.debug("    image bounds: (lon/lat) " + imageBounds.w + ", " + imageBounds.s + " to " + imageBounds.e + ", " +
              imageBounds.n)
    bounds = Some(bounds match {
      case None => imageBounds
      case Some(b) => b.expand(imageBounds)
    })
    val zx:Int = TMSUtils.zoomForPixelSize(imageBounds.width / image.getRasterXSize, tilesize)
    val zy:Int = TMSUtils.zoomForPixelSize(imageBounds.height / image.getRasterYSize, tilesize)
    if (zoomlevel < zx) {
      zoomlevel = zx
    }
    if (zoomlevel < zy) {
      zoomlevel = zy
    }
    if (firstInput) {
      firstInput = false
      calculateMinimalParams(image)
    }
  }

  private def calculateMinimalParams(image:Dataset):Unit = {
    bands = image.GetRasterCount
    tiletype = GDALUtils.toRasterDataBufferType(image.GetRasterBand(1).getDataType)
    val nd = Array.ofDim[Double](bands)
    if (nodataOverride == null) {
      var b:Int = 1
      while (b <= bands) {
        val band:Band = image.GetRasterBand(b)
        val ndValue = new Array[java.lang.Double](1)
        band.GetNoDataValue(ndValue)
        if (ndValue(0) != null) {
          nd(b - 1) = ndValue(0)
          log.debug(
            "nodata: b: " + nd(b - 1).byteValue + " d: " + nd(b - 1).doubleValue + " f: " + nd(b - 1).floatValue +
            " i: " + nd(b - 1).intValue + " s: " + nd(b - 1).shortValue + " l: " + nd(b - 1).longValue)
        }
        else {
          log.debug("Unable to retrieve nodata from source, using NaN")
          nd(b - 1) = Double.NaN
        }
        b += 1
      }
    }
    else {
      if (nodataOverride.length == 1) {
        for (i <- 0 until bands) {
          nd(i) = nodataOverride(0)
        }
      }
      else if (nodataOverride.length < bands) {
        throw new Exception(f"There are too few nodata override values (${
          nodataOverride.length
        }) compared to the number of bands ($bands) in the inputs")
      }
      else {
        log.warn(f"There are more nodata values (${nodataOverride.length}) than bands ($bands) in the inputs")
        for (i <- 0 until bands) {
          nd(i) = nodataOverride(i)
        }
      }
    }
    nodata = Some(nd)
  }
}
