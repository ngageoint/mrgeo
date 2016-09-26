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

package org.mrgeo.hdfs.partitioners

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.hadoop.fs.Path
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.hdfs.utils.HadoopFileUtils


class BlockSizePartitioner() extends FileSplitPartitioner() with Externalizable {

  var partitions:Int = 0

  override def numPartitions: Int = { partitions }

  def getPartition(key: Any): Int = 0

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

  def hasFixedPartitions:Boolean = true

  override def calculateNumPartitions(raster:RasterRDD, output:String):Int = {
    val path = new Path(output)
    val fs = HadoopFileUtils.getFileSystem(path)
    val blocksize = fs.getDefaultBlockSize(path)

    // val tile = RasterWritable.toRaster(raster.first()._2)
    val tile = RasterWritable.toMrGeoRaster(raster.first()._2)


    val tilesperblock = (blocksize / tile.data().length) - 1  // subtract 1 for the 0-based counting

    partitions = Math.ceil(raster.count() / tilesperblock.toDouble).toInt

    partitions
  }
}
