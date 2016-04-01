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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider
import org.mrgeo.hdfs.output.image.HdfsMrsPyramidOutputFormatProvider
import org.mrgeo.hdfs.tile.{FileSplit, PartitionerSplit}
import org.mrgeo.utils.tms.{TileBounds, Bounds, TMSUtils}
import org.mrgeo.utils.{SparkUtils}

@SerialVersionUID(-1)
class RowPartitioner() extends FileSplitPartitioner() with Externalizable
{
  private val splits = new PartitionerSplit

  def this(bounds:Bounds, zoom:Int, tilesize:Int)
  {
    this()

    val tileBounds: TileBounds = TMSUtils
        .boundsToTile(bounds, zoom, tilesize)
    val tileIncrement: Int = 1
    val splitGenerator: ImageSplitGenerator = new ImageSplitGenerator(tileBounds.w, tileBounds.s, tileBounds.e,
      tileBounds.n, zoom, tileIncrement)

    splits.generateSplits(splitGenerator)
  }

  override def numPartitions: Int = {
    splits.length
  }

  override def getPartition(key: Any): Int = {
    key match {
    case id: TileIdWritable =>
      val split = splits.getSplit(id.get())
      split.getPartition
    case _ => throw new RuntimeException("Bad type sent into SparkTileIdPartitioner.getPartition(): " +
        key.getClass + ". Expected org.mrgeo.data.tile.TileIdWritable or a subclass.")
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    splits.readExternal(in)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    splits.writeExternal(out)
  }

  def hasFixedPartitions:Boolean = false

}
