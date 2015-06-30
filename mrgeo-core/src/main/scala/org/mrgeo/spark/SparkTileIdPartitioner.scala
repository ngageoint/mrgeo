package org.mrgeo.spark

import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Partitioner
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider
import org.mrgeo.hdfs.partitioners.SplitGenerator
import org.mrgeo.hdfs.tile.PartitionerSplit

@SerialVersionUID(-1)
class SparkTileIdPartitioner(splitGenerator:SplitGenerator) extends Partitioner with Externalizable
{

  private var splits = new PartitionerSplit

  if (splitGenerator != null) {
    splits.generateSplits(splitGenerator)
  }

  def this() {
    this(null)
  }


  override def numPartitions: Int = {
    splits.length + 1
  }

  override def getPartition(key: Any): Int = {
    key match
    {
    case id: TileIdWritable =>
      val split = splits.getSplit(id.get())
       split.getPartition
    case _ =>     throw new RuntimeException("Bad type sent into SparkTileIdPartitioner.getPartition(): " +
        key.getClass + ". Expected org.mrgeo.data.tile.TileIdWritable or a subclass.")
    }
  }

//  def writeSplits(pyramid:String, zoom:Int, conf:Configuration): Unit = {
//    val dp: HdfsMrsImageDataProvider = new HdfsMrsImageDataProvider(conf, pyramid, null)
//
//    val inputWithZoom: Path = new Path(dp.getResourcePath(false), "" + zoom)
//
//    writeSplits(inputWithZoom.toString, conf)
//  }
//
//  def writeSplits(path:String, conf:Configuration): Unit = {
//    splits.writeSplits(new Path(path))
//  }

  def findSplit(list: Array[java.lang.Long], target: Long): Int =
  {
    // First check the min and max values before binary searching.
    if (list.length == 0)
    {
      // All tiles fit in a single split
      return 0
    }
    if (target <= list(0))
    {
      return 0
    }
    if (target > list(list.length - 1))
    {
      return list.length
    }
    // The target does not fall in the minimum or maximum split, so let's
    // binary search to find it.
    binarySearch(list, target, 0, list.length - 1)
  }

  def binarySearch(list: Array[java.lang.Long], target: Long, start: Int, end: Int): Int =
  {
    val mid:Int = start + (end - start + 1) / 2

    //println("s:" + list(start) + " m1:" + list(mid - 1) + " t:" + target + " m2:" + list(mid) + " e:" + list(end))
    if (list(mid - 1) < target && list(mid) >= target) {
      mid
    }
    else if (list(mid) > target) {
      binarySearch(list, target, start, mid - 1)
    }
    else {
      binarySearch(list, target, mid, end)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    splits.readExternal(in)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    splits.writeExternal(out)
  }
}
