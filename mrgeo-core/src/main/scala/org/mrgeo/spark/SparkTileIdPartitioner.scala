package org.mrgeo.spark

import java.io.PrintWriter
import java.nio.ByteBuffer

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FSDataOutputStream, FileSystem}
import org.apache.spark.{SparkContext, Partitioner}
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.tile.{TiledInputFormatContext, TileIdWritable}
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider
import org.mrgeo.hdfs.tile.SplitFile
import org.mrgeo.image.MrsImagePyramid
import scala.collection.JavaConverters._
import org.mrgeo.tile.SplitGenerator

@SerialVersionUID(-1)
class SparkTileIdPartitioner(splitGenerator:SplitGenerator) extends Partitioner
{

  private val splits = splitGenerator.getSplits.asScala.toArray


  override def numPartitions: Int = {
    splits.length + 1
  }

  override def getPartition(key: Any): Int = {
    key match
    {
    case id: TileIdWritable =>
      //print("*** " + id.get + " ")
      // lots of splits, binary search
      if (splits.length > 1000) {
        //println(binarySearch(splits, id.get, 0, splits.length - 1))
        return binarySearch(splits, id.get, 0, splits.length - 1)
      }
      // few splits, brute force search
      for (split <- splits.indices) {
        if (id.get <= splits(split)) {
          //println(split)
          return split
        }
      }
      //println(splits.length)
      return splits.length
    }

    throw new RuntimeException("Bad type sent into SparkTileIdPartitioner.getPartition(): " +
        key.getClass + ". Expected org.mrgeo.data.tile.TileIdWritable or a subclass.")
  }

  def writeSplits(pyramid:String, zoom:Int, conf:Configuration): Unit = {
    val dp: HdfsMrsImageDataProvider = new HdfsMrsImageDataProvider(conf, pyramid, null)

    val inputWithZoom: Path = new Path(dp.getResourcePath(false), "" + zoom)

    writeSplits(inputWithZoom.toString, conf)
  }

  def writeSplits(path:String, conf:Configuration): Unit = {
    val HasPartitionNames: Long = -12345L
    val SplitFile: String = "splits"

    val fs: FileSystem = FileSystem.get(conf)
    val fdos: FSDataOutputStream = fs.create(new Path(path, SplitFile))

    val out: PrintWriter = new PrintWriter(fdos)
    try
    {
      // write the magic number
      val magic: Array[Byte] = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(HasPartitionNames).array)

      out.println(new String(magic))

      println("\nsplits:")
      splits.indices.foreach(ndx => {
        val id: Array[Byte] = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(splits(ndx)).array)
        out.println(new String(id))
        val part: Array[Byte] = Base64.encodeBase64("part-r-%05d".format(ndx).getBytes)
        out.println(new String(part))

        //println("  " + splits(ndx) + " " + "part-r-%05d".format(ndx))
      })

      // just need to write the name of the last partition, we can use magic number
      // for the tileid...
      out.println(new String(magic))

      val part: Array[Byte] = Base64.encodeBase64("part-r-%05d".format(numPartitions - 1).getBytes)
      out.println(new String(part))

      //println("  " + HasPartitionNames + " " + "part-r-%05d".format(numPartitions - 1))
    }
    finally
    {
      out.close()
      fdos.close()
    }
  }

  private def binarySearch(list: Array[java.lang.Long], target: Long, start: Int, end: Int): Int = {
    // if not found at the end of the list, return the end + 1th partitiomm
    if (start > end) return splits.length

    val mid:Int = start + (end - start + 1) / 2

    if (list(mid) == target) {
      mid
    }

    if (list(mid) > target) {
      binarySearch(list, target, start, mid - 1)
    }

    binarySearch(list, target, mid + 1, end)
  }


}
