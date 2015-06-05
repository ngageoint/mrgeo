package org.mrgeo.spark

import java.io._
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
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.MrsImagePyramid
import scala.collection.JavaConverters._
import org.mrgeo.tile.SplitGenerator

import scala.collection.mutable

@SerialVersionUID(-1)
class SparkTileIdPartitioner(splitGenerator:SplitGenerator) extends Partitioner with Externalizable
{

  private var splits:Array[java.lang.Long] = null

  if (splitGenerator != null) {
    splits = splitGenerator.getSplits.asScala.toArray
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
      //print("*** " + id.get + " ")
      // lots of splits, binary search
      if (splits.length > 1000) {
        //println("# " + binarySearch(splits, id.get, 0, splits.length - 1))
        return binarySearch(splits, id.get, 0, splits.length - 1)
      }
      // few splits, brute force search
      splits.foreach(split => {
        if (id.get <= split) {
          //println("@ " + split)
          return split.toInt
        }
      })

      //println("& " + splits.length)
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

    if (splits.length > 1) {
      val HasPartitionNames: Long = -12345L
      val SplitFile: String = "splits"

      val fs: FileSystem = HadoopFileUtils.getFileSystem(conf, new Path(path))

      // check which type of "part-" scheme we have.  This can change depending of what actually wrote the file
      var prefix = ""
      if (fs.exists(new Path(path, "part-r-00000"))) {
        prefix = "part-r-" // created as a reduce step
      }
      else if (fs.exists(new Path(path, "part-m-00000"))) {
        prefix = "part-m-" // created as a map-only step
      }
      else {
        prefix = "part-" // generic step
      }
        val fdos: FSDataOutputStream = fs.create(new Path(path, SplitFile))

      val out: PrintWriter = new PrintWriter(fdos)
      try {
        // write the magic number
        val magic: Array[Byte] = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(HasPartitionNames).array)

        out.println(new String(magic))

        //println("\nsplits:")
        splits.indices.foreach(ndx => {
          val id: Array[Byte] = Base64.encodeBase64(ByteBuffer.allocate(8).putLong(splits(ndx)).array)
          out.println(new String(id))
          val part: Array[Byte] = Base64.encodeBase64("%s%05d".format(prefix, ndx).getBytes)
          out.println(new String(part))

          //println("  " + splits(ndx) + " " + "%s%05d".format(prefix, ndx))
        })

        // just need to write the name of the last partition, we can use magic number
        // for the tileid...
        out.println(new String(magic))

        val part: Array[Byte] = Base64.encodeBase64("%s%05d".format(prefix,numPartitions - 1).getBytes)
        out.println(new String(part))

        //println("  " + HasPartitionNames + " " + "%s%05d".format(prefix, numPartitions - 1))
      }
      finally {
        out.close()
        fdos.close()
      }
    }
  }

  private def binarySearch(list: Array[java.lang.Long], target: Long, start: Int, end: Int): Int = {
    // if not found at the end of the list, return the end + 1th partitiomm
    if (start > end) {
      return splits.length
    }

    // if not found at the start of the list, return the start
    if (end == 0) {
      return 0
    }

    val mid:Int = start + (end - start + 1) / 2

    //println("s:" + list(start) + " m1:" + list(mid - 1) + " t:" + target + " m2:" + list(mid) + " e:" + list(end))
    if (list(mid - 1) < target && list(mid) >= target) {
      mid
    }
    else if (list(mid) > target) {
      binarySearch(list, target, start, mid)
    }
    else {
      binarySearch(list, target, mid, end)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    val count = in.readInt()

    val builder = mutable.ArrayBuilder.make[java.lang.Long]()
    for (i <- 0 until count) {
      builder += in.readLong()
    }
    splits = builder.result()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(splits.length)
    splits.foreach(split => out.writeLong(split))
  }
}
