package org.mrgeo.hdfs.partitioners

import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.util

import org.apache.hadoop.fs.Path
import org.mrgeo.data.image.ImageOutputFormatContext
import org.mrgeo.data.raster.RasterUtils
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.utils.TMSUtils


class BlockSizePartitioner() extends FileSplitPartitioner() with Externalizable {

  var splits:Array[Long] = Array.empty[Long]

  def this(context: ImageOutputFormatContext) {
    this()

    val path = new Path(context.getOutput)
    val fs = HadoopFileUtils.getFileSystem(path)
    val blocksize = fs.getDefaultBlockSize(path)

    val pixelbytes = RasterUtils.getElementSize(context.getTiletype) * context.getBands
    val imagebytes = pixelbytes * context.getTilesize * context.getTilesize

    val tilesperblock = (blocksize / imagebytes) - 1  // subtract 1 for the 0-based counting

    val zoom = context.getZoomlevel
    // There MUST be a better way than this brute force method!
    val tilebounds = TMSUtils.boundsToTile(context.getBounds.getTMSBounds, zoom, context.getTilesize)

    val splitsbuilder = Array.newBuilder[Long]
    var cnt = 0
    for (ty <- tilebounds.s to tilebounds.n) {
      for (tx <- tilebounds.w to tilebounds.e) {
        if (cnt >= tilesperblock) {
          splitsbuilder += TMSUtils.tileid(tx, ty, zoom)
          cnt = 0
        }
        else {
          cnt += 1
        }
      }
    }

    // see if we need to add the last id in...
    if (cnt > 0) {
      splitsbuilder += TMSUtils.tileid(tilebounds.e, tilebounds.n, zoom)
    }

    // reverse sort the array
    splits = splitsbuilder.result()
  }


  override def numPartitions: Int = { splits.length }

  // Inspired by Sparks RangePartitioner.getPartition()
  def getPartition(key: Any): Int = {
    val tid = key.asInstanceOf[TileIdWritable].get()
    var partition = 0

    // If we have less than 128 partitions naive search
    if (splits.length <= 128) {
      val ordering = implicitly[Ordering[Long]]
      while (partition < splits.length && ordering.gt(tid, splits(partition))) {
        partition += 1
      }
    }
    else {
      // Determine which binary search method to use only once.
      partition = util.Arrays.binarySearch(splits, tid)

      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {

        partition = -partition - 1
      }

      if (partition >= splits.length) {
        partition = splits.length - 1
      }
    }

    partition
  }

  override def readExternal(in: ObjectInput): Unit = {
    val splitsbuilder = Array.newBuilder[Long]

    val length = in.readInt()
    for (i <- 0 until length) {
      splitsbuilder += in.readLong()
    }
    splits = splitsbuilder.result()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(splits.length)
    splits.foreach(out.writeLong)
  }
}
