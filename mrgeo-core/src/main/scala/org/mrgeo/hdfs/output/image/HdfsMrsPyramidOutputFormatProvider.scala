package org.mrgeo.hdfs.output.image

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{SequenceFile, Writable, WritableComparable}
import org.apache.hadoop.mapreduce.{Job, OutputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.spark.rdd.PairRDDFunctions
import org.mrgeo.data.DataProviderException
import org.mrgeo.data.image.{ImageOutputFormatContext, MrsImageOutputFormatProvider}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider
import org.mrgeo.hdfs.partitioners.{RowPartitioner, BlockSizePartitioner, FileSplitPartitioner}
import org.mrgeo.hdfs.tile.FileSplit
import org.mrgeo.hdfs.utils.HadoopFileUtils


class HdfsMrsPyramidOutputFormatProvider(context: ImageOutputFormatContext) extends MrsImageOutputFormatProvider(context) {

  private[image] object PartitionType extends Enumeration {
    val ROW, BLOCKSIZE = Value
  }

  private[image] var provider: HdfsMrsImageDataProvider = null
  private[image] var partitioner: PartitionType.Value = null

  def this(provider: HdfsMrsImageDataProvider, context: ImageOutputFormatContext) {
    this(context)

    this.provider = provider
    partitioner = PartitionType.BLOCKSIZE
  }

  def setInfo(conf: Configuration, job: Job) {
    conf.set("io.map.index.interval", "1")
    if (job != null) {
      SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD)
    }
  }

  def setOutputInfo(conf: Configuration, job: Job, output: String) {
    setInfo(conf, job)
    if (job != null) {
      FileOutputFormat.setOutputPath(job, new Path(output))
    }
    else {
      conf.set("mapred.output.dir", output)
      conf.set("mapreduce.output.fileoutputformat.outputdir", output)
    }
  }

  override def save(raster: RasterRDD, conf:Configuration): Unit = {

    implicit val tileIdOrdering = new Ordering[TileIdWritable] {
      override def compare(x: TileIdWritable, y: TileIdWritable): Int = x.compareTo(y)
    }

    val outputWithZoom: String = provider.getResolvedResourceName(false) + "/" + context.getZoomlevel
    val outputPath: Path = new Path(outputWithZoom)

    val jobconf = try {
      val fs: FileSystem = HadoopFileUtils.getFileSystem(conf, outputPath)

      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true)
      }

      // location of the output
      conf.set("mapred.output.dir", outputPath.toString)
      conf.set("mapreduce.output.fileoutputformat.outputdir", outputPath.toString)

      // compress
      conf.setBoolean(FileOutputFormat.COMPRESS, true)

      // add every tile to the index
      conf.set("io.map.index.interval", "1")

      Job.getInstance(super.setupOutput(conf)).getConfiguration
    }
    catch {
      case e: IOException =>
        throw new DataProviderException("Error running spark job setup", e)
    }


    val sparkPartitioner = getSparkPartitioner

    // Repartition the output if the output data provider requires it
    val sorted = RasterRDD(
      if (sparkPartitioner == null) {
        raster.sortByKey()
      }
      else if (sparkPartitioner.hasFixedPartitions) {
        raster.sortByKey(numPartitions = sparkPartitioner.calculateNumPartitions(raster, outputWithZoom))
      }
      else {
        raster.repartitionAndSortWithinPartitions(sparkPartitioner)
      })


    val wrappedForSave = new PairRDDFunctions(sorted)
    wrappedForSave.saveAsNewAPIHadoopDataset(jobconf)

    if (sparkPartitioner != null)
    {
      sparkPartitioner.writeSplits(sorted, context.getOutput, context.getZoomlevel, jobconf)
    }

  }

  override def finalizeExternalSave(conf: Configuration): Unit = {
    try {
      val imagePath: String = provider.getResolvedResourceName(true)
      val outputWithZoom: Path = new Path(imagePath + "/" + context.getZoomlevel)
      val split: FileSplit = new FileSplit
      split.generateSplits(outputWithZoom, conf)
      split.writeSplits(outputWithZoom)
    }
    catch {
      case e: IOException => {
        throw new DataProviderException("Error in finalizeExternalSave", e)
      }
    }
  }

  override def validateProtectionLevel(protectionLevel: String): Boolean = true

  private def getSparkPartitioner:FileSplitPartitioner = {
    partitioner match {
    case PartitionType.ROW =>
      new RowPartitioner(context.getBounds, context.getZoomlevel, context.getTilesize)
    case PartitionType.BLOCKSIZE =>
      new BlockSizePartitioner()
    case _ =>
      new BlockSizePartitioner()
    }
  }

  override protected def getOutputFormat: OutputFormat[WritableComparable[_], Writable] = new HdfsMrsPyramidOutputFormat
}
