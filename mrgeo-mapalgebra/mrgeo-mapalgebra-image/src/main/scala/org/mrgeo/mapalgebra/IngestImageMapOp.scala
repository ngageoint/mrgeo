package org.mrgeo.mapalgebra

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.mrgeo.core.{MrGeoConstants, MrGeoProperties}
import org.mrgeo.data.rdd.RasterRDD
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.image.MrsImagePyramidMetadata
import org.mrgeo.ingest.IngestImageSpark
import org.mrgeo.mapalgebra.old.MapOpRegistrar
import org.mrgeo.mapalgebra.parser.{ParserException, ParserNode}
import org.mrgeo.mapalgebra.raster.RasterMapOp
import org.mrgeo.spark.job.JobArguments
import org.mrgeo.utils.GDALUtils

import scala.util.control.Breaks

object IngestImageMapOp extends MapOpRegistrar {

  override def register: Array[String] = {
    Array[String]("ingest")
  }

  override def apply(node:ParserNode, variables: String => Option[ParserNode]): MapOp =
    new IngestImageMapOp(node, variables)
}

class IngestImageMapOp extends RasterMapOp with Externalizable {
  private val TileSize = "tilesize"

  private var rasterRDD: Option[RasterRDD] = None

  private var input:Option[String] = None
  private var categorical:Option[Boolean] = None
  private var zoom:Option[Int] = None

  private[mapalgebra] def this(node: ParserNode, variables: String => Option[ParserNode]) = {
    this()

    if (node.getNumChildren < 1 || node.getNumChildren > 3) {
      throw new ParserException("Usage: ingest(input(s), [zoom], [categorical]")
    }

    input = MapOp.decodeString(node.getChild(0), variables)

    if (node.getNumChildren >= 2) {
      zoom = MapOp.decodeInt(node.getChild(1), variables)
    }

    if (node.getNumChildren >= 3) {
      categorical = MapOp.decodeBoolean(node.getChild(2), variables)
    }
  }


  override def rdd(): Option[RasterRDD] = rasterRDD

  override def setup(job: JobArguments, conf: SparkConf): Boolean = {

    conf.set("spark.storage.memoryFraction", "0.25") // set the storage amount lower...
    conf.set("spark.shuffle.memoryFraction", "0.50") // set the shuffle higher

    true
  }

  override def execute(context: SparkContext): Boolean = {

    val inputs = input.getOrElse(throw new IOException("Inputs not set"))

    val path = new Path(inputs)
    val fs = HadoopFileUtils.getFileSystem(context.hadoopConfiguration, path)

    val rawfiles = fs.listFiles(path, true)

    val filebuilder = Array.newBuilder[String]
    while (rawfiles.hasNext) {
      val raw = rawfiles.next()

      if (!raw.isDirectory) {
        filebuilder += raw.getPath.toUri.toString
      }
    }
    val tilesize = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT).toInt

    if (zoom.isEmpty) {
      var newZoom = 1
      filebuilder.result().foreach(file => {
        val z = GDALUtils.calculateZoom(file, tilesize)
        if (z > newZoom) {
          newZoom = z
        }
      })
      zoom = Some(newZoom)
    }

    var nodata = Double.NaN

    val done = new Breaks
    done.breakable({
      filebuilder.result().foreach(file => {
        try {
          nodata = GDALUtils.getnodata(file)
          done.break()
        }
        catch {
          case e:Exception => // ignore and go on
        }
      })
    })

    if (categorical.isEmpty) {
      categorical = Some(false)
    }

    val result = IngestImageSpark.ingest(context, filebuilder.result(), zoom.get, tilesize, categorical.get, nodata)
    rasterRDD = result._1 match {
    case rrdd:RasterRDD => Some(rrdd)
    case _ => None
    }

    metadata(result._2 match {
    case md:MrsImagePyramidMetadata => md
    case _ => null
    })

    true
  }

  override def teardown(job: JobArguments, conf: SparkConf): Boolean = true

  override def readExternal(in: ObjectInput): Unit = {}
  override def writeExternal(out: ObjectOutput): Unit = {}

}
