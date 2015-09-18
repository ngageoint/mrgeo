package org.mrgeo.mapalgebra.vector

import org.apache.spark.SparkContext
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.data.rdd.VectorRDD
import org.mrgeo.data.vector.{VectorDataProvider, VectorMetadata}
import org.mrgeo.data.{DataProviderFactory, ProviderProperties}
import org.mrgeo.mapalgebra.MapOp

object VectorMapOp {

  val EPSILON: Double = 1e-8

  def isNodata(value:Double, nodata:Double):Boolean = {
    if (nodata.isNaN) {
      value.isNaN
    }
    else {
      nodata == value
    }
  }
  def isNotNodata(value:Double, nodata:Double):Boolean = {
    if (nodata.isNaN) {
      !value.isNaN
    }
    else {
      nodata != value
    }
  }

  def nearZero(v:Double):Boolean = {
    if (v >= -EPSILON && v <= EPSILON) {
      true
    }
    else {
      false
    }
  }

}
abstract class VectorMapOp extends MapOp {

  private var meta:VectorMetadata = null


  def rdd():Option[VectorRDD]

  def metadata():Option[VectorMetadata] =  Option(meta)
  def metadata(meta:VectorMetadata) = { this.meta = meta}

  def save(output: String, providerProperties:ProviderProperties, context:SparkContext) = {
    rdd() match {
      case Some(rdd) =>
        val provider: VectorDataProvider =
          DataProviderFactory.getVectorDataProvider(output, AccessMode.OVERWRITE, providerProperties)
        metadata() match {
          case Some(metadata) =>
            val meta = metadata

            // TODO: Need new SparkVectorUtils.save method
//            SparkVectorUtils.save(rdd, provider, metadata,
//              context.hadoopConfiguration, providerproperties =  providerProperties)
          case _ =>
        }
      case _ =>
    }
  }


}
