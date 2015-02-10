/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.utils

import java.util.Properties

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.image.MrsImageDataProvider
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.hdfs.input.image.HdfsMrsImagePyramidSimpleInputFormat
import org.mrgeo.image.MrsImagePyramidMetadata

object SparkUtils {

  def loadMrsPyramid(imageName: String, context: SparkContext):
  (RDD[(TileIdWritable, RasterWritable)], MrsImagePyramidMetadata) =
  {
    val keyclass = classOf[TileIdWritable]
    val valueclass = classOf[RasterWritable]
    
    //TODO:   Get this from the DataProvider
    val inputformatclass = classOf[HdfsMrsImagePyramidSimpleInputFormat] // MrsImagePyramidInputFormat]

    // build a phony job...
    val job = new Job()

    val providerProps: Properties = null
    val dp: MrsImageDataProvider = DataProviderFactory.getMrsImageDataProvider(imageName,
      DataProviderFactory.AccessMode.READ, providerProps)

    val metadata: MrsImagePyramidMetadata = dp.getMetadataReader.read()

    FileInputFormat.addInputPaths(job, imageName + "/" + metadata.getMaxZoomLevel)

    //    MrsImageDataProvider.setupMrsPyramidInputFormat(job, imageName, metadata.getMaxZoomLevel,
    //      metadata.getTilesize, providerProps)

    // calculate the friction surface
    val image = context.newAPIHadoopRDD(job.getConfiguration,
      inputformatclass,
      keyclass,
      valueclass).persist(StorageLevel.MEMORY_AND_DISK_SER)

    (image, metadata)
  }

}
