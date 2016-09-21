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

package org.mrgeo.ingest

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.SparkContext
import org.mrgeo.data.DataProviderFactory
import org.mrgeo.data.DataProviderFactory.AccessMode
import org.mrgeo.utils.SparkUtils

class IngestLocal extends IngestImage with Externalizable {

  override def execute(context: SparkContext): Boolean = {

    val ingested = IngestImage.localingest(context, inputs, zoom, skipPreprocessing, tilesize,
      categorical, skipCategoryLoad, nodata, protectionlevel)

    val dp = DataProviderFactory.getMrsImageDataProvider(output, AccessMode.OVERWRITE, providerproperties)
    SparkUtils.saveMrsPyramid(ingested._1, dp, ingested._2, zoom, context.hadoopConfiguration, providerproperties)

    true
  }

  override def readExternal(in: ObjectInput): Unit = {
    val bands = in.readInt()
    nodata = Array.ofDim[Double](bands)
    for (band <- 0 until bands) {
      nodata(band) = in.readDouble()
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nodata.length)
    for (nd <- nodata) {
      out.writeDouble(nd.doubleValue())
    }
  }


}