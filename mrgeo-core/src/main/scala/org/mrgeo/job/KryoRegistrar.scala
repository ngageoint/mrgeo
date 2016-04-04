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

package org.mrgeo.job

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.data.tile.TileIdWritable
import org.mrgeo.job.serializers.{PixelSerializer, BoundsSerializer, RasterWritableSerializer}
import org.mrgeo.utils.tms.{Pixel, Bounds}

class KryoRegistrar extends KryoRegistrator
{
  override def registerClasses(kryo: Kryo) {
    //    kryo.setReferences(false)

    kryo.register(classOf[TileIdWritable])
    kryo.register(classOf[RasterWritable], new RasterWritableSerializer)

    kryo.register(classOf[Bounds], new BoundsSerializer)
    kryo.register(classOf[Pixel], new PixelSerializer)

    kryo.register(classOf[Array[Long]])
    kryo.register(Class.forName("org.apache.spark.util.BoundedPriorityQueue"))

  }
}

