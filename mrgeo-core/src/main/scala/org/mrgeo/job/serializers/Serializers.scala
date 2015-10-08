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

package org.mrgeo.job.serializers

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.mrgeo.data.raster.RasterWritable
import org.mrgeo.utils.Bounds

class Serializers {

}

class RasterWritableSerializer extends Serializer[RasterWritable] {
  override def write(kryo: Kryo, output: Output, rw: RasterWritable) = {

    val bytes = rw.getBytes
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[RasterWritable]): RasterWritable = {

    val length = input.readInt()
    return new RasterWritable(input.readBytes(length))
  }
}

class BoundsSerializer extends Serializer[Bounds] {
  override def write(kryo: Kryo, output: Output, bounds: Bounds): Unit = {
    output.writeDouble(bounds.getMinX)
    output.writeDouble(bounds.getMinY)
    output.writeDouble(bounds.getMaxX)
    output.writeDouble(bounds.getMaxY)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Bounds]): Bounds = {
    return new Bounds(input.readDouble(),input.readDouble(),input.readDouble(),input.readDouble())
  }
}
