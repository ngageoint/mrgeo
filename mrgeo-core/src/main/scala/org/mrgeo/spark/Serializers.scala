package org.mrgeo.spark

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
