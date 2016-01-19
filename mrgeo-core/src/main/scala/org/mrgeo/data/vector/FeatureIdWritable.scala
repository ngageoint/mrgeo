package org.mrgeo.data.vector

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.hadoop.io.LongWritable

class FeatureIdWritable extends LongWritable with Externalizable
{
  def this(featureId: Long) {
    this()
    set(featureId)
  }

  override def readExternal(in:ObjectInput): Unit = {
    set(in.readLong())
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(get())
  }
}
