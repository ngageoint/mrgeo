/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.tile;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TileIdZoomWritable extends TileIdWritable
{

  private int zoom = -1;

  public TileIdZoomWritable()
  {
  }

  public TileIdZoomWritable(final long value)
  {
    super(value);
  }

  public TileIdZoomWritable(final long value, final int zoom)
  {
    super(value);
    this.zoom = zoom;
  }

  public TileIdZoomWritable(final TileIdWritable writable)
  {
    super(writable);
  }

  public TileIdZoomWritable(final TileIdZoomWritable writable)
  {
    super(writable);
    this.zoom = writable.zoom;
  }

  public int compareTo(final LongWritable o)
  {
    if (o instanceof TileIdZoomWritable)
    {
      return compareTo((TileIdZoomWritable) o);
    }
    return super.compareTo(o);
  }

  /** Compares two TileIdZoomWritable. */
  public int compareTo(final TileIdZoomWritable o)
  {
    if (this.zoom < o.zoom)
    {
      return -1;
    }
    else if (this.zoom > o.zoom)
    {
      return 1;
    }
    final long thisValue = this.get();
    final long thatValue = o.get();

    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  @Override
  public boolean equals(final Object o)
  {
    if (!(o instanceof TileIdZoomWritable))
    {
      return false;
    }
    final TileIdZoomWritable other = (TileIdZoomWritable) o;
    return this.get() == other.get() && this.zoom == other.zoom;
  }

  public int getZoom()
  {
    return zoom;
  }

  @Override
  public int hashCode()
  {
    return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
        // if deriving: appendSuper(super.hashCode()).
        appendSuper(super.hashCode()).append(zoom).toHashCode();
  }

  @Override
  public void readFields(final DataInput in) throws IOException
  {
    super.readFields(in);
    zoom = in.readInt();
  }

  @Override
  public void write(final DataOutput out) throws IOException
  {
    super.write(out);
    out.writeInt(zoom);
  }

}
