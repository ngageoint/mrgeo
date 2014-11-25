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

package org.mrgeo.data.tile;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class TileIdWritable extends LongWritable implements Serializable
{	 
	public TileIdWritable() {
		super();
	}

	public TileIdWritable(long value) {
		super(value);
	}

	public TileIdWritable(TileIdWritable writable) {
		this(writable.get());
	}

  // we could use the default serializations here, but instead we'll just do it manually
  private synchronized void writeObject(ObjectOutputStream stream) throws IOException
  {
    stream.writeLong(get());
  }

  private synchronized void readObject(ObjectInputStream stream) throws IOException
  {
    set(stream.readLong());
  }

}
