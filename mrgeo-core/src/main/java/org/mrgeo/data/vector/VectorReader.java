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

package org.mrgeo.data.vector;

import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.KVIterator;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Bounds;

public interface VectorReader
{
  public abstract void close();
  public abstract KVIterator<LongWritable, Geometry> get();
  // TODO: How do we handle the following for vector sources that aren't
  // indexed at all (like maybe tsv/csv)? Should these methods be included
  // in a separate interface which callers must use "instanceof" followed
  // by a typecast in order to call these methods?
  public abstract boolean exists(LongWritable featureId);
  public abstract Geometry get(LongWritable featureId);
  public abstract KVIterator<LongWritable, Geometry> get(final Bounds bounds);
  // TODO: Do we want to include some more advanced spatial query capability here?
  // We could handle that via a different interface that callers could use
  // "instanceof" to see of the reader supports that capability...
}
