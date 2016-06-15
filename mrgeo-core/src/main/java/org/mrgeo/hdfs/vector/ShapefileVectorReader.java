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

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.data.vector.VectorReaderContext;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.tms.Bounds;

import java.io.IOException;

public class ShapefileVectorReader implements VectorReader
{
  public ShapefileVectorReader(VectorDataProvider provider, VectorReaderContext context, Configuration conf)
  {
  }

  @Override
  public void close()
  {
  }

  @Override
  public CloseableKVIterator<FeatureIdWritable, Geometry> get() throws IOException
  {
    return null;
  }

  @Override
  public boolean exists(FeatureIdWritable featureId) throws IOException
  {
    return false;
  }

  @Override
  public Geometry get(FeatureIdWritable featureId) throws IOException
  {
    return null;
  }

  @Override
  public CloseableKVIterator<FeatureIdWritable, Geometry> get(Bounds bounds) throws IOException
  {
    return null;
  }

  @Override
  public long count() throws IOException
  {
    return 0;
  }
}
