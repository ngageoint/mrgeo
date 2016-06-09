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

package org.mrgeo.data.vector;

import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.tms.Bounds;

import java.io.IOException;

public interface VectorReader
{
  public abstract void close();

  /**
   * Return an iterator that allows the caller to visit all features
   * within this reader. The caller is responsible for calling the close()
   * method on the returned iterator when they are done with it.
   * 
   * @return
   * @throws IOException 
   */
  public abstract CloseableKVIterator<FeatureIdWritable, Geometry> get() throws IOException;
  
  /**
   * Returns true if there is a feature with the specified featureId.
   * For non-indexed data sources (like delimited text), this method will
   * iterate through all of the features until it finds the one that matches
   * the specified featureId.
   * 
   * @param featureId
   * @return
   * @throws IOException
   */
  public abstract boolean exists(FeatureIdWritable featureId) throws IOException;
  
  /**
   * Returns the Geometry object corresponding to the featureId passed in.
   * For non-indexed data sources (like delimited text), this method will
   * iterate through all of the features until it finds the one that matches
   * the specified featureId. If no matching feature is found, a null is
   * returned.
   * 
   * @param featureId
   * @return
   * @throws IOException
   */
  public abstract Geometry get(FeatureIdWritable featureId) throws IOException;

  /**
   * Return an iterator that allows the caller to visit all geometries
   * within this reader that fall within the specified bounds. The
   * caller is responsible for calling the close()
   * method on the returned iterator when they are done with it.
   * 
   * It is possible that the last call to the next() method of the returned
   * iterator could give back a null value, so the caller should test for that.
   * 
   * @return
   */
  public abstract CloseableKVIterator<FeatureIdWritable, Geometry> get(final Bounds bounds) throws IOException;

  /**
   * Returns the number of features.
   * 
   * @return
   * @throws IOException
   */
  public long count() throws IOException;

  // TODO: Do we want to include some more advanced spatial query capability here?
  // We could handle that via a different interface that callers could use
  // "instanceof" to see of the reader supports that capability...
}
