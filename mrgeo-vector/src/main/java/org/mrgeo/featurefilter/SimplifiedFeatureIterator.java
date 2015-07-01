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

package org.mrgeo.featurefilter;

import org.mrgeo.data.FeatureIterator;
import org.mrgeo.geometry.Geometry;

import java.util.NoSuchElementException;

/**
 * Provides a slightly simpler version of the iterator interface.
 */
public abstract class SimplifiedFeatureIterator implements FeatureIterator
{
  Geometry current = null;
  boolean primed;

  @Override
  public boolean hasNext()
  {
    if (current == null)
    {
      current = simpleNext();
    }
    return current != null;
  }

  @Override
  public Geometry next()
  {
    if (hasNext())
    {
      Geometry result = current;
      current = null;
      return result;
    }
    throw new NoSuchElementException();
  }

  /**
   * Returns either the next feature or null if there are not features
   * remaining. This is a bit simpler to implement than the full Iterator
   * interface.
   * 
   * @return
   */
  public abstract Geometry simpleNext();

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
