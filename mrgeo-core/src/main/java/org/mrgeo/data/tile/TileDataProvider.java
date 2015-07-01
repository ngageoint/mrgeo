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

package org.mrgeo.data.tile;

import java.io.IOException;

import org.mrgeo.data.ProtectionLevelValidator;


/**
 * Data plugins must provide a sub-class for this class and implementations
 * for each of the abstract methods. This is the interface that must be
 * implemented by a data plugin to store and retrieve data.
 */
public abstract class TileDataProvider<V> implements ProtectionLevelValidator
{
  private String resourceName;

  /**
   * This constructor should only be used by sub-classes in the case where
   * they don't know the resource name when they are constructed. They are
   * responsible for subsequently calling setResourceName.
   */
  protected TileDataProvider()
  {
    resourceName = null;
  }

  /**
   * Sub-classes which use the default constructor must subsequently call
   * this method to assign the resource name,
   * 
   * @param resourceName
   */
  protected void setResourceName(String resourceName)
  {
    this.resourceName = resourceName;
  }

  public TileDataProvider(final String resourceName)
  {
    this.resourceName = resourceName;
  }

  public String getResourceName()
  {
    return resourceName;
  }

  public abstract void delete() throws IOException;
  public abstract void move(String toResource) throws IOException;
}
