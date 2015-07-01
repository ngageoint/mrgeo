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

package org.mrgeo.opimage;

/**
 * This interface is used to communicate location information to
 * classes that need to know where the tile they are operating
 * on resides in the world. Implementors can use the information
 * passed through this interface along with TMSUtils to find the
 * world coordinates of the tile.
 * 
 * This interface is useful for JAI OpImage classes used within
 * OpImageDriver to know where they are located.
 */
public interface TileLocator
{
  /**
   * Specifies the tile x/y coordinates, zoom level and tile size.
   * Implementors can use TMSUtils to obtain world coordinates based
   * on this information.
   * 
   * @param tx
   * @param ty
   * @param zoom
   * @param tileSize
   */
  public void setTileInfo(long tx, long ty, int zoom, int tileSize);
}
