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

package org.mrgeo.pyramid;

import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;

public abstract class MrsPyramid
{
  protected abstract MrsPyramidMetadata getMetadataInternal();

  protected MrsPyramid()
  {
  }

  public Bounds getBounds()
  {
    return getMetadataInternal().getBounds();
  }

  public LongRectangle getTileBounds(int zoomLevel)
  {
    return getMetadataInternal().getTileBounds(zoomLevel);
  }

  public int getTileSize()
  {
    return getMetadataInternal().getTilesize();
  }

  public int getMaximumLevel()
  {
    return getMetadataInternal().getMaxZoomLevel();
  }

  public String getName()
  {
    return getMetadataInternal().getPyramid();
  }

  public int getNumLevels()
  {
    return getMetadataInternal().getMaxZoomLevel();
  }

  /**
   * Return true if there is data at each of the pyramid levels.
   * 
   * @return
   */
  public boolean hasPyramids()
  {
    MrsPyramidMetadata metadata = getMetadataInternal();
    return metadata.hasPyramids();
  }
}
