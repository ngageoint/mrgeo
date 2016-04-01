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

package org.mrgeo.mapreduce.formats;

import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Defines a rectangular area of tiles to be read during input of data
 * in map/reduce jobs. This will be used when tiling any input for a
 * map/reduce job.
 */
public class TileClusterInfo implements Serializable
{
  private static final long serialVersionUID = 1L;
  
  private long offsetX;
  private long offsetY;
  private int width;
  private int height;
  private long stride;
  
  public TileClusterInfo()
  {
    this.offsetX = 0;
    this.offsetY = 0;
    this.width = 1;
    this.height = 1;
    this.stride = 1;
  }
  
  public TileClusterInfo(long offsetX, long offsetY, int width, int height, long stride)
  {
    this.offsetX = offsetX;
    this.offsetY = offsetY;
    this.width = width;
    this.height = height;
    this.stride = stride;
  }

  public void expand(TileClusterInfo other)
  {
    long maxX = offsetX + width;
    long maxY = offsetY + height;
    long otherMaxX = other.offsetX + other.width;
    long otherMaxY = other.offsetY + other.height;
    long newMinX = Math.min(offsetX,  other.offsetX);
    long newMinY = Math.min(offsetY,  other.offsetY);

    offsetX = newMinX;
    width = (int)(Math.max(maxX, otherMaxX) - offsetX);
    offsetY = newMinY;
    height = (int)(Math.max(maxY, otherMaxY) - offsetY);
  }

  public int getNeighborCount()
  {
    int count = 0;
    for (long x = offsetX; x < offsetX + width; x++)
    {
      for (long y = offsetY; y< offsetY + height; y++)
      {
        if (x != 0 || y != 0)
        {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Computes the specific neighbor tile id's of the baseTileId
   * that are required and stores them in the neighbors array passed
   * in. This allows the caller to allocate the array the once and
   * reuse it for each call to save repeated allocation and deallocation
   * of the neighbors array. The caller can use getNeighborCount() to
   * determine the maximum array size required for returning neighbors.
   * This function returns the number of neighbors required for the
   * specific baseTileId passed in. The caller should only access
   * that number of elements from the neighbors array (beginning at
   * index 0). Array elements beyond that will be -1. If the neighbors
   * array is not large enough, and ArrayOutOfBoundsException will be
   * thrown by Java.
   * 
   * Only neighbor tiles that intersects the bounds passed in will be
   * returned. 
   * 
   * @param baseTileId The tile id for which to return the required
   * neighbors.
   * @param zoom The zoom level being used
   * @param tileSize The tile size being used
   * @param bounds The bounds which returned neighbors must intersects
   * 
   * @return Null if no neighbors required or an array of 1 or more
   * neighbor tile id's otherwise. The baseTileId will not be included
   * in the returned array.
   */
  public int getNeighbors(long baseTileId, int zoom, int tileSize,
      final Bounds bounds, long[] neighbors)
  {
    int index = 0;
    Arrays.fill(neighbors, -1);
    TMSUtils.Tile baseTile = TMSUtils.tileid(baseTileId, zoom);
    long numXTiles = TMSUtils.numXTiles(zoom);
    for (long x = offsetX; x < offsetX + width; x++)
    {
      for (long y = offsetY; y < offsetY + height; y++)
      {
        // Do not include the base tile in the neighbors list
        if (x != 0 || y != 0)
        {
          long tx = baseTile.tx + x;
          // If the required tiles are beyond the tile boundaries for the
          // globe, then we wrap around.
          if (tx < 0)
          {
            tx += numXTiles;
          }
          else if (tx >= numXTiles)
          {
            tx -= numXTiles;
          }
          long ty = baseTile.ty + y;
          Bounds tileBounds = TMSUtils.tileBounds(tx, ty, zoom, tileSize);
          if (bounds.intersects(tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n) &&
              TMSUtils.isValidTile(tx, ty, zoom))
          {
            long neighborTileId = TMSUtils.tileid(tx, ty, zoom);
            neighbors[index] = neighborTileId;
            index++;
          }
        }
      }
    }
    return index;
  }

  public long getOffsetX()
  {
    return offsetX;
  }

  public void setOffsetX(long offsetX)
  {
    this.offsetX = offsetX;
  }

  public long getOffsetY()
  {
    return offsetY;
  }

  public void setOffsetY(long offsetY)
  {
    this.offsetY = offsetY;
  }

  public int getWidth()
  {
    return width;
  }

  public void setWidth(int width)
  {
    this.width = width;
  }

  public int getHeight()
  {
    return height;
  }

  public void setHeight(int height)
  {
    this.height = height;
  }

  public long getStride()
  {
    return stride;
  }

  public void setStride(long stride)
  {
    this.stride = stride;
  }
}
