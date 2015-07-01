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

package org.mrgeo.rasterops;

import javax.media.jai.TileCache;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.Serializable;
import java.util.*;

/**
 * @author jason.surratt
 * 
 */
public class SimpleTileCache implements TileCache, Serializable
{
  class Entry
  {
    public long insertionTime;
    public Raster tile = null;

    public Entry(Raster tile)
    {
      this.tile = tile;
      insertionTime = System.currentTimeMillis();
    }
  }

  private static final long serialVersionUID = 1L;
  // initialize to 64MB of cache
  long memoryCapacity = 1048576 * 64;
  float memoryThreshold = .75f;

  long stored = 0;

  private transient HashMap<RenderedImage, HashMap<Point, Entry>> tiles = 
    new HashMap<RenderedImage, HashMap<Point, Entry>>();

  synchronized public long getBytesStored()
  {
    return stored;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#add(java.awt.image.RenderedImage, int, int,
   * java.awt.image.Raster)
   */
  @Override
  synchronized public void add(RenderedImage owner, int tx, int ty, Raster tile)
  {
    stored += calculateTileSize(tile);
    HashMap<Point, Entry> h2 = tiles.get(owner);
    if (h2 == null)
    {
      h2 = new HashMap<Point, Entry>();
      tiles.put(owner, h2);
    }
    Point p = new Point(tx, ty);
    if (h2.containsKey(p))
    {
      Entry e = h2.get(p);
      stored -= calculateTileSize(e.tile);
    }
    h2.put(new Point(tx, ty), new Entry(tile));
    if (stored > memoryCapacity)
    {
      memoryControl();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#add(java.awt.image.RenderedImage, int, int,
   * java.awt.image.Raster, java.lang.Object)
   */
  @Override
  synchronized public void add(RenderedImage owner, int tx, int ty, Raster tile,
      Object tileCacheMetric)
  {
    // we're ignoring the compute cost
    add(owner, tx, ty, tile);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#addTiles(java.awt.image.RenderedImage,
   * java.awt.Point[], java.awt.image.Raster[], java.lang.Object)
   */
  @Override
  synchronized public void addTiles(RenderedImage owner, Point[] tileIndices, Raster[] t,
      Object tileCacheMetric)
  {
    assert (tileIndices.length == t.length);
    for (int i = 0; i < t.length; i++)
    {
      add(owner, tileIndices[i].x, tileIndices[i].y, t[i]);
    }
  }

  static long calculateTileSize(Raster tile)
  {
    int size = tile.getDataBuffer().getSize();
    int typeSize = DataBuffer.getDataTypeSize(tile.getDataBuffer().getDataType()) / 8;
    return size * typeSize;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#flush()
   */
  @Override
  synchronized public void flush()
  {
    tiles.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getMemoryCapacity()
   */
  @Override
  public long getMemoryCapacity()
  {
    return memoryCapacity;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getMemoryThreshold()
   */
  @Override
  public float getMemoryThreshold()
  {
    return memoryThreshold;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getTile(java.awt.image.RenderedImage, int,
   * int)
   */
  @Override
  synchronized public Raster getTile(RenderedImage owner, int tx, int ty)
  {
    HashMap<Point, Entry> h2 = tiles.get(owner);
    Raster result = null;
    if (h2 != null)
    {
      Entry e = h2.get(new Point(tx, ty));
      if (e != null)
      {
        result = e.tile;
      }
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getTileCapacity()
   */
  @Override
  public int getTileCapacity()
  {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getTileComparator()
   */
  @Override
  public Comparator getTileComparator()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getTiles(java.awt.image.RenderedImage)
   */
  @Override
  synchronized public Raster[] getTiles(RenderedImage owner)
  {
    HashMap<Point, Entry> h2 = tiles.get(owner);
    Raster[] result = null;
    if (h2 != null)
    {
      result = new Raster[h2.size()];
      Entry[] entries = (Entry[]) h2.values().toArray();
      for (int i = 0; i < result.length; i++)
      {
        result[i] = entries[i].tile;
      }
    }

    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#getTiles(java.awt.image.RenderedImage,
   * java.awt.Point[])
   */
  @Override
  synchronized public Raster[] getTiles(RenderedImage owner, Point[] tileIndices)
  {
    Raster[] result = new Raster[tileIndices.length];
    for (int i = 0; i < tileIndices.length; i++)
    {
      result[i] = getTile(owner, tileIndices[i].x, tileIndices[i].y);
    }

    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#memoryControl()
   */
  @Override
  synchronized public void memoryControl()
  {
    if (stored < memoryCapacity)
    {
      return;
    }

    class Tmp
    {
      public RenderedImage owner;
      public Point point;

      Tmp(RenderedImage owner, Point point)
      {
        this.owner = owner;
        this.point = point;
      }
    }

    // load all the entries up into a tree. This way we can easily remove the
    // smallest ones.
    TreeMap<Long, Tmp> map = new TreeMap<Long, Tmp>();

    Set<RenderedImage> owners = tiles.keySet();
    for (RenderedImage owner : owners)
    {
      HashMap<Point, Entry> h2 = tiles.get(owner);
      Set<Point> points = h2.keySet();
      for (Point point : points)
      {
        Entry e = h2.get(point);
        map.put(e.insertionTime, new Tmp(owner, point));
      }
    }

    Iterator<Tmp> it = map.values().iterator();
    while (stored > memoryCapacity * memoryThreshold && it.hasNext())
    {
      Tmp t = it.next();
      remove(t.owner, t.point.x, t.point.y);
    }
    map.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#remove(java.awt.image.RenderedImage, int,
   * int)
   */
  @Override
  synchronized public void remove(RenderedImage owner, int tx, int ty)
  {
    HashMap<Point, Entry> h2 = tiles.get(owner);
    if (h2 != null)
    {
      Point key = new Point(tx, ty);
      if (h2.containsKey(key))
      {
        Raster tile = h2.get(key).tile;
        stored -= calculateTileSize(tile);
        h2.remove(key);
      }
      if (h2.size() == 0)
      {
        tiles.remove(owner);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#removeTiles(java.awt.image.RenderedImage)
   */
  @Override
  synchronized public void removeTiles(RenderedImage owner)
  {
    tiles.remove(owner);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#setMemoryCapacity(long)
   */
  @Override
  synchronized public void setMemoryCapacity(long memoryCapacity)
  {
    this.memoryCapacity = memoryCapacity;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#setMemoryThreshold(float)
   */
  @Override
  synchronized public void setMemoryThreshold(float threshold)
  {
    memoryThreshold = threshold;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#setTileCapacity(int)
   */
  @Override
  public void setTileCapacity(int arg0)
  {
    // deprecated
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.media.jai.TileCache#setTileComparator(java.util.Comparator)
   */
  @Override
  public void setTileComparator(Comparator arg0)
  {
    // noop
  }
  
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
