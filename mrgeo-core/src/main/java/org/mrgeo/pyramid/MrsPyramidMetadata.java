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

import org.codehaus.jackson.annotate.JsonIgnore;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * MrsPyramidMetada is the class that describes a MrGeo Pyramid.
 * The content of the pyramid is defined by sub-classes. This
 * base metadata object describes geospatial bounds, geospatial
 * tiled bounds and maximum zoom level.
 *
 * A pyramid represents zoom levels of data based on TMS.
 */
public abstract class MrsPyramidMetadata implements Serializable
{
// version
private static final long serialVersionUID = 1L;


/**
 * TileMetadata is a container of the multiple types of
 * bounds for tiled data.
 */
public static class TileMetadata implements Serializable
{
  private static final long serialVersionUID = 1L;

  // tile bounds (min tx, min ty, max tx, max ty)
  public LongRectangle tileBounds = null;

  // hdfs path or accumulo table for the image min, max, mean pixel values
  public String name = null;

  // basic constructor
  public TileMetadata()
  {
  }

} // end TileMetadata
  
  
  /*
   * start globals section
   */

protected String pyramid; // base hdfs path or accumulo table for the pyramid

// NOTE: bounds, and pixel bounds are for the exact image data, they do
// not include the extra "slop" around the edges of the tile.
protected Bounds bounds; // The bounds of the image in decimal degrees.

protected int tilesize; // tile width/height, in pixels

protected int maxZoomLevel; // maximum zoom level (minimum pixel size)

protected Map<String, String> tags = new HashMap<String, String>(); // freeform k,v pairs of tags

protected String protectionLevel = "";

  /*
   * end globals section
   */

public MrsPyramidMetadata()
{
}

public MrsPyramidMetadata(MrsPyramidMetadata copy) {
  this.pyramid = copy.pyramid;
  this.bounds = new Bounds(copy.bounds.getMinX(), copy.bounds.getMinY(), copy.bounds.getMaxX(), copy.bounds.getMaxY());
  this.tilesize = copy.tilesize;
  this.maxZoomLevel = copy.maxZoomLevel;
  this.tags.putAll(copy.tags);
  this.protectionLevel = copy.protectionLevel;
}

  /*
   * start get section
   */


public Bounds getBounds()
{
  return bounds;
}

public abstract String getName(final int zoomlevel);

public int getMaxZoomLevel()
{
  return maxZoomLevel;
}


@JsonIgnore
public String getPyramid()
{
  return pyramid;
}


public abstract LongRectangle getTileBounds(final int zoomlevel);


public abstract LongRectangle getOrCreateTileBounds(final int zoomlevel);


public int getTilesize()
{
  return tilesize;
}

public Map<String, String> getTags()
{
  return tags;
}

@JsonIgnore
public String getTag(final String tag)
{
  return getTag(tag, null);
}

@JsonIgnore
public String getTag(final String tag, final String defaultValue)
{
  if (tags.containsKey(tag))
  {
    return tags.get(tag);
  }

  return defaultValue;
}

public String getProtectionLevel()
{
  return protectionLevel;
}

  /*
   * end get section
   */

  
  /*
   * start set section
   */

public void setBounds(final Bounds bounds)
{
  this.bounds = bounds;
}


public void setName(final int zoomlevel)
{
  setName(zoomlevel, Integer.toString(zoomlevel));
}


public abstract void setName(final int zoomlevel, final String name);


public abstract void setMaxZoomLevel(final int zoomlevel);


public void setPyramid(final String pyramid)
{
  this.pyramid = pyramid;
}


public abstract void setTileBounds(final int zoomlevel, final LongRectangle tileBounds);


public void setTilesize(final int size)
{
  tilesize = size;
}

public void setTags(final Map<String, String> tags)
{
  // make a copy of the tags...
  this.tags = new HashMap<String, String>(tags);
}

@JsonIgnore
public void setTag(final String tag, final String value)
{
  tags.put(tag, value);
}

public void setProtectionLevel(final String protectionLevel)
{
  this.protectionLevel = protectionLevel;
}

  /*
   * end set section
   */

/**
 * Return true if there is data at each of the pyramid levels.
 */
public boolean hasPyramids()
{
  int levels = 0;
  if (getMaxZoomLevel() > 0)
  {
    for (int i = 1; i <= getMaxZoomLevel(); i++)
    {
      if (getName(i) != null)
      {
        levels++;
      }
    }
  }
  return (levels == getMaxZoomLevel());
}
} // end MrsPyramidMetadata

